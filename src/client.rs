use anyhow::{anyhow, Result};
use openmls::prelude::*;
use openmls::treesync::RatchetTreeIn;
use openmls_basic_credential::SignatureKeyPair;
use openmls_rust_crypto::OpenMlsRustCrypto;
use tls_codec::{Deserialize, Serialize};

use crate::debug::print_bytes;

pub struct Client {
    pub name: String,
    crypto: OpenMlsRustCrypto,
    signature_keypair: SignatureKeyPair,
    credential_with_key: CredentialWithKey,
    group: Option<MlsGroup>,
    pending_commit_bytes: Option<Vec<u8>>,
    pending_welcome_bytes: Option<Vec<u8>>,
    pending_welcome_recipients: Vec<String>,
}

pub struct EpochChangeOutput {
    pub commit_bytes: Vec<u8>,
}

pub enum CommitReceiveOutcome {
    OwnCommitAccepted {
        self_removed: bool,
        welcome_recipients: Vec<String>,
        welcome_bytes: Option<Vec<u8>>,
        ratchet_tree_bytes: Option<Vec<u8>>,
    },
    ExternalCommitApplied {
        self_removed: bool,
    },
}

impl Client {
    fn fresh(name: &str) -> Result<Self> {
        let crypto = OpenMlsRustCrypto::default();
        let ciphersuite = Ciphersuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519;
        let credential = BasicCredential::new(name.as_bytes().to_vec());
        let signature_keypair = SignatureKeyPair::new(ciphersuite.signature_algorithm())?;

        let public_key = signature_keypair.to_public_vec();
        print_bytes(&format!("{} signature public key", name), &public_key);
        print_bytes(&format!("{} identity bytes", name), name.as_bytes());

        let credential_with_key = CredentialWithKey {
            credential: credential.into(),
            signature_key: signature_keypair.to_public_vec().into(),
        };

        Ok(Self {
            name: name.to_string(),
            crypto,
            signature_keypair,
            credential_with_key,
            group: None,
            pending_commit_bytes: None,
            pending_welcome_bytes: None,
            pending_welcome_recipients: Vec::new(),
        })
    }

    pub fn new(name: &str) -> Result<Self> {
        Self::fresh(name)
    }

    fn reset_local_state_fully(&mut self) -> Result<()> {
        let name = self.name.clone();
        *self = Self::fresh(&name)?;
        Ok(())
    }

    fn hex_encode(bytes: &[u8]) -> String {
        bytes.iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join("")
    }

    pub fn group_id_hex(&self) -> Result<String> {
        let group = self
            .group
            .as_ref()
            .ok_or_else(|| anyhow!("Client is not in a group"))?;
        Ok(Self::hex_encode(group.group_id().as_slice()))
    }

    pub fn current_epoch_u64(&self) -> Result<u64> {
        let group = self
            .group
            .as_ref()
            .ok_or_else(|| anyhow!("Client is not in a group"))?;
        Ok(group.epoch().as_u64())
    }

    pub fn member_names(&self) -> Result<Vec<String>> {
        let group = self
            .group
            .as_ref()
            .ok_or_else(|| anyhow!("Client is not in a group"))?;

        Ok(group
            .members()
            .map(|member| {
                String::from_utf8_lossy(member.credential.serialized_content()).to_string()
            })
            .collect())
    }

    pub fn create_group(&mut self) -> Result<()> {
        let group_config = MlsGroupCreateConfig::default();

        let group = MlsGroup::new(
            &self.crypto,
            &self.signature_keypair,
            &group_config,
            self.credential_with_key.clone(),
        )?;

        self.group = Some(group);

        if let Some(group) = &self.group {
            let group_id_bytes = group.group_id().as_slice();
            print_bytes(&format!("{} group_id", self.name), group_id_bytes);
            println!("[DBG] {} epoch={:?}", self.name, group.epoch());
        }

        Ok(())
    }

    pub fn generate_key_package(&mut self) -> Result<Vec<u8>> {
        let ciphersuite = Ciphersuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519;

        let key_package_bundle = KeyPackage::builder().build(
            ciphersuite,
            &self.crypto,
            &self.signature_keypair,
            self.credential_with_key.clone(),
        )?;

        let key_package = key_package_bundle.key_package().clone();

        let key_package_bytes = key_package
            .tls_serialize_detached()
            .map_err(|_| anyhow!("Could not serialize KeyPackage"))?;
        print_bytes(&format!("{} KeyPackage", self.name), &key_package_bytes);

        Ok(key_package_bytes)
    }

    fn clear_pending_local_state(&mut self) {
        self.pending_commit_bytes = None;
        self.pending_welcome_bytes = None;
        self.pending_welcome_recipients.clear();
    }

    fn store_pending_epoch_change(
        &mut self,
        commit_message: MlsMessageOut,
        welcome_message: Option<MlsMessageOut>,
        welcome_recipients: Vec<String>,
    ) -> Result<EpochChangeOutput> {
        let commit_bytes = commit_message
            .tls_serialize_detached()
            .map_err(|_| anyhow!("Could not serialize commit message"))?;
        print_bytes(&format!("{} commit", self.name), &commit_bytes);

        let welcome_bytes = if let Some(welcome) = welcome_message {
            let bytes = welcome
                .tls_serialize_detached()
                .map_err(|_| anyhow!("Could not serialize welcome message"))?;
            print_bytes(&format!("{} welcome", self.name), &bytes);
            Some(bytes)
        } else {
            None
        };

        self.pending_commit_bytes = Some(commit_bytes.clone());
        self.pending_welcome_bytes = welcome_bytes;
        self.pending_welcome_recipients = welcome_recipients;

        Ok(EpochChangeOutput { commit_bytes })
    }

    fn cleanup_if_removed(&mut self) -> Result<bool> {
        let still_member = {
            let group = self
                .group
                .as_ref()
                .ok_or_else(|| anyhow!("Client is not in a group"))?;

            group.members().any(|member| {
                member.credential.serialized_content() == self.name.as_bytes()
            })
        };

        if still_member {
            return Ok(false);
        }

        self.reset_local_state_fully()?;
        Ok(true)
    }

    pub fn add_members(
        &mut self,
        key_package_bytes_list: &[Vec<u8>],
        member_names: &[String],
    ) -> Result<EpochChangeOutput> {
        if key_package_bytes_list.len() != member_names.len() {
            return Err(anyhow!(
                "Number of key packages does not match number of member names"
            ));
        }

        let mut key_packages = Vec::with_capacity(key_package_bytes_list.len());

        for key_package_bytes in key_package_bytes_list {
            let mut bytes = key_package_bytes.as_slice();

            let key_package_in = KeyPackageIn::tls_deserialize(&mut bytes)
                .map_err(|_| anyhow!("Could not deserialize KeyPackage"))?;

            let key_package = key_package_in
                .validate(self.crypto.crypto(), ProtocolVersion::Mls10)
                .map_err(|_| anyhow!("Invalid KeyPackage"))?;

            key_packages.push(key_package);
        }

        let group = self
            .group
            .as_mut()
            .ok_or_else(|| anyhow!("Client is not in a group"))?;

        let (commit_message, welcome_message, _group_info) = group.add_members(
            &self.crypto,
            &self.signature_keypair,
            &key_packages,
        )?;

        self.store_pending_epoch_change(
            commit_message,
            Some(welcome_message),
            member_names.to_vec(),
        )
    }

    fn find_member_index_by_name(&self, target_name: &str) -> Result<LeafNodeIndex> {
        let group = self
            .group
            .as_ref()
            .ok_or_else(|| anyhow!("Client is not in a group"))?;

        let member = group
            .members()
            .find(|member| member.credential.serialized_content() == target_name.as_bytes())
            .ok_or_else(|| anyhow!("Member '{}' not found in group", target_name))?;

        Ok(member.index)
    }

    fn find_member_indices_by_name(&self, target_names: &[String]) -> Result<Vec<LeafNodeIndex>> {
        let mut indices = Vec::with_capacity(target_names.len());

        for target_name in target_names {
            indices.push(self.find_member_index_by_name(target_name)?);
        }

        Ok(indices)
    }

    pub fn remove_members(&mut self, target_names: &[String]) -> Result<EpochChangeOutput> {
        let target_indices = self.find_member_indices_by_name(target_names)?;

        let group = self
            .group
            .as_mut()
            .ok_or_else(|| anyhow!("Client is not in a group"))?;

        let (commit_message, _welcome_option, _group_info) = group.remove_members(
            &self.crypto,
            &self.signature_keypair,
            &target_indices,
        )?;

        self.store_pending_epoch_change(commit_message, None, Vec::new())
    }

    pub fn self_update(&mut self) -> Result<EpochChangeOutput> {
        let group = self
            .group
            .as_mut()
            .ok_or_else(|| anyhow!("Client is not in a group"))?;

        let message_bundle = group.self_update(
            &self.crypto,
            &self.signature_keypair,
            LeafNodeParameters::default(),
        )?;

        let (commit_message, _welcome_option, _group_info) = message_bundle.into_contents();

        self.store_pending_epoch_change(commit_message, None, Vec::new())
    }

    pub fn rollback_pending_commit(&mut self) -> Result<()> {
        let group = self
            .group
            .as_mut()
            .ok_or_else(|| anyhow!("Client is not in a group"))?;

        group
            .clear_pending_commit(self.crypto.storage())
            .map_err(|_| anyhow!("Could not clear pending commit"))?;

        self.clear_pending_local_state();

        Ok(())
    }

    pub fn join_from_welcome(
        &mut self,
        welcome_bytes: &[u8],
        ratchet_tree_bytes: &[u8],
    ) -> Result<()> {
        let welcome_in = MlsMessageIn::tls_deserialize_exact(welcome_bytes)
            .map_err(|_| anyhow!("Could not deserialize Welcome message"))?;

        let welcome = match welcome_in.extract() {
            MlsMessageBodyIn::Welcome(welcome) => welcome,
            _ => return Err(anyhow!("Expected Welcome message")),
        };

        let ratchet_tree = RatchetTreeIn::tls_deserialize_exact(ratchet_tree_bytes)
            .map_err(|_| anyhow!("Could not deserialize ratchet tree"))?;

        let join_config = MlsGroupJoinConfig::default();

        let group = StagedWelcome::new_from_welcome(
            &self.crypto,
            &join_config,
            welcome,
            Some(ratchet_tree),
        )?
            .into_group(&self.crypto)?;

        self.group = Some(group);
        Ok(())
    }

    pub fn send_application_message(&mut self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let group = self
            .group
            .as_mut()
            .ok_or_else(|| anyhow!("Client is not in a group"))?;

        let message = group.create_message(&self.crypto, &self.signature_keypair, plaintext)?;

        let message_bytes = message
            .tls_serialize_detached()
            .map_err(|_| anyhow!("Could not serialize application message"))?;

        Ok(message_bytes)
    }

    pub fn receive_application_message(
        &mut self,
        message_bytes: &[u8],
        profile: bool,
    ) -> Result<Vec<u8>> {
        let group = self
            .group
            .as_mut()
            .ok_or_else(|| anyhow!("Client is not in a group"))?;

        let mls_message_in = MlsMessageIn::tls_deserialize_exact(message_bytes)
            .map_err(|_| anyhow!("Could not deserialize incoming MLS message"))?;

        let protocol_message = mls_message_in
            .try_into_protocol_message()
            .map_err(|_| anyhow!("Expected a protocol message"))?;

        let processed_message = group.process_message(&self.crypto, protocol_message, profile)?;

        match processed_message.into_content() {
            ProcessedMessageContent::ApplicationMessage(application_message) => {
                Ok(application_message.into_bytes())
            }
            _ => Err(anyhow!("Expected an application message")),
        }
    }

    pub fn receive_commit(&mut self, commit_bytes: &[u8]) -> Result<CommitReceiveOutcome> {
        let is_own_pending_commit = self
            .pending_commit_bytes
            .as_ref()
            .map(|pending| pending.as_slice() == commit_bytes)
            .unwrap_or(false);

        if is_own_pending_commit {
            let (welcome_recipients, welcome_bytes, ratchet_tree_bytes) = {
                let group = self
                    .group
                    .as_mut()
                    .ok_or_else(|| anyhow!("Client is not in a group"))?;

                group
                    .merge_pending_commit(&self.crypto)
                    .map_err(|_| anyhow!("Could not merge local pending commit"))?;

                let welcome_recipients = std::mem::take(&mut self.pending_welcome_recipients);
                let welcome_bytes = self.pending_welcome_bytes.take();

                let ratchet_tree_bytes = if welcome_bytes.is_some() {
                    let ratchet_tree: RatchetTreeIn = group.export_ratchet_tree().into();
                    let bytes = ratchet_tree
                        .tls_serialize_detached()
                        .map_err(|_| anyhow!("Could not serialize ratchet tree"))?;
                    print_bytes(&format!("{} ratchet tree", self.name), &bytes);
                    Some(bytes)
                } else {
                    None
                };

                (welcome_recipients, welcome_bytes, ratchet_tree_bytes)
            };

            self.pending_commit_bytes = None;
            let self_removed = self.cleanup_if_removed()?;

            return Ok(CommitReceiveOutcome::OwnCommitAccepted {
                self_removed,
                welcome_recipients,
                welcome_bytes,
                ratchet_tree_bytes,
            });
        }

        {
            let group = self
                .group
                .as_mut()
                .ok_or_else(|| anyhow!("Client is not in a group"))?;

            let mls_message_in = MlsMessageIn::tls_deserialize_exact(commit_bytes)
                .map_err(|_| anyhow!("Could not deserialize incoming commit"))?;

            let protocol_message = mls_message_in
                .try_into_protocol_message()
                .map_err(|_| anyhow!("Expected a protocol message"))?;

            let processed_message = group.process_message(&self.crypto, protocol_message, false)?;

            match processed_message.into_content() {
                ProcessedMessageContent::StagedCommitMessage(staged_commit) => {
                    group
                        .merge_staged_commit(&self.crypto, *staged_commit)
                        .map_err(|_| anyhow!("Could not merge staged commit"))?;
                }
                _ => return Err(anyhow!("Expected a staged commit message")),
            }
        }

        let self_removed = self.cleanup_if_removed()?;

        Ok(CommitReceiveOutcome::ExternalCommitApplied { self_removed })
    }
}