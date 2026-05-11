use std::future::Future;
use std::time::SystemTime;

use anyhow::{Context, Result};
use libsignal_core::DeviceId;
use libsignal_core::ProtocolAddress;
use libsignal_protocol::kem;
use libsignal_protocol::GenericSignedPreKey;
use libsignal_protocol::Timestamp;
use libsignal_protocol::{
    message_decrypt, message_encrypt, process_prekey_bundle, CiphertextMessage, IdentityKey,
    IdentityKeyPair, InMemSignalProtocolStore, KyberPreKeyId, KyberPreKeyRecord, KyberPreKeyStore,
    PreKeyBundle, PreKeyId, PreKeyRecord, PreKeyStore, SessionStore, SignedPreKeyId,
    SignedPreKeyRecord, SignedPreKeyStore,
};
use rand::rand_core::TryRngCore;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::debug::print_bytes;

const DEFAULT_ONE_TIME_PREKEY_COUNT: u32 = 1024;
const DEVICE_ID: u8 = 1;

pub struct SignalParticipant {
    pub name: String,
    pub address: ProtocolAddress,
    pub store: InMemSignalProtocolStore,
    pub identity_key_pair: IdentityKeyPair,
    registration_id: u32,
    signed_prekey_id: SignedPreKeyId,
    signed_prekey_record: SignedPreKeyRecord,
    kyber_prekey_id: KyberPreKeyId,
    kyber_prekey_record: KyberPreKeyRecord,
    one_time_prekey_ids: Vec<PreKeyId>,
    one_time_prekey_records: Vec<PreKeyRecord>,
    csprng: StdRng,
}

fn new_csprng() -> StdRng {
    let mut seed = <StdRng as SeedableRng>::Seed::default();
    rand::rand_core::OsRng
        .try_fill_bytes(seed.as_mut())
        .expect("OsRng entropy");
    StdRng::from_seed(seed)
}

fn block_on_protocol<F: Future>(future: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(future))
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("protocol runtime")
            .block_on(future)
    }
}

impl SignalParticipant {
    pub fn new(name: &str) -> Result<Self> {
        let device_id = DeviceId::new(DEVICE_ID).expect("DEVICE_ID is valid");
        let address = ProtocolAddress::new(name.to_string(), device_id);
        let mut csprng = new_csprng();

        let identity_key_pair = IdentityKeyPair::generate(&mut csprng);
        let registration_id: u32 = csprng.random();

        let signed_prekey_id = SignedPreKeyId::from(1u32);
        let signed_prekey = libsignal_core::curve::KeyPair::generate(&mut csprng);
        let msg = signed_prekey.public_key.serialize();
        let signed_prekey_signature = identity_key_pair
            .private_key()
            .calculate_signature(&msg, &mut csprng)?;
        let signed_prekey_record = SignedPreKeyRecord::new(
            signed_prekey_id,
            Timestamp::from_epoch_millis(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .context("system time")?
                    .as_millis() as u64,
            ),
            &signed_prekey,
            &signed_prekey_signature,
        );

        let store = InMemSignalProtocolStore::new(identity_key_pair, registration_id)?;

        let kyber_prekey_id = KyberPreKeyId::from(1u32);
        let kyber_key_pair = kem::KeyPair::generate(kem::KeyType::Kyber1024, &mut csprng);
        let kyber_msg = kyber_key_pair.public_key.serialize();
        let kyber_signature = identity_key_pair
            .private_key()
            .calculate_signature(&kyber_msg, &mut csprng)?;
        let kyber_prekey_record = KyberPreKeyRecord::new(
            kyber_prekey_id,
            Timestamp::from_epoch_millis(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .context("system time")?
                    .as_millis() as u64,
            ),
            &kyber_key_pair,
            &kyber_signature,
        );

        let one_time_prekey_count = std::env::var("SIGNAL_ONE_TIME_PREKEY_COUNT")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|count| *count > 0)
            .unwrap_or(DEFAULT_ONE_TIME_PREKEY_COUNT);

        let mut one_time_prekey_ids = Vec::new();
        let mut one_time_prekey_records = Vec::new();
        for i in 1..=one_time_prekey_count {
            let prekey_id = PreKeyId::from(i);
            let key_pair = libsignal_core::curve::KeyPair::generate(&mut csprng);
            let record = PreKeyRecord::new(prekey_id, &key_pair);
            one_time_prekey_ids.push(prekey_id);
            one_time_prekey_records.push(record);
        }

        Ok(Self {
            name: name.to_string(),
            address,
            store,
            identity_key_pair,
            registration_id,
            signed_prekey_id,
            signed_prekey_record,
            kyber_prekey_id,
            kyber_prekey_record,
            one_time_prekey_ids,
            one_time_prekey_records,
            csprng,
        })
    }

    pub fn generate_prekey_bundles(&mut self) -> Result<Vec<PreKeyBundle>> {
        let mut bundles = Vec::with_capacity(self.one_time_prekey_records.len() + 1);

        let identity_public = *self.identity_key_pair.identity_key().public_key();
        let signed_prekey = self.signed_prekey_record.public_key()?;
        let signed_prekey_signature = self.signed_prekey_record.signature()?.to_vec();
        let registration_id = self.registration_id;
        let kyber_prekey_public = self.kyber_prekey_record.public_key()?;
        let kyber_signature = self.kyber_prekey_record.signature()?.to_vec();

        for (prekey_id, prekey_record) in self
            .one_time_prekey_ids
            .iter()
            .copied()
            .zip(self.one_time_prekey_records.iter())
        {
            bundles.push(PreKeyBundle::new(
                registration_id,
                DeviceId::new(DEVICE_ID).expect("DEVICE_ID is valid"),
                Some((prekey_id, prekey_record.public_key()?)),
                self.signed_prekey_id,
                signed_prekey,
                signed_prekey_signature.clone(),
                self.kyber_prekey_id,
                kyber_prekey_public.clone(),
                kyber_signature.clone(),
                IdentityKey::new(identity_public),
            )?);
        }

        bundles.push(PreKeyBundle::new(
            registration_id,
            DeviceId::new(DEVICE_ID).expect("DEVICE_ID is valid"),
            None,
            self.signed_prekey_id,
            signed_prekey,
            signed_prekey_signature,
            self.kyber_prekey_id,
            kyber_prekey_public,
            kyber_signature,
            IdentityKey::new(identity_public),
        )?);

        print_bytes(
            &format!("{} PreKeyBundle identity", self.name),
            &identity_public.serialize(),
        );

        Ok(bundles)
    }

    pub fn generate_prekey_bundle(&mut self) -> Result<PreKeyBundle> {
        self.generate_prekey_bundles()?
            .into_iter()
            .next()
            .context("no prekey bundles generated")
    }

    pub fn consume_one_time_prekey(&mut self) {
        if !self.one_time_prekey_ids.is_empty() {
            self.one_time_prekey_ids.remove(0);
            self.one_time_prekey_records.remove(0);
        }
    }

    pub fn store_own_prekeys(&mut self) -> Result<()> {
        block_on_protocol(async {
            for (prekey_id, record) in self
                .one_time_prekey_ids
                .iter()
                .copied()
                .zip(self.one_time_prekey_records.iter())
            {
                self.store
                    .pre_key_store
                    .save_pre_key(prekey_id, record)
                    .await?;
            }
            self.store
                .signed_pre_key_store
                .save_signed_pre_key(self.signed_prekey_id, &self.signed_prekey_record)
                .await?;
            self.store
                .kyber_pre_key_store
                .save_kyber_pre_key(self.kyber_prekey_id, &self.kyber_prekey_record)
                .await?;
            Ok(())
        })
    }

    pub fn establish_session_from_bundle(
        &mut self,
        remote_address: &ProtocolAddress,
        bundle: &PreKeyBundle,
    ) -> Result<()> {
        block_on_protocol(async {
            process_prekey_bundle(
                remote_address,
                &self.address,
                &mut self.store.session_store,
                &mut self.store.identity_store,
                bundle,
                SystemTime::now(),
                &mut self.csprng,
            )
            .await
        })?;

        Ok(())
    }

    pub fn encrypt_message(
        &mut self,
        remote_address: &ProtocolAddress,
        plaintext: &[u8],
    ) -> Result<CiphertextMessage> {
        let msg = block_on_protocol(async {
            message_encrypt(
                plaintext,
                remote_address,
                &self.address,
                &mut self.store.session_store,
                &mut self.store.identity_store,
                SystemTime::now(),
                &mut self.csprng,
            )
            .await
        })?;

        Ok(msg)
    }

    pub fn decrypt_message(
        &mut self,
        remote_address: &ProtocolAddress,
        ciphertext: &CiphertextMessage,
    ) -> Result<Vec<u8>> {
        let plaintext = block_on_protocol(async {
            message_decrypt(
                ciphertext,
                remote_address,
                &self.address,
                &mut self.store.session_store,
                &mut self.store.identity_store,
                &mut self.store.pre_key_store,
                &self.store.signed_pre_key_store,
                &mut self.store.kyber_pre_key_store,
                &mut self.csprng,
            )
            .await
        })?;

        Ok(plaintext)
    }

    pub fn has_session_with(&self, remote_address: &ProtocolAddress) -> bool {
        block_on_protocol(async {
            self.store
                .session_store
                .load_session(remote_address)
                .await
                .ok()
                .flatten()
                .is_some()
        })
    }

    pub fn remaining_prekeys(&self) -> usize {
        self.one_time_prekey_ids.len()
    }

    pub fn private_key(&self) -> &libsignal_core::curve::PrivateKey {
        self.identity_key_pair.private_key()
    }
}
