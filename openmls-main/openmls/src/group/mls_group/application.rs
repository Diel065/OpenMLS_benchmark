use openmls_traits::signatures::Signer;
use tls_codec::Serialize as _;

use crate::storage::OpenMlsProvider;

use super::{errors::CreateMessageError, *};

#[cfg(feature = "profiling-json")]
use allocation_counter::measure;

#[cfg(feature = "profiling-json")]
use crate::profiling::{emit_event, ProfileScope};

impl MlsGroup {
    // === Application messages ===

    /// Creates an application message.
    /// Returns `CreateMessageError::MlsGroupStateError::UseAfterEviction`
    /// if the member is no longer part of the group.
    /// Returns `CreateMessageError::MlsGroupStateError::PendingProposal` if pending proposals
    /// exist. In that case `.process_pending_proposals()` must be called first
    /// and incoming messages from the DS must be processed afterwards.
    pub fn create_message<Provider: OpenMlsProvider>(
        &mut self,
        provider: &Provider,
        signer: &impl Signer,
        message: &[u8],
    ) -> Result<MlsMessageOut, CreateMessageError> {
        if !self.is_active() {
            return Err(CreateMessageError::GroupStateError(
                MlsGroupStateError::UseAfterEviction,
            ));
        }
        if !self.proposal_store().is_empty() {
            return Err(CreateMessageError::GroupStateError(
                MlsGroupStateError::PendingProposal,
            ));
        }

        #[cfg(feature = "profiling-json")]
        let scope = ProfileScope::start("application_message_create", "openmls");

        #[cfg(feature = "profiling-json")]
        let aad_len = self.aad.len();

        #[cfg(feature = "profiling-json")]
        let plaintext_len = message.len();

        #[cfg(feature = "profiling-json")]
        let group_epoch = self.context().epoch().as_u64();

        #[cfg(feature = "profiling-json")]
        let tree_size = self.treesync().tree_size().u32();

        #[cfg(feature = "profiling-json")]
        let member_count = self.members().count();

        #[cfg(feature = "profiling-json")]
        let ciphersuite = format!("{:?}", self.ciphersuite());

        #[cfg(feature = "profiling-json")]
        let mut measured_result: Option<Result<(MlsMessageOut, Option<usize>), CreateMessageError>> =
            None;

        #[cfg(feature = "profiling-json")]
        let allocation_info = measure(|| {
            measured_result = Some((|| -> Result<(MlsMessageOut, Option<usize>), CreateMessageError> {
                let authenticated_content = AuthenticatedContent::new_application(
                    self.own_leaf_index(),
                    &self.aad,
                    message,
                    self.context(),
                    signer,
                )?;
                let ciphertext = self
                    .encrypt(authenticated_content, provider)
                    // We know the application message is wellformed and we have the key material of the current epoch
                    .map_err(|_| LibraryError::custom("Malformed plaintext"))?;

                let ciphertext_size_bytes = ciphertext
                    .tls_serialize_detached()
                    .map(|v| v.len())
                    .ok();

                self.reset_aad();
                Ok((
                    MlsMessageOut::from_private_message(ciphertext, self.version()),
                    ciphertext_size_bytes,
                ))
            })());
        });

        #[cfg(feature = "profiling-json")]
        {
            let (message_out, ciphertext_size_bytes) =
                measured_result.expect("allocation_counter measure closure did not run")?;

            let artifact_size_bytes = message_out
                .tls_serialize_detached()
                .map(|v| v.len())
                .ok();

            if let Some(scope) = scope {
                let mut event = scope.finish();
                event.group_epoch = Some(group_epoch);
                event.tree_size = Some(tree_size);
                event.member_count = Some(member_count);
                event.ciphersuite = Some(ciphersuite);
                event.alloc_bytes = Some(allocation_info.bytes_total as u64);
                event.alloc_count = Some(allocation_info.count_total as u64);
                event.artifact_size_bytes = artifact_size_bytes;
                event.app_msg_plaintext_bytes = Some(plaintext_len);
                event.app_msg_ciphertext_bytes = ciphertext_size_bytes;
                event.aad_bytes = Some(aad_len);
                emit_event(&event);
            }

            return Ok(message_out);
        }

        #[cfg(not(feature = "profiling-json"))]
        {
            let authenticated_content = AuthenticatedContent::new_application(
                self.own_leaf_index(),
                &self.aad,
                message,
                self.context(),
                signer,
            )?;
            let ciphertext = self
                .encrypt(authenticated_content, provider)
                // We know the application message is wellformed and we have the key material of the current epoch
                .map_err(|_| LibraryError::custom("Malformed plaintext"))?;

            self.reset_aad();
            Ok(MlsMessageOut::from_private_message(
                ciphertext,
                self.version(),
            ))
        }
    }
}