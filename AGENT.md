# AGENT.md — Build `Signal_benchmark` from `OpenMLS_benchmark`

## 0. Mission

You are the primary implementation agent for `Signal_benchmark`.

Your task is to create a benchmark repository named `Signal_benchmark` that is structurally and operationally modeled after the sibling repository `../OpenMLS_benchmark`, but whose cryptographic protocol implementation is Signal Protocol using the locally available libsignal Rust source.

The goal is **comparative E2EE benchmarking**:

- Keep the benchmark scaffold, orchestration, containerization, worker layout, staircase/plateau runner, external IoT-device support, and aggregation workflow as close as reasonably possible to `OpenMLS_benchmark`.
- Replace all MLS/OpenMLS-specific protocol logic with Signal-specific protocol logic.
- Produce Signal-specific measurements, not MLS measurements with renamed labels.
- Use **vanilla pairwise Signal Protocol only**.
- Do **not** implement Signal Sender Keys, group sender keys, group encryption, MLS-like epochs, MLS commits, MLS Welcome messages, MLS ratchet trees, or any cryptographic group state abstraction.

This repository must answer the question:

> Under the same distributed benchmark scaffold used for OpenMLS, what are the processing, communication, and scaling costs of pure pairwise Signal sessions as the simulated conversation size grows?

---

## 1. Repository assumptions

Assume the working directory is the root of `Signal_benchmark`.

Assume these sibling/local resources exist:

- `../OpenMLS_benchmark`  
  Read-only reference implementation. You may inspect it and copy scaffold code that is not MLS-specific.

- A local libsignal Rust repository, path to be discovered from the local filesystem or existing project notes.  
  Do not use a remote libsignal dependency.

You may copy non-protocol-specific benchmark infrastructure from `../OpenMLS_benchmark`, including scripts, Docker files, worker orchestration, HTTP retry logic, service metrics, compose generation, external-device wiring, staircase runner structure, CSV aggregation style, and plotting/analysis scaffolding.

You must rewrite or replace all MLS-specific semantics.

---

## 2. Non-negotiable semantic distinction

`OpenMLS_benchmark` measures an MLS group protocol.

`Signal_benchmark` measures many pairwise Signal sessions arranged into a simulated many-participant conversation.

In Signal, there is no MLS-style cryptographic group state. Therefore:

- There is no group epoch.
- There is no MLS commit.
- There is no Welcome message.
- There is no ratchet tree.
- There is no KeyPackage.
- There is no Delivery Service carrying MLS group commits.
- There is no “member added to a cryptographic group” operation.
- There is no single encrypted application message shared by a group.
- There is no Sender Key optimization.

The benchmark may still use a **conversation**, **participant set**, **active participant count**, or **recipient fanout** as an experimental scaffold. These are benchmark-level concepts, not Signal cryptographic group concepts.

When the benchmark target size is `N`, interpret it as:

> `N` active logical Signal identities participating in the simulated conversation. A broadcast message from one participant to the conversation requires `N - 1` independent pairwise Signal encryptions and `N - 1` independent pairwise decryptions, unless the scenario explicitly measures a single pair.

This is the central semantic rule of the project.

---

## 3. Terminology mapping

Use Signal terminology in all new code, API names, logs, metrics, docs, and comments.

### 3.1 Replace these MLS terms

| MLS/OpenMLS term | Signal_benchmark term | Notes |
|---|---|---|
| client | participant, device, worker-local identity | Use `participant` for benchmark-level identity. Use `device` only when modeling a concrete Signal device/address. Avoid ambiguous `client` unless preserving scaffold API names would otherwise create churn. |
| group | conversation, participant set, recipient set | A Signal “conversation” here is a benchmark abstraction over pairwise sessions, not a cryptographic group. |
| group size | participant count, conversation size, active participants, recipient fanout | Do not report `group_size` unless preserved only as a compatibility alias with clear documentation. Prefer `conversation_size` and `recipient_count`. |
| epoch | forbidden | There is no Signal epoch. Do not invent `signal_epoch`, `session_epoch`, or equivalent. |
| KeyPackage | prekey bundle | Signal recipients publish prekey bundles. |
| Delivery Service / DS | key repository, message relay, prekey repository | Split storage of prekey bundles from message relay semantics if the OpenMLS DS combines them. |
| commit | session establishment, pairwise session setup, session update, membership transition | Do not model a Signal add/remove as a commit. |
| Welcome | initial PreKeySignalMessage / session-init message | Use libsignal’s actual concepts. |
| ratchet tree | forbidden | Signal has pairwise Double Ratchet state, not a group tree. |
| member | participant | `member` is tolerable only in copied scaffold where it clearly means benchmark participant and is expensive to rename. New code should use `participant`. |
| add_members | add_participants / enroll_participants / establish_sessions_for_new_participants | The operation should publish prekey material and establish pairwise sessions, not create MLS commits. |
| remove_members | deactivate_participants / remove_participants_from_conversation | Removal means benchmark exclusion and optional local session deletion; it does not cause a Signal group rekey. |
| self_update | rotate_prekeys / refresh_signed_prekey / ratchet_message_round | Signal has prekey rotation and ratchet progression; do not imitate MLS self-update. |
| process_pending_commit | process_pending_messages / drain_relay / process_session_init_messages | Preserve the async/fanout scaffold, not the MLS meaning. |

### 3.2 Forbidden vocabulary in Signal-specific implementation

Do not introduce new Signal code, metrics, or docs containing these concepts except inside explicit comments describing what was intentionally removed from OpenMLS:

- `epoch`
- `commit`
- `welcome`
- `ratchet_tree`
- `tree_size`
- `group_info`
- `key_package`
- `mls_group`
- `MlsGroup`
- `OpenMls`
- `DeliveryService` when it means MLS DS
- `sender_key`
- `SenderKey`
- `group session`
- `group encryption`

It is acceptable for copied files or compatibility wrappers to contain legacy names temporarily, but the final implementation must minimize them and document any remaining compatibility names.

Before finishing, run a terminology audit:

```sh
rg -n "epoch|commit|welcome|ratchet_tree|tree_size|group_info|KeyPackage|key_package|MlsGroup|OpenMls|openmls|sender.?key|group session|group encryption" .
```

Every remaining match must be either:

1. in an explicit compatibility boundary,
2. in documentation explaining the OpenMLS-to-Signal replacement,
3. in ignored/generated/vendor code,
4. or removed.

---

## 4. What must stay as identical as possible

Preserve the benchmark design paradigm of `OpenMLS_benchmark`.

The following should remain functionally equivalent unless Signal semantics require a change:

- Docker/container layout.
- Compose generation.
- Runner-in-Docker mode.
- External IoT device support.
- Hybrid worker layout.
- Singleton workers and packed workers.
- Multiple logical workers within one physical container.
- Worker health checks.
- HTTP command API between runner, workers, relay/repository, and devices.
- Fanout/adaptive fanout controls.
- Startup/teardown batching.
- Plateau/staircase progression.
- CLI shape and scenario configuration where meaningful.
- Output directory structure.
- Per-worker metrics files.
- Aggregated CSV workflow.
- Profiling/trace collection workflow.
- Analysis notebook/script structure where not protocol-specific.

If the OpenMLS benchmark has changed since this file was written, inspect `../OpenMLS_benchmark` and preserve the current scaffold shape rather than following stale assumptions.

---

## 5. What must change

### 5.1 Protocol backend

Replace OpenMLS dependencies with local libsignal Rust dependencies. When profiling, DO NOT change anything about the Signal Protocol itself!

`Cargo.toml` must not depend on OpenMLS crates for the Signal benchmark.

Use path dependencies or workspace wiring to the local libsignal Rust repository. Discover the correct crate names from the local libsignal checkout. Do not guess crate names when the local source is inspectable.

The Signal protocol layer should be isolated behind a small benchmark-facing module so the runner does not directly depend on libsignal internals everywhere.

Recommended module boundaries, adjusted to fit the actual repo:

- `signal_participant` or `participant`
- `key_repository`
- `message_relay`
- `pairwise_session`
- `signal_metrics`
- `staircase_runner`

Keep transport/orchestration modules close to OpenMLS_benchmark unless protocol semantics require changes.

### 5.2 Conversation semantics

A “conversation” in this repository is a benchmark abstraction over pairwise Signal sessions.

At plateau size `N`:

- There are `N` active participants.
- Each active participant has a Signal identity/address.
- Pairwise sessions may exist between participant pairs depending on the chosen setup policy.
- For a broadcast-style application round, one sender sends `N - 1` independent pairwise Signal messages.
- Each recipient decrypts exactly one message for that broadcast round.
- The total operation cost should report both aggregate and per-recipient/per-pair values.

### 5.3 Participant addition

Adding participants must not be implemented as an MLS add commit.

For Signal, participant addition should mean:

1. Create or activate new Signal identity/registration for the participant.
2. Generate identity key material and prekey material.
3. Publish the participant’s prekey bundle to the key repository.
4. Establish pairwise sessions between the new participant and already-active participants according to the benchmark’s session-establishment policy.
5. Record the costs of prekey generation, prekey publication, prekey fetch, X3DH/session setup (modern libsignal uses PQXDH, so default to that if necessary), initial session-init message processing, and any resulting message-relay traffic.

Choose and document one of these policies:

- **Eager full mesh**: when a participant joins, establish pairwise sessions with all existing active participants immediately. This is closest to MLS “add member” as a scaling event, but it is not a Signal group add.
- **Lazy on first message**: publish prekeys at join time, but establish pairwise sessions only when messages require them. This reflects asynchronous Signal behavior more closely.

Prefer eager full mesh if the primary goal is direct comparability with an OpenMLS add-member plateau transition (!). If you choose lazy setup, make sure `results.csv` separates join publication costs from later first-message X3DH costs. 
Preferably, **Eager full mesh** should be the primary choice!

### 5.4 Participant removal

Removing participants must not trigger an MLS-style group rekey.

For Signal, removal means:

- Remove/deactivate the participant from the benchmark’s active conversation set.
- Stop including that participant in future recipient fanouts.
- Optionally delete local pairwise sessions involving that participant if the scenario explicitly measures deletion/cleanup cost.
- Record cleanup cost separately from cryptographic session-establishment or ratchet cost.

Do not claim that remaining participants are cryptographically rekeyed as a group. They are not.

### 5.5 Application messages

An application message round in a conversation of size `N` must be modeled as pairwise Signal message operations.

For one sender and `N - 1` recipients, measure:

- sender-side encryption count,
- recipient-side decryption count,
- total ciphertext bytes,
- per-recipient ciphertext bytes,
- sender wall time,
- aggregate recipient wall time,
- per-recipient latency distribution,
- number of session creations required by this round,
- number of existing sessions reused,
- number of ratchet steps,
- number of DH ratchet steps if detectable,
- number of symmetric-chain ratchet steps if detectable,
- prekey-message count versus ordinary signal-message count.

Do not produce a single group ciphertext and send it to all participants. That would be Sender Keys or an invented group mode, and is out of scope.

---

## 6. Metrics and `results.csv`

The `results.csv` schema must be Signal-specific.

You may keep the aggregation pipeline style from OpenMLS_benchmark, but the columns must not imply MLS semantics.

Recommended core columns:

- `run_id`
- `scenario`
- `phase`
- `operation`
- `conversation_size`
- `active_participants`
- `sender_id`
- `recipient_count`
- `pairwise_session_count`
- `new_sessions_created`
- `existing_sessions_reused`
- `prekey_bundles_generated`
- `prekey_bundles_published`
- `prekey_bundles_fetched`
- `one_time_prekeys_consumed`
- `x3dh_initiations` (modern libsignal uses PQXDH, so default to that if necessary)
- `x3dh_responses` (modern libsignal uses PQXDH, so default to that if necessary)
- `prekey_signal_messages_created`
- `prekey_signal_messages_processed`
- `signal_messages_created`
- `signal_messages_processed`
- `encrypt_ops`
- `decrypt_ops`
- `ratchet_steps_total`
- `dh_ratchet_steps`
- `symmetric_ratchet_steps`
- `plaintext_bytes`
- `ciphertext_bytes_total`
- `ciphertext_bytes_p50`
- `ciphertext_bytes_p95`
- `wall_ms`
- `cpu_thread_ns`
- `alloc_bytes`
- `alloc_count`
- `worker_latency_p50_ms`
- `worker_latency_p95_ms`
- `worker_latency_p99_ms`
- `worker_latency_max_ms`
- `logical_request_count`
- `physical_request_count`
- `singleton_request_count`
- `packed_request_count`
- `packed_logical_participant_count`
- `profile_enabled_recipient_count`
- `implementation`
- `libsignal_version_or_git`
- `rust_target`
- `device_kind`
- `container_mode`

Do not include these columns unless they are explicitly marked as legacy compatibility and always empty for Signal:

- `group_epoch`
- `epoch`
- `tree_size`
- `encrypted_group_info_bytes`
- `encrypted_secrets_count`
- `commit_bytes`
- `welcome_bytes`
- `ratchet_tree_bytes`
- `key_package_bytes`

If compatibility with existing plotting scripts requires a transitional column, name it clearly, for example:

- `conversation_size` as the canonical column,
- `legacy_group_size_alias` only if needed,
- never `group_epoch`.

---

## 7. Profiling requirements

Profiling must be performed as close to libsignal Rust protocol operations as practical (!).

Do not merely time HTTP handlers or the outer benchmark runner and call that Signal profiling. More precisely, any metric regarding HTTP networking is absolutely IRRELEVANT for the results.csv.

Add or adapt profiling around the libsignal operations that actually matter, such as:

- identity key generation,
- signed prekey generation,
- one-time prekey generation,
- prekey bundle serialization/deserialization,
- prekey fetch handling,
- X3DH/session initialization from a prekey bundle,
- processing an incoming prekey/session-init message,
- pairwise encryption,
- pairwise decryption,
- Double Ratchet message-key derivation,
- DH ratchet advancement if observable,
- symmetric ratchet advancement if observable,
- session store load/save,
- identity/prekey/session store operations.

Prefer small wrapper-level instrumentation if modifying libsignal internals is too invasive, but if the project requirement is to profile inside the local libsignal Rust library, implement the minimal local instrumentation there and expose structured profile events to the benchmark.

Profile event names must be Signal-specific, for example:

- `signal.identity_key.generate`
- `signal.prekey_bundle.generate`
- `signal.prekey_bundle.publish`
- `signal.prekey_bundle.fetch`
- `signal.x3dh.initiate`
- `signal.x3dh.respond`
- `signal.session.store_load`
- `signal.session.store_save`
- `signal.message.encrypt.prekey`
- `signal.message.decrypt.prekey`
- `signal.message.encrypt.signal`
- `signal.message.decrypt.signal`
- `signal.ratchet.step.symmetric`
- `signal.ratchet.step.dh`

Do not reuse OpenMLS profile event names unless they refer to protocol-independent infrastructure.

---

## 8. Key repository and relay semantics

In OpenMLS, the Delivery Service may combine several roles. In Signal_benchmark, separate the concepts even if they are implemented in one service for scaffold compatibility.

### Key repository

The key repository stores and serves Signal public prekey material.

It should support benchmark operations equivalent to:

- publish identity/prekey material for a participant,
- fetch a recipient’s prekey bundle,
- consume a one-time prekey when a prekey bundle is used,
- expose metrics for prekey publication, fetches, and depletion.

Use the name `key_repository`, `prekey_repository`, or similar. Avoid `delivery_service` unless preserving a copied transport boundary, and document the rename.

### Message relay

The message relay stores/forwards encrypted Signal messages between participants.

It should support:

- enqueue pairwise ciphertext,
- fetch pending messages for a participant,
- drain relay during benchmark phases,
- record fanout/network metrics.

The relay does not understand plaintext and does not perform cryptographic group operations.

---

## 9. Worker model

Preserve the OpenMLS worker model as much as possible:

- logical workers,
- packed workers,
- singleton workers,
- profile-enabled workers,
- external-device workers,
- physical worker IDs,
- worker layout JSON,
- health endpoints,
- command endpoints.

But the worker-local state must be Signal state:

- participant identity/address,
- identity key store,
- signed prekey store,
- one-time prekey store,
- session store,
- sender/receiver pairwise session state,
- pending encrypted messages.

Avoid naming worker-local state `group`.

---

## 10. Staircase/plateau semantics

Keep the staircase/plateau idea.

However, reinterpret plateaus as active participant counts, not MLS group sizes.

At each plateau:

1. Ensure `conversation_size == target active participant count`.
2. Add or deactivate participants as needed.
3. Ensure required pairwise sessions exist according to the chosen session-establishment policy.
4. Verify Signal-level convergence:
   - active participant set is consistent,
   - required pairwise sessions exist,
   - no pending session-init messages remain unless deliberately using lazy setup,
   - message relay has no unexpected backlog.
5. Run configured message rounds.
6. Aggregate Signal-specific metrics.

The log output should say things like:

- `Plateau target active participants = 256`
- `Established 255 new pairwise sessions for participant worker-0256`
- `Broadcast round: sender=worker-0001 recipients=255 encrypt_ops=255 decrypt_ops=255`

It must not say:

- `converged at epoch`
- `merged commit`
- `joined group from welcome`
- `exported ratchet tree`

---

## 11. Correctness checks

Implement correctness checks suited to Signal:

- A participant can publish and fetch a prekey bundle.
- Initiator can establish a session from a recipient prekey bundle.
- Recipient can process the initial prekey/session-init message.
- After session establishment, both sides can exchange encrypted messages.
- Broadcast fanout creates one pairwise ciphertext per recipient.
- Each intended recipient decrypts exactly its own ciphertext.
- Removed/deactivated participants do not receive future broadcast messages.
- Ordinary messages advance pairwise session state.
- Replaying the same message should fail or be explicitly handled according to libsignal behavior.
- Out-of-order handling should be tested if supported by the chosen relay behavior.
- One-time prekeys are consumed exactly once where applicable.
- The key repository never serves private key material.

Do not use MLS convergence checks such as matching group epoch or ratchet tree.

---

## 12. Safety and scope constraints

Do not rewrite the whole benchmark unless necessary.

Do not invent a new benchmark framework.

Do not replace Docker/compose orchestration with a different system.

Do not simplify away packed workers, external devices, or runner-in-Docker mode.

Do not use mock cryptography as the final implementation. Temporary mocks are allowed only during scaffolding and must be removed before completion.

Do not implement Sender Keys or any other Signal group optimization.

Do not make results look artificially comparable by mapping Signal onto fake MLS epochs or commits.

Do not hide semantic mismatches. Document them.

---

## 13. Implementation strategy

Work in this order:

1. Inspect `../OpenMLS_benchmark` read-only.
2. Identify scaffold files that can be copied with little or no change.
3. Identify MLS-specific files that must be rewritten.
4. Create or update `Signal_benchmark` structure to mirror the reference benchmark.
5. Replace Rust dependencies with local libsignal path dependencies.
6. Implement Signal participant/key repository/message relay/session wrapper modules.
7. Adapt worker API commands from MLS operations to Signal operations.
8. Adapt staircase runner semantics from group size to conversation size.
9. Adapt metrics/profile events to Signal-specific fields.
10. Adapt Docker/scripts/compose generation while preserving functionality.
11. Run format/build/tests.
12. Run small smoke benchmarks with at most 512 workers.
13. Run terminology audit.
14. Produce self-review.
15. Run external review cycle with Gemini and Codex as specified below.
16. Implement only confirmed/high-confidence reviewer findings.
17. Repeat the Gemini/Codex review cycle once.
18. Produce final report.

---

## 14. Build and smoke benchmark expectations

Run the project’s normal build/test commands as discovered locally.

At minimum, attempt:

```sh
cargo fmt --check
cargo check
cargo test
```

Then run one or more small smoke benchmarks. Keep them bounded:

- maximum 512 workers,
- short timeout,
- low round counts,
- small payload sizes,
- no uncontrolled long-running benchmark.

Prefer at least these smoke profiles if feasible:

1. Tiny local correctness:
   - 2 to 8 participants,
   - one add/enroll step,
   - one message round,
   - no external device required.

2. Small staircase:
   - e.g. 2 → 16 or 2 → 32 participants,
   - one message round per plateau,
   - one session establishment policy.

3. Optional external-device smoke:
   - only if devices are configured and available,
   - do not block final completion if external devices are absent.
   - but, for this implementation, you can assume the LUCKFOX pico plus is connected.

Capture logs and results in a review artifact directory, for example:

```sh
mkdir -p /tmp/signal-benchmark-review
git diff > /tmp/signal-benchmark-review/agent.diff
git status --short > /tmp/signal-benchmark-review/status.txt
```

Store benchmark logs/results there as well.

---

## 15. Self-review requirements

Before asking sub-agents to review, write your own review to:

```sh
/tmp/signal-benchmark-review/opencode-self-review.md
```

Your self-review must answer these questions directly:

1. Is this still effectively a carbon copy of `OpenMLS_benchmark`, or does it produce Signal-specific data?
2. Where exactly does the implementation use libsignal Rust?
3. Which operations are true Signal operations?
4. Which copied scaffold parts remain protocol-independent?
5. Does any MLS concept remain in code, logs, metrics, or docs?
6. Is `conversation_size` defined correctly as active Signal participants rather than MLS group size?
7. Does one broadcast to `N` participants perform `N - 1` pairwise encryptions?
8. Are Sender Keys absent?
9. Are fake epochs absent?
10. Are results.csv columns Signal-specific?
11. Are profile events emitted around libsignal operations rather than only HTTP scaffolding?
12. What correctness tests passed?
13. What smoke benchmarks ran?
14. What risks remain?

Be skeptical. Do not write a celebratory summary.

---

## 16. Gemini review cycle

Run Gemini as a review-only sub-agent after your implementation and self-review.

Gemini must not edit files and must not provide code patches. It should provide findings, risks, and suggested changes in prose.

Use the locally available Gemini invocation style. If headless mode is available, prefer it. Save output to:

```sh
/tmp/signal-benchmark-review/gemini-review-round-1.md
```

Prompt Gemini with this content, adjusted only for local paths/CLI syntax:

```text
You are a REVIEW-ONLY agent. Do not edit files. Do not write code patches.

Repository: Signal_benchmark.
Reference repository: ../OpenMLS_benchmark.

Task context:
This repository should be a Signal Protocol benchmark heavily inspired by OpenMLS_benchmark. It must preserve the benchmark scaffold where possible, but replace MLS/OpenMLS cryptographic semantics with vanilla pairwise Signal Protocol using local libsignal Rust. No Sender Keys. No MLS epochs. No commits. No Welcome messages. No ratchet trees.

Read:
- AGENT.md
- git diff
- /tmp/signal-benchmark-review/opencode-self-review.md
- smoke benchmark logs/results under /tmp/signal-benchmark-review/

Answer these questions directly:

1. Is this just a carbon copy of OpenMLS_benchmark, or does it really produce Signal-specific data?
2. Are any MLS concepts still present in code, logs, metrics, CSV fields, APIs, or docs?
3. Does the benchmark correctly interpret plateau/group size as Signal conversation_size / active participants?
4. Does a message to N participants require N-1 pairwise Signal encryptions and N-1 decryptions?
5. Are X3DH/session setup costs measured separately from normal Double Ratchet message costs?
6. Are prekey bundle generation, publication, fetch, and one-time prekey consumption measured?
7. Are ratchet steps measured or at least approximated honestly?
8. Is profiling close enough to libsignal Rust operations?
9. Are Sender Keys or group-message shortcuts absent?
10. Are removal/deactivation semantics honest for Signal?
11. Are the CSV/results columns Signal-specific and useful for comparison against OpenMLS?
12. Are the smoke benchmarks sufficient to prove basic correctness?
13. What are the top correctness risks?
14. What are the top measurement-validity risks?
15. What changes should the primary agent make next?

Do not provide code. Provide prioritized findings:
- Critical
- High
- Medium
- Low
- Likely false positives / needs human decision
```

---

## 17. Codex review cycle

After Gemini review round 1, run Codex as a review-only synthesizer.

Codex must not edit files. Prefer read-only/non-interactive mode.

Save output to:

```sh
/tmp/signal-benchmark-review/codex-review-round-1.md
```

Prompt Codex with this content, adjusted only for local CLI syntax:

```text
You are a REVIEW-ONLY agent. Do not modify files.

Repository: Signal_benchmark.
Reference repository: ../OpenMLS_benchmark.

Read:
- AGENT.md
- git diff
- /tmp/signal-benchmark-review/opencode-self-review.md
- /tmp/signal-benchmark-review/gemini-review-round-1.md
- smoke benchmark logs/results under /tmp/signal-benchmark-review/

Your job:
Synthesize a final technical review of whether Signal_benchmark is a faithful Signal-specific analogue of OpenMLS_benchmark.

Be skeptical of both the implementation and Gemini's review.

Answer directly:
1. Does the implementation preserve benchmark scaffold comparability?
2. Does it avoid fake MLS concepts such as epoch, commit, Welcome, ratchet tree, KeyPackage?
3. Does it correctly model vanilla pairwise Signal without Sender Keys?
4. Are X3DH, prekey bundle, session setup, encrypt/decrypt, and ratchet costs measured in meaningful places?
5. Are results.csv fields semantically correct?
6. Are benchmark phases named accurately?
7. Are smoke benchmarks adequate?
8. Which Gemini findings are correct?
9. Which Gemini findings are likely false positives?
10. What should the primary agent implement next?

Do not provide code. Provide:
- Confirmed issues
- Rejected/uncertain issues
- Recommended refactoring
- Recommended metric changes
- Recommended tests
- Suggested implementation order
```

If using Codex CLI, a safe pattern is:

```sh
codex exec \
  --cd "$REPO" \
  --sandbox read-only \
  --ask-for-approval never \
  "Review this repository using the prompt in /tmp/signal-benchmark-review/codex-prompt-round-1.txt"
```

If the installed Codex version requires global flags before `exec`, adapt the command accordingly.

---

## 18. Primary-agent refactor after round 1

After Gemini and Codex round 1:

1. Read both reviews.
2. Do not blindly implement all suggestions.
3. Triage every finding as:
   - accepted,
   - rejected,
   - deferred,
   - unclear/human decision needed.
4. Implement only accepted findings that are correct and within scope.
5. Prefer semantic fixes over cosmetic renaming.
6. Rerun build/tests/smoke benchmarks.
7. Update self-review with a section:
   - `Round 1 reviewer findings addressed`
   - `Round 1 reviewer findings rejected/deferred`

---

## 19. Repeat review cycle once

After the round 1 refactor, repeat the review cycle exactly once:

- Save Gemini output to:
  - `/tmp/signal-benchmark-review/gemini-review-round-2.md`

- Save Codex output to:
  - `/tmp/signal-benchmark-review/codex-review-round-2.md`

Prompts should include:

- current `AGENT.md`,
- current `git diff`,
- original self-review,
- updated self-review,
- round 1 Gemini review,
- round 1 Codex review,
- benchmark/test logs after refactor.

Ask round 2 reviewers to focus on:

1. Whether round 1 fixes actually addressed the semantic issues.
2. Whether new regressions were introduced.
3. Whether any MLS terminology or fake semantics remain.
4. Whether results and profile metrics are now genuinely Signal-specific.
5. Whether the benchmark is ready for human review.

After round 2, do one final triage and implement only high-confidence, in-scope fixes.

---

## 20. Final deliverables

At completion, produce a final report in the repository root, for example:

```text
SIGNAL_BENCHMARK_IMPLEMENTATION_REPORT.md
```

The report must include:

- Summary of preserved OpenMLS scaffold components.
- Summary of replaced MLS components.
- Explanation of Signal conversation-size semantics.
- Explanation of pairwise fanout cost model.
- Explanation of participant addition/removal semantics.
- `results.csv` schema description.
- Profiling event schema description.
- Build/test commands run.
- Smoke benchmark commands run.
- Key benchmark outputs.
- Gemini/Codex review findings accepted/rejected.
- Known limitations.
- Remaining human-review questions.

Also ensure:

- `README.md` describes the Signal benchmark accurately.
- Commands shown in README are runnable or clearly marked as examples.
- Any remaining legacy names are documented.
- The repository does not claim Signal has MLS-like group epochs or group commits.

---

## 21. Final acceptance checklist

Before declaring success, verify:

- [ ] `Cargo.toml` no longer uses OpenMLS as the Signal protocol backend.
- [ ] Local libsignal Rust source is used.
- [ ] Containerization still works.
- [ ] Compose generation still works.
- [ ] Runner-in-Docker still works if it existed in the reference.
- [ ] External-device support is preserved if it existed in the reference.
- [ ] Packed and singleton workers are preserved.
- [ ] Staircase/plateau progression works.
- [ ] Plateau size means active Signal participants/conversation size.
- [ ] Broadcast to `N` participants performs `N - 1` pairwise Signal sends.
- [ ] Participant addition uses prekey bundles and pairwise session setup.
- [ ] Removal/deactivation does not claim group rekeying.
- [ ] Sender Keys are absent.
- [ ] MLS epoch/commit/Welcome/ratchet-tree semantics are absent.
- [ ] `results.csv` is Signal-specific.
- [ ] Profiling is near libsignal protocol operations.
- [ ] Smoke benchmark with at most 512 workers has run or a clear reason is documented.
- [ ] Gemini review round 1 completed.
- [ ] Codex review round 1 completed.
- [ ] Round 1 refactor completed.
- [ ] Gemini review round 2 completed.
- [ ] Codex review round 2 completed.
- [ ] Final triage completed.
- [ ] Final implementation report exists.

---

## 22. The core rule

When in doubt, preserve the benchmark scaffold, not the MLS semantics.

Signal_benchmark should feel operationally like the OpenMLS benchmark harness, but cryptographically and metrically it must be Signal:

> many identities, many prekey bundles, many pairwise sessions, many ratchets, many per-recipient ciphertexts — no group epoch, no group commit, no Sender Keys.
