# Rig отдельно: состояние, история, контекст

Дата 2026-11-06. Source research, не выбор framework. Published rig-memory 0.46.0, MIT; .cargo_vcs_info.json указывает 6e039cd24994bc1e7d963af6f6278df73a81b130. Main HEAD при сверке scout: 9c372fc72669dc40430631002f1119e0ab17a9ef; чтения были по /main, не каждый файл независимо сопоставлен SHA. Main уже содержит post-release breaking fix(memory)! #3095: replaced_hash/Provenance и проверки cross-reset undo отсутствуют в v0.46.0. Released Compacted содержит summary/replaced/state; main proof semantics нельзя приписывать release. Parent отдельно проверил опубликованные Store source/Cargo.toml.orig/vcs info. Runtime/build proof отсутствует.

## Назначение

Rig не только provider API: отдельный rig-memory содержит conversation logs, folding/history/as_of, fork/compaction/store. Это дополняет раннее исследование только provider/MCP. Полный Rig runtime не требуется нашему продукту.

## Подтверждённый release-контракт

Store/AsyncStore append_log(name,len,bytes) сравнивает длину лога и пишет одним шагом для всех writers, включая процессы. При mismatch возвращает LogWriteError::Changed без записи. Контракт отдельно предупреждает ABA: лог, уменьшенный и выросший до той же длины, не отличим. replace_log обещает atomic/durable replace; append допускает torn last line с последующим recovery. Не выдавать контракт trait за доказанную реализацию любого backend.

rig-memory 0.46.0 default features пусты, fs/test-utils optional; зависит от rig-core без default features плюс serde/json/sha2/base64/memchr/thiserror. Это библиотечный срез, не обязательный отдельный server. Scout отдельно сверил released root Cargo.toml и crates.io metadata: rust-version 1.97.1, edition 2024. Transitive closure и доступность toolchain проверяются перед внедрением, никаких upgrades не выполнялось.

## Дополнительные main-наблюдения scout

Conversation changes и lineage/revision guards, fork по turn boundary, summary через splice с aside/replaced IDs, история/as_of/undo; SQLite adapter хранит log chunks, не реляционные message rows. Sans-I/O AgentRun выдаёт CallModel/CallTools/Done, host владеет драйвером и persistence; повторный CallModel не обязательно новый attempt, late responses надо отсеивать host. Engine save после successful run не выполняет наш intent-before-effect contract. Effect cassette log описан как replay recording, не restore authority.

Message model хранит canonical content и native provider items с Origin/fingerprint. Повторное использование native ограничено целевым model route и неизменностью content. Fingerprint — consistency marker, не криптографическая подпись и не authority. History adaptation включает потерю opaque/изображений при несовместимости, placeholders unanswered calls, удаление orphan results. Это donor behavior, не наши accepted правила.

## Предложение parent

Не подключать второй loop и не заменять SQLite rows log-chunk authority. Взять адресно идеи:

- Revision-checked изменение проекции: expected branch/projection revision проверяется в SQLite transaction; stale compaction не меняет актуальную ветку. Не нужен второй byte-length log только ради CAS.
- Сохраняемые summary provenance/input IDs; archive отдельно. Undo/branch — по нашему контракту, не обещание полного переноса Rig as_of.
- Canonical/native separation и explicit origin полезны для provider replay. Native payload сохранить в archive, даже если wire target его не принимает. Silent media downgrade/opaque drop не переносить в D41; несовместимость должна быть видна.
- Call/result pairing validation пригодна, но NO_RESULT_PROVIDED не должен маскировать unknown effect. Runtime outcome хранится отдельно от искусственного provider wire placeholder; правила конверсии ещё не выбраны.
- Sans-I/O форма run полезна как reference по state transitions, но wholesale migration OMP AgentLoop не обоснована. Durable deliveries и call intents реализуются в нашем session owner; Rig CAS store их не предоставляет автоматически.

## Границы и проверки

SQLite authority физически может хранить и логи, но это не делает donor chunk schema готовой нашей branch/call/delivery моделью. Противоречие — не с SQLite как engine, а с полным переносом чужой authority model. Предпочтение reuse не оправдывает дублирование live JSONL.

Android/Termux build и сериализация resumed run не проверены; Linux aarch64 support не равно Bionic. Optional transport/rig-sqlite/rig-lancedb могут добавлять native зависимости и version mismatches, их не включать автоматически. Не утверждать стоимость сборки по LOC.

Нужны targeted proof после разрешения: stale projection CAS; torn save recovery выбранного backend; compaction/fork pairing; native signature replay после edit/model switch; partial stream и duplicate tool result; cross-version resume envelope. Heavy Android build только CI. Ни одна проверка здесь не запускалась.

## Решения

Rig — дополнительный source donor с полезными state/replay механизмами. Не принят как обязательная зависимость или новый authority. Нового продуктового вопроса нет; exact extraction проверяется по pinned release source перед кодированием.

## Источники

- https://docs.rs/rig_memory/0.46.0/rig_memory/
- https://docs.rs/crate/rig-memory/0.46.0/source/src/store.rs
- https://docs.rs/crate/rig-memory/0.46.0/source/Cargo.toml.orig
- https://docs.rs/crate/rig-memory/0.46.0/source/.cargo_vcs_info.json
- https://github.com/0xPlaygrounds/rig

Полный main-source evidence scout: local://rig-state-research.md; не считать sections «брать почти целиком» выбором parent.
