# Ревизия предложений до реализации

Дата 2026-11-06. Оценка трёх переданных пакетов, включая самокоррекцию D84–D93. Не accepted decisions; номера D84–D93 не зарезервированы. D01–D83 не изменены. Source-only: no install/build/runtime/security certification. Планировщик получил безопасные предпосылки и полный согласованный scope; кодирование не разрешено.

## Вывод

Не принимать пакет целиком. Принять в предварительное проектирование раздельные real workspace и synthetic URI, собственный interpreter state агента, единый SQLite transaction owner, event-driven client notifications и существующие библиотечные engines. Не менять D82/топологию/defaults/форматы автоматически. jj-lib, CAS, Cedar, конкретный shell/backend и wire format остаются gated candidates.

## 1. Workspace, URI, снимки и merge

Рекомендуется различать managed data root (раскладка), workspace (реальный cwd/файлы/snapshot/ownership), synthetic resource router (artifact ranges, history/context, plan/memory views), execution boundary (OS enforcement). Обычные файлы не требуют универсального VFS proxy; engine FS adapter допустим, но не создаёт вторую authority или копию дерева. URI routing также проверяет права. Имена vfs:// и layout cas/ не выбраны.

D82 all-project visibility сохраняется. Чтение всех проектов не разрешает запись; runtime-controlled mutation проверяет workspace ownership. Произвольный subprocess пока не объявлен ограниченным. Delta scan после shell может показать изменения, но не предотвратить повреждение, не увидеть изменение-восстановление и не доказать автора при внешнем редакторе. Whole-root scan на каждый шаг имеет необоснованную цену. Не превращать его в security gate.

jj-lib 0.44.0 source: LockedWorkingCopy::snapshot возвращает MergedTree/SnapshotStats; commit создаёт caller. Colocated CLI reset_head меняет HEAD/index. Прямой driver может не вызывать reset_head/import/export; colocated backend всё равно пишет objects/keep refs в пользовательский .git. Private Workspace::init_internal_git применим к обычной папке без пользовательского Git. Checkout materializes файлы, не обещает reflink/COW; хранятся executable bit, UTF8 paths/targets, API версий меняется. Поэтому ни «бесплатная полная замена», ни «невозможно не трогать HEAD» не верны. Candidate gate: private metadata location, dirty/non-Git/untracked/ignore snapshot fidelity, copies/storage/RSS, user .git untouched, Bionic proof. TUR CLI другой версии не доказывает наш jj-lib.

Собственный CAS BLAKE3+zstd тоже не бесплатен: manifest/GC/corruption/recovery и file types остаются продукту. D81 требует no-clobber artifacts, не content-addressed store. Решение между private jj и более узким snapshot backend — после адресного proof, не по слогану «легковесный».

imara-diff0.2 — token diff/hunks, не three-way merge/applier. diffy — кандидат текстового merge; mergiraf — отдельный structural candidate с license/coverage/safety gate, не semantic conflict solver. Автоматически слитый AST не доказывает правильную программу. Интеграция по D11 сохраняет конфликт и base/current/result.

## 2. Shell и процессы

D76 — per-agent state, не обязательный живой bash/PTY. Bashkit serialization покрывает variables/env/cwd/functions/aliases/traps/options и не покрывает jobs/fds/pipes; RealFs contents не входят в snapshot. Brush serde в доноре не включена, jobs пропущены и fd restore неполон. Нельзя называть это checkpoint живого исполнения.

Long-lived processes D59 могут принадлежать отдельному ProcessBackend и переживать конец вызова интерпретатора. Их смерть при закрытии interpreter не закон POSIX: зависит от spawn/PTY/SIGHUP/lifecycle. Обратное, «всё короткоживущее», также нарушит D59. Выбор OS shell для совместимости vs embedded interpreter не делается по ложной дихотомии.

Внешний child не передаёт свой cwd/env в parent. Wrapper внутри native shell может выгрузить её exported env/cwd, но не произвольную память child. Cwd/env-only hibernation теряет функции/options/aliases/traps/FD; transparent resume не обещать. Marker в stdout может быть подделан/перемешан/не выполниться при exit/exec/error; control channel требует отдельного контракта. Dump env может утечь: credentials runtime не должны входить в shell env изначально.

Noninteractive stdin явно EOF либо заданный input; interactive режим PTY по запросу. CI/GIT_TERMINAL_PROMPT/PAGER/DEBIAN_FRONTEND меняют поведение, поэтому child env profile configurable, не unconditional global state. Тишина не означает зависание, wchan Android может быть закрыт/неоднозначен; не убивать команду по этому признаку.

Subreaper только подбирает orphan descendants для wait, pgid best-effort signal group; escaped descendants, PID reuse и runtime death остаются. Env _AGENT_TASK_TOKEN не unforgeable identity и /proc/environ не универсально доступен. Не сигналить PID после restart только по БД/token. Проверка текущей принадлежности backend обязательна.

## 3. SQLite, события и поиск

D80 не требует full event sourcing и не запрещает его как возможный внутренний дизайн; нельзя выдавать append-only event authority за уже согласованное. Рекомендация: authoritative history/current state/call/delivery + transactional event outbox для UI/reconnect. Изменение state и pending fact/event коммитятся совместно. Snapshot+tail с курсором; если retention gap, resync. Event cursor не ack/model processing и не pin retention.

Tool intent — запись о намерении, не команда на blind replay. Log replay не восстанавливает внешние эффекты/процессы. Одно SQLite transaction не делает file publication/derived index атомарными. Append-only не tamper-proof и не решает privacy purge.

Single writer внутри процесса упрощает порядок; другие runtime/backup/checkpoint/read txns всё ещё могут давать busy/locked. WAL/busy_timeout5000/pool не исключают SQLITE_BUSY, synchronous/commit batching выбираются по durability boundaries. Нельзя delayed batch переносить intent после эффекта или outcome после следующего model request.

FTS5 BM25 и unicode61/trigram — кандидат lexical history/memory baseline. Trigram substring имеет ограничения коротких запросов и увеличивает индекс; не semantic retrieval и не «0 МБ RAM». Удаление/обновление source синхронизируется с индексом, external-content сам не синхронизируется от одного объявления. Векторы/LanceDB необязательны, но не запрещаются без отдельного решения.

## 4. Контекст, cache, topology, hooks

Рекомендуется stable prefix на время instruction/config/toolset revision и tail updates, когда корректно. Не frozen prefix навсегда: D29/D42 требуют текущих deny/toolset; repo map может устаревать, нужно явное обновление. Cache benefit зависит от provider/dialect, blanket3–5x не доказан. 85→30 compaction не выбран default. Context epoch/revision проверяет applicability summary; сообщения во время compaction сохраняются независимо и не теряются. Archive отдельно.

Строгий coordinator-worker workflow допустим как профиль, не отмена D06/D49. Peer default=false, max_peer_hops и запрет любых waiting — новые choices, не приняты. Hop budget может ограничить маршрутизацию сообщения, но новый ответ модели — новый message; не контролирует всю token цепь автоматически. Async delivery исключает blocking OS thread, не логический wait cycle. Общие token/time/depth/queue budgets и explicit waiting semantics нужны независимо.

Skills — инструкции/каталог, hooks — runtime events/actions. Общие config/JSON adapters можно reuse, но декларативный predicate не заменяет action lifecycle/trust/error semantics. Project hook не получает полномочий от факта загрузки skill. CLI JSON stream не готовый hook protocol.

Согласованный scope не сокращается без user решения. Вертикальные срезы implementation order допустимы, выдавать недоделанный продукт за итог — нет. Event-driven idle, bounded indexing/output/CPU и lazy load — цели. Literal zero RAM/CPU и instant exactly-once recovery невозможны как общая гарантия; Android может заморозить/убить runtime.

## 5. Крейты, edits, discovery, web, secrets

Crate boundaries выделяются по dependency seams, не фиксированным шести слоям. Один core с модулями и существующие engines допустимы. Workspace orchestration/jj не обязан жить в SQL storage, structural merge не обязан принадлежать code index. agent-proto для stable wire types полезен при реальной независимой границе, не type dumping ground. Один binary может иметь режимы, отдельные binaries не обязательны.

Полный raw-byte file version token нужен вместо короткого hashline как единственного guard. BLAKE3 подходит кандидатом, size/mtime лишь оптимизация. Hash check → temp/fsync/rename обеспечивает atomic replacement/durability лишь при выбранном platform protocol, НЕ atomic conditional replace против внешнего writer. Symlink/file identity/metadata/hard links требуют обработки. Fuzzy/AST matching только внутри проверенной revision с однозначным target; stale version не исправляется молча fuzzy rebase. imara-diff не whitespace matcher. Short tag остаётся UI handle, не proof.

ast-grep/tree-sitter уже исследованы и не новые замены самописного parser. Stack-graphs unsupported GitHub, MIT OR Apache2, rules для ограниченных языков, tree-sitter version mismatch. Tags дают syntax definitions/mentions, не полноценные resolved references/rename. «30+ мгновенно/minimum RAM» не доказано; не урезать D35/D75 до tags-only. Semantic backends Oxc/rust-analyzer/SCIP остаются кандидатами адресно, без LSP.

ignore/globset подходят discovery patterns; не ACL/sandbox и не канонизатор symlink. globset separator semantics надо задавать, не угадывать. Access policy отдельно от игнорирования при поиске: read конкретного пути не должен ошибочно считаться разрешённым только из-за walk exclude.

Web: reqwest transport+limits/security, article extraction и Markdown conversion — разные stages. readability0.3 старый, DOM без built-in caps; htmd0.5.6 converter, не extractor, lossy modes. dom_smoothie0.19 API Readability::new/parse — актуальный кандидат article extraction, не автоматический выбор: docs/code pages могут терять важные примеры/таблицы при readability. Режим raw/full/main должен быть явным, fallback не скрывает потери. 80% savings не измерены. Redirect/network policy и bounded decompression/HTML/parser input до DOM сохраняются.

Cedar4.14.1 Apache2: Allow/Deny, default deny, forbid precedence, skip-on-error diagnostics. Не готовый allow/deny/ask/UI engine; host строит schema/entities/resources, обрабатывает errors консервативно и child ceilings. Не навязывать Cedar DSL пользователю: config мог бы компилироваться в policy. Альтернатива «globset в200строк» не доказана: shell args, resources, precedence/trust и autoanswer risk больше glob match. Выбор после policy contract, не binary-size/µs без измерений.

secrecy0.10.3 полезен explicit ExposeSecret/Debug redaction/zeroize собственной памяти, не стирает intermediate copies/HTTP buffers и не sandbox. aho-corasick1.1.5 умеет literal streaming replacement; Gitleaks regex/entropy/allowlists не переносится одной Aho таблицей. Exact known-secret scrub и pattern detection различаются; cross-chunk overlap, multiline credentials, bounded buffering, errors обязательны. Нет гарантии обнаружить все неизвестные секреты. Не переписывать произвольный source file ради redaction.

## 6. Android build

Bundled SQLite/tree-sitter всё ещё C/C++, нужен NDK Clang/sysroot/linker на CI host; cargo-ndk удобная оболочка, не единственно необходимый способ. Rustls providers ring/AWS-LC тоже могут содержать native code. Отсутствие system OpenSSL/libgit2 не доказывает pure Rust/native-free closure. Статическая линковка codecs не означает полностью static Bionic binary или отсутствие system loader. API level/arch/features и transitive notices проверяются в CI, затем device smoke built artifact. Ни одной такой проверки здесь не было.

## План и согласование

PreliminaryImplPlan получил эти safe assumptions, не D84–D93 как принятое. Нет блокера для приблизительного плана на текущих D: uncertain backends имеют gated choices; D82/topology/scope сохраняются. Решения о точном storage backend/shell/Cedar/protocol формируются по proof, product-visible changes — пользователю, не молчаливый выбор planner.

## Источники и evidence

- https://docs.jj-vcs.dev/latest/git-compatibility/ ; jj-lib0.44 source symbols Workspace::init_internal_git, LockedWorkingCopy::snapshot, git::reset_head. Подробно local://jj-revision-evidence.md.
- https://docs.cedarpolicy.com/auth/authorization.html ; https://docs.rs/cedar-policy/4.14.1/cedar_policy/
- https://github.com/github/stack-graphs ; https://docs.rs/imara-diff/0.2.0/imara_diff/
- https://docs.rs/secrecy/0.10.3/secrecy/ ; https://docs.rs/aho-corasick/1.1.5/aho_corasick/ ; https://github.com/gitleaks/gitleaks
- https://sqlite.org/fts5.html ; https://doc.rust-lang.org/rustc/platform-support/android.html
- https://docs.rs/dom_smoothie/0.19.0/dom_smoothie/struct.Readability.html ; https://docs.rs/htmd/0.5.6/htmd/
- local://shell-revision-evidence.md, local://journal-revision-evidence.md, local://crate-revision-evidence.md, local://policy-redaction-revision.md — source anchors. Parent не принимает source-agent architecture prescriptions/blanket claims автоматически.
- Existing [execution proposal](agent-execution-contract-proposal.md), [provider proposal](agent-provider-mcp-contract-proposal.md), [shared FS](agent-shared-fs-research-2026-11-06.md), [code research](research-2026-11-06-code-analysis.md).
