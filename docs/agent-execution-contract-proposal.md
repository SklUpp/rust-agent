# Исполнение: контракт реализации на согласование

Статус: предложение, не новая принятая архитектура. Основа: D23/D26/D29/D38/D39/D44/D47/D59/D67/D68/D76/D80–D82; исследования execution/shared-fs от 2026-11-06. Реализации, сборок и runtime-проверок нет.

## Назначение

Один runtime flow для CLI/TUI/headless и всех агентов. Не копировать frontend-specific ToolRegistry или JSONL writers. Не требовать второго loop Rig ради MCP/retrieval. Конкретные Rust signatures/SQL и packaging отложены; ниже контракты поведения, не новые crate-per-type.

## Вызов инструмента

1. Loop завершает сборку сообщения модели, включая все tool-call IDs и аргументы. Session owner ожидает запись сообщения в SQLite. Неполный stream не превращается в подтверждённый вызов.
2. Admission связывает call с agent/task/branch и проверяет опубликованный toolset, trust/policy, аргументы на границе инструмента и бюджеты. Неизвестный/скрытый tool не разрешается через lookup имени.
3. Если нужны очередь, writer lease или другой ограниченный ресурс, вызов ждёт. Cancel до допуска завершает ожидание без старта. Начальная проверка policy не выдаёт бессрочного разрешения.
4. После ожидания admission повторно проверяет cancel/current policy/resource ownership; tool-specific writer проверяет актуальность файлов непосредственно при записи. Между сохранением intent и эффектом deny/cancel также проверяются; внешняя гонка этим не устраняется.
5. Session owner сохраняет intent, затем runtime передаёт исполнение конкретному backend. Intent означает «может быть запущен», не доказанный старт/успех. Если запись intent не удалась, действие не запускается.
6. Backend возвращает outcome либо process handle. Outcome и ссылки на готовые artifacts сохраняются до следующего model request. Ошибка сохранения после эффекта не даёт оснований повторить действие.
7. Для batch сохраняются отдельные IDs/outcomes. Batch не обещает параллельность, атомарность или rollback. Действия, зависящие от результата предыдущего, не запускаются как независимые.

Не вводится пользовательское подтверждение записи или TurnAck. Это обычное await persistence внутри runtime.

## Долгая работа

Background handle описывает runtime-owned работу, а не успешное выполнение команды. Выход процесса обновляет exit/result; завершение ответа родителя ничего не меняет в lifecycle. Ограниченный сбор явно отмечает неполноту; полный вывод не обещается при превышении storage/collection limits.

Живой process handle хранится в памяти; SQLite хранит его идентичность и наблюдённое состояние. PID не служит долговременным удостоверением процесса: после restart возможен reuse. Существование строки БД не разрешает посылать сигнал произвольному PID. Как доказать принадлежность surviving process после crash, требует платформенного backend; пока нет доказательства, состояние interrupted/unconfirmed.

Explicit task stop передаётся дочерней ветви и её процессам best-effort: graceful stop, configurable timeout, затем force. Состояние cancelled и фактический exit процесса различаются. Незавершённая остановка должна быть видна; выполненные эффекты не откатываются. Runtime shutdown имеет тот же cleanup contract; crash не гарантирует cleanup.

## Shell и FS

Один сохраняемый interpreter state на агента, lazy creation предлагается, не навязывается как принятый default. Общий FS root по D82; cwd проекта/isolated workspace не является sandbox. Вызовы, изменяющие одну shell-state, сериализуются; конкурентный вызов не получает молча временную shell без сохранения state, как в донорском fallback. Background process I/O/control не обязаны удерживать interpreter lock всё время жизни процесса; способ запуска/снимка env требует backend proof.

Shell env составляется явно без массового копирования env runtime. Exported shell variables передаются её external children согласно разрешённой execution policy; credentials provider не входят в shell env. Состояние функций/options нельзя без доказанного совместимого моста переносить между Bashkit/brush/native shell. Ошибка `exit` in-process не должна завершать runtime.

В текущем extraction это `state::platform_env`: allowlist (`PATH`, `HOME`, `PREFIX`, `TMP*`, `LANG`, `LC_*`, `TERM`, `SHELL`, `TZ`, `USER`, `LOGNAME`, toolchain roots вроде `VIRTUAL_ENV`/`JAVA_HOME`/`CARGO_HOME`/`GOPATH`, именованные `XDG_*`-каталоги, `PI_MAX_AST_FILES`) применяется и к `AgentContext`-конструкторам, и к overlay-ветке process broker; произвольное имя ключа провайдера не наследуется, явный `ctx.env` caller override сохраняется. `LD_PRELOAD`/`LD_LIBRARY_PATH`/`NODE_OPTIONS` — явно именованная executable-конфигурация для Termux-старта, не sandbox; `SSH_AUTH_SOCK` не наследуется автоматически (credential capability) и добавляется только явным `ctx.env`. `AgentContext: Debug` печатает ключи env без значений. Это фильтрация источника env, не sandbox и не authority (D92).

Bashkit может выполнить встроенный участок, но его jobs ограничены exec и no-spawn моделью. Для настоящих toolchains и dev server нужен process backend; нельзя выдать его гарантии за гарантии VFS. Поведение при недоступности OS-изоляции явно отложено пользователем.

## Запись и integration

Writer ownership координируется между runtime-процессами, не только mutex одного процесса. Каноническая идентичность workspace должна переживать aliases; механизм lease/recovery не выбран. Неизолированный read других проектов разрешён, write — нет без соответствующего владения. Внешние редакторы могут менять файлы независимо: lease не даёт право затирать такие изменения.

Edit, formatter и merge используют один контракт версии и записи. Hashline 16-bit tag не служит строгим version token. Проверка версии до staging не является атомарным compare-and-swap. Multi-file операция не обещает транзакцию: preflight плюс результат каждой записи, частичный эффект явно сообщается.

Изолированная задача использует snapshot фактических файлов, не обязательный HEAD/index. Integration использует base/current target/worker result, проверяет версию target перед применением и ограничивает recompute. Неразрешённый конфликт сохраняет варианты и владельца ожидания, не делает ours/theirs автоматически. Правки конфликта не становятся новой базой без явного контракта recheck.

## Восстановление

| Durable evidence | Допустимая трактовка |
|---|---|
| Намерения нет | Runtime не должен был запускать действие; внешние независимые эффекты этим не исключаются |
| Intent есть, outcome нет | Эффект неизвестен; tool contract определяет reconcile/idempotent replay либо interrupted/unconfirmed |
| Outcome сохранён | Возвращаем сохранённый результат, не повторяем tool ради восстановления диалога |
| Background handle сохранён, exit нет | Не подтверждает живой процесс и не разрешает blind PID reuse |

Безопасный повтор вычисления/индексирования и неизвестный внешний shell effect — разные контракты. Rebuildable index допускает повторное применение по стабильному key/revision; произвольный subprocess не становится идемпотентным от записи CallId.

## Донорские срезы

- Rust `AgentLoop`/`ToolRegistry`/scheduler: управление вызовами; admission/resource coordinator придётся дополнить.
- `ProcessBackend`, `ReadSnapshotSink`, `InternalUrlRouter`/`ProtocolHandler`, `ToolApprover`, `AskChannel`: существующие seam, не основание копировать frontend runtime.
- Original Rust shell/edit/isolation engines: перенос адресно; proc/temp/native fast paths требуют отдельной классификации.
- Fork `pi_shell::Shell::{new,run,abort}`: API постоянного состояния; current foreground tool wiring не доказывает его применение.
- Session owner и durable deliveries — собственная интеграция SQLite, не донорский in-memory mailbox.

Подробные pins/source: [execution research](research-2026-11-06-execution.md), [core extraction](agent-core-extraction-design.md), [shared FS](agent-shared-fs-research-2026-11-06.md).

## Proof до принятия реализации

Нужен runnable smoke с fake backend для orchestration boundaries и настоящими subprocess для lifecycle, не source-text assertions: queued cancel; deny после ожидания; persistence failure до запуска; crash после эффекта до outcome; отдельные cwd/env; долгий процесс после ответа; stop ветви без stop соседа; stale edit и partial multi-file outcome; cross-runtime writer exclusion; dirty/non-Git snapshot integration. Проверки engines на Bionic и тяжёлые builds — CI, не телефон. Эти проверки пока не запускались.
