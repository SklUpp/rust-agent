# Исследование: расширение VFS до единого формата зеркалирования путей (`PathSpace` / `RootMap`)

Дата: 2026-11-27  
Статус: исследование (`ScoutVfsMirroring`, только чтение кода) — не нормативное требование; пересматривает границы **D86** (`docs/agent-preliminary-implementation-plan.md:289`).

---

## 1. Вопрос

> Имеет ли смысл расширение VFS до полного покрытия работы агента, кроме случаев, когда нужен доступ наружу — не в смысле изоляции, а в смысле зеркалирования путей в единый формат?

---

## 2. Текущее состояние в кодовой базе

Отдельный крейт `pi-vfs` в workspace отсутствует (`Cargo.toml`, `crates/vendor/` содержат только `rmcp` и `brush-core`). Действует **D86**: «real disk workspace/snapshot/integration vs synthetic resource URI router — generic VFS proxy для обычных файлов не требуется».

Фактически в коде сосуществуют **5 независимых парсеров/резолверов пути** и **3 несовпадающих списка схем**:

| Модуль | Механизм | Схемы / поведение |
|---|---|---|
| `crates/pi-agent/src/tools/internal_urls.rs:38,53,95,265,319-435` | `InternalUrlRouter`, `ProtocolHandler`, `UrlScope` | `RESERVED_SCHEMES` (10): `agent`, `artifact`, `history`, `issue`, `local`, `mcp`, `mcp-prompt`, `pr`, `security`, `ssh` |
| `crates/pi-agent/src/tools/read.rs:1440,1545,1555` | схема + отделение селектора (`:12-40`, `:raw`) | `SELECTOR_SCHEMES` (13): добавляет `memory`, `omp`, `rule`, `skill`, `vault` |
| `crates/pi-agent/src/tools/edit_variants/paths.rs:30,54,129` | `ensure_no_internal_scheme` для `replace`/`patch`/`apply_patch`/`sloppy` | 9 схем: `agent`, `artifact`, `skill`, `rule`, `security`, `local`, `mcp`, `ssh`, `vault` (без `history`/`issue`/`pr`) |
| `crates/pi-agent/src/tools/ast_grep.rs:620,759-820,939` | `is_url_like_path` + `file_url_to_path` | общий check на `://`; отвергает любые не-`file://` URL |
| `crates/aragent/src/acp/session.rs:2910` | `is_internal_url` для ACP `tool_locations` | отдельный парсер |

Резолвинг «входной путь + `cwd`» также продублирован с разной семантикой:
- `tools/read.rs:1247,1271` — `bare / -> cwd`, `expand_tilde` через `ctx.env`, поиск workspace-суффикса (`:1346-1375`).
- `tools/edit_variants/paths.rs:75,99,108` — `~` через `std::env`, лексическая нормализация без обращения к ФС.
- `tools/mod.rs:3240,3252` (`glob`/`grep`) — `canonicalize`-или-лексика.
- `lsp/utils.rs:160` — собственное раскрытие `~` из `std::env`, а не из `ctx.env`.
- `aragent/src/tool_commands.rs:641` — CLI-нормализация с удалением `:`/`@`/`~`.

### Покрытие по инструментам

| Инструмент | Что происходит сейчас |
|---|---|
| `read` (`read.rs:468,534-541,702`) | Единственный полностью VFS-осведомлённый инструмент: URL-first диспетчер, `source_path` как физическая идентичность для `TextSource::Memory`, ACP `PathMap::map_read_path` |
| `write` (`tools/mod.rs:2581-2665`) | `xd://propose`, `conflict://`, plan-guard по *нерезолвленной* строке (`:2587`), `local://` sandbox (`:2600`), router `write` (`:2620`), но далее `ctx.cwd.join(path)` без `~` и без лексики (`:2643`) и ACP `write_text_file` без `PathMap` (`:2647`) |
| `edit` hashline (`hashline/parser.rs:762`, `hashline/edit.rs:519,889,913`) | Спец-ветка только для `local://`; `skill://x` не отвергается и превращается в `cwd/skill:/...` |
| `edit` variants (`replace`, `patch`, `apply_patch`, `sloppy`) | Отвергают 9 схем из `INTERNAL_SCHEMES`, но `history://`/`issue://`/`pr://` не входят в список |
| `glob` / `grep` (`tools/mod.rs:3322,3621,3240`) | Нулевая осведомлённость о VFS: `skill://x` ищется как буквальное имя файла относительно `cwd` |
| `ast_grep` / `ast_edit` (`ast_grep.rs:759-820`, `ast_edit.rs:207`) | Явный отказ на любые не-`file://` URL |
| `bash` (`tools/mod.rs:181,400,483`) | Только `cwd`; ACP `terminal_create` передаёт `cwd_text` напрямую без `PathMap` |
| `lsp` (`lsp/utils.rs:24-203`, `client.rs:1034`, `edits.rs:203-297`) | Изолированный мир `file://` URI; `WorkspaceEdit` (create/rename/delete) мутирует ФС в обход VFS/снапшотов/plan-guard |
| `dap` (`dap/session.rs:97`, `dap/tool.rs:726`) | Собственный `resolve_to_cwd` |
| ACP `PathMap` (`pi-agent/src/state.rs:1367-1506`, `aragent/src/acp/session.rs:412-425`) | Покрывает только `read` (`map_read_path`), не покрывает `write` и `terminal` |

### Действующие провайдеры ресурсов (`InternalUrlRouter`)

- `local://` (`internal_urls.rs:637` `LocalRootHandler`, descriptor-relative `O_NOFOLLOW` через `read_contained:715`)
- `artifact://` (`internal_urls_artifact.rs:102,223,278`)
- `agent://` (`internal_urls_agent.rs:49-56,200,257`)
- `history://` (`hub_park.rs:166,176,524,651`)
- `security://` (`aragent/src/security/resource.rs:13-79`)
- `skill://` (`aragent/src/skill_protocol.rs:39-92`)
- `memory://` (`aragent/src/memory_protocol.rs:197-345`)
- `ssh://` (`tools/ssh.rs:166,308,684,742`, с dynamic trust-check)
- `mcp://` (`internal_urls.rs:505`), `mcp-prompt://` (`internal_urls_mcp_prompt.rs`)
- `issue://`, `pr://` (`internal_urls_github.rs:216,267`)
- Девайсы состояния: `xd://` (`tools/pending.rs:31,46`), `conflict://` (`tools/conflict.rs:249`)
- Внероутерный host bridge: `aragent/src/rpc/host_uris.rs:23-226` и `eval/tool.rs:43-47,210-218` (`PI_SESSION_FILE`, `PI_ARTIFACTS_DIR`)

---

## 3. Что даёт единый слой и где скрытые сбои

### Конкретные выгоды
1. **Снятие асимметрии `read` vs `edit`/`write`/`glob`/`grep`:** сейчас модель читает `skill://plugin/notes.md`, но `edit` по тому же URL пишет в `cwd/skill:/...` (hashline) или падает.
2. **Единая политика `~`, `.`/`..`, каноникализации и `ctx.env`:** убирает расхождение между `write`, `read`, `glob`/`grep` и `lsp`.
3. **Одна идентичность для снапшотов, хеш-тегов, plan-guard и `workspace_writer`:** вместо 4 разных форм (`canonical_key`, `display_path`, `to_client_path`, `canonical_identity`) и plan-guard по нерезолвленной строке (`tools/mod.rs:2587`).
4. **Один проверяемый containment:** вместо 4 отдельных реализаций (`memory_protocol`, `skill_protocol`, `internal_urls_artifact`, `hub_park`).
5. **Чистый display-слой и стабильные ACP `locations`** без утечки host-путей.

### Скрытые режимы отказа
1. **LLM-эргономика против синтетических URI:** модели обучены на POSIX-путях; `cargo`, `git`, `tsc`, `rg` выдают физические пути. Принудительные синтетические префиксы (`ws://...`) увеличивают галлюцинации и токены, а также конфликтуют с `:` в селекторах (`path:12-40`), `ssh://host:port` и легитимными POSIX-файлами с `://` в имени (`read.rs:600-607`).
2. **Outbound-граница (`bash` / `git` / компиляторы / LSP / MCP):** внешние процессы всегда работают по реальным путям; файлы, созданные через `bash`, не имеют «URI-рождения». Авторитетом должен оставаться физический путь, а URI — алиасом.
3. **Symlink / TOCTOU:** текущая защита опирается на descriptor-relative `O_NOFOLLOW` при чтении (`read_contained`) и повторную проверку `assert_prepared_target` + `dev/ino` при записи (`hashline/edit.rs:895`). Единый слой **никогда** не должен кэшировать `ResolvedPath` через `await` или заменять fd-проверки строковой нормализацией.
4. **Worktree-изоляция (`task_isolation.rs:434,786,2827,3984`):** дочерний агент работает в `sandbox.merged_dir()`, но физически видит и основной репозиторий; без явного `RootId` + обратимого отображения для `finalize`/`merge`/`conflict://` родитель и ребёнок будут путать пути.
5. **LSP `file://` и `WorkspaceEdit` (`lsp/edits.rs:203-217`):** создание/переименование/удаление файлов через LSP сейчас обходят VFS и снапшоты.
6. **Android / Termux:** `/data/data/com.termux/files/home`, `$PREFIX`, отсутствие `/tmp` и `/usr`, возможный symlink в `$HOME`. Корни обязаны браться из конфига/`ctx.env`, а не угадываться из пути.
7. **Порядок диспетчеризации:** приоритет `xd://` → `conflict://` → `plan-guard` → `local://` sandbox → `InternalUrlRouter` → `PathMap` → FS (`tools/mod.rs:2575-2643`) должен сохраняться дословно.

---

## 4. Варианты архитектуры

| Критерий | Вариант 1: Один резолвер без новых схем | Вариант 2: URI-first для всего дерева (`ws://...`) | Вариант 3: `PathSpace` + физическая идентичность + URI-алиасы (рекомендуется) |
|---|---|---|---|
| **Суть** | Замена 5 резолверов на `PathSpace::resolve`; обычные файлы — пути | Все файлы адресованы через `ws://root/...`, физический путь скрыт | Единый `Resolved { physical, display, url?, root, writable }`; `physical` — authority, `scheme://` — алиас при наличии `source_path` |
| **Плюсы** | Минимальный риск, снимает разнобой `~`/display/containment | Полное единообразие записи и remote-прозрачность | Устраняет асимметрию `read`/`edit`/`glob`/`grep`, закрывает ACP `write`/`terminal` и LSP `WorkspaceEdit`, сохраняет `bash`/`git`/TOCTOU |
| **Минусы** | Не решает `edit` по URL и тихий мусор в `glob`/`grep` | Ломает LLM-эргономику, конфликтует с `:line`, нужен новый `pi-vfs`, риск TOCTOU-регрессий | Не даёт синтетического URI на каждый файл (осознанно); требует `RootMap` и правила для `source_path` |
| **Риск** | Низкий | Высокий | Средний (поэтапный) |

---

## 5. Рекомендация и поэтапный план (Вариант 3)

**Формулировка решения:** единый формат — это единый резолвер `PathSpace` и единая физическая идентичность с URI-алиасами для ресурсов, а не принудительное URI-пространство поверх обычных файлов.

1. **`PathSpace` (один модуль в `crates/pi-agent`):**
   `parse(input, ctx) -> Parsed { Device(xd/conflict), Resource(InternalUrl), Physical }` → `Resolved { physical, display, url?, root: RootId, writable }`.
   Списки схем запрашиваются у `InternalUrlRouter` вместо 3 захардкоженных констант.
2. **Правило мутации по URL:** разрешена только когда `InternalResource.source_path` задан и помечен `writable`; иначе — явная ошибка `"read-only resource"` вместо `cwd.join("skill://...")`.
3. **`RootMap`:** обобщение `PathMap` (`state.rs:1367`) на `write`, `terminal_create`, `lsp` (`file://` и `WorkspaceEdit`), а также явный `RootId` для worktree-дочерних агентов.
4. **Граница outbound:** `bash`, child-процессы, host RPC и eval-ядра получают физический путь + `RootId`; display-слой форматирует вывод назад без перезаписи семантики.
5. **Этапы:** `M0` — `PathSpace` с сохранением поведения и удалением дублей → `M1` — перевод `write`/`edit`/`glob`/`grep`/`ast_*` → `M2` — `RootMap` для ACP write/terminal и LSP → `M3` — worktree `RootId`.

**Обязательные приёмочные тесты перед внедрением:**
- литеральный файл с `://` в имени;
- `ssh://host:22` (не путать `:22` с номером строки);
- селектор на URL (`skill://a:10-20`);
- подмена symlink между резолвом и `open`/записью;
- запись ребёнка в `local://` sandbox и чтение родителем;
- LSP `WorkspaceEdit` create/rename/delete через `file://`;
- Termux `$PREFIX` и symlink в `$HOME`;
- файл, созданный через `bash`, затем `edit`; `read` по URL, затем `edit` по тому же URL.
