# D48 / D51 / config / D57: инструкции, профили ролей, слои конфигурации, hooks

Дата проверки: 2026-11-06. Это read-only разбор исходников и документов, без сборки, запуска и выбора продуктовой архитектуры.

Pins из локального checkout:
- original OMP: `035e4f95c314f9aff2f5a779fa335422680f8e36`;
- Rust fork: `3008cfd0ca88bb0e28323561a165f39efa7159e1`.

Проверялись локальные файлы `/data/data/com.termux/files/home/omp-work`. Совпадение локальных байтов с remote по этим pins отдельно не проверялось.

## 0. Требования, к которым относится отчёт

- **D48** (`docs/agent-architecture-decisions.md:225-227`): три уровня LLM-инструкций — общие, проектные (область «все агенты проекта» либо «только родитель», включая вложенные правила подкаталога), частные (профиль роли и инструкции конкретного агента/задачи). Задача и выбранный контекст передаются отдельно. Ни один уровень не расширяет runtime permissions. Форматы, discovery, разрешение противоречий, default области и исключение `AGENTS.md` из workers по имени не приняты.
- **D51** (`docs/agent-architecture-decisions.md:241-243`): профиль роли — Markdown + YAML-поля (описание, инструкции, модель, инструменты). Встроенные роли доступны без настройки; личные и проектные дополняют/переопределяют. Родитель может настроить конкретного субагента при запуске без правки постоянного профиля. Точные поля, пути, reload, precedence открыты. Проектные hooks/расширения не активируются наличием профиля.
- **D57** (`docs/agent-architecture-decisions.md:265-267`): hooks реализовать; события, язык/формат, порядок, синхронность, контракты ошибок и действия открыты. Действуют trust/policy D20 и разделение проектных этапов D46.
- Связанные ограничения: **D15** (`:75-79`) — слои defaults → user config → project → launch → session; на project уровне LLM-инструкции/контекст отделены от машинной конфигурации; **D20** (`:97-101`) — проектные hooks/MCP/команды/расширения активируются только после явного доверия либо правила владельца, режим без подтверждений не даёт доверия коду репозитория; **D69** (`:315-317`) — TUI/CLI/headless используют одни правила значений/проверок и слои D15.

## 1. Главные факты

1. **OMP не реализует единую трёхуровневую модель D48.** Он разделяет context files (`AGENTS.md`/`CLAUDE.md`/…), sticky rules (`RULES.md`), rules (`rules/*.md`), `SYSTEM.md`/`APPEND_SYSTEM.md` и `instructions/*.md`. Это разные поверхности с разным precedence; настройки «проектные инструкции только родителю» не найдено.
2. **OMP-субагент получает проектные context files с фильтром по имени `agents.md`** (`src/task/structured-subagent.ts:437`) и все `rules` без такого фильтра (`:441`). D48 прямо говорит, что исключение `AGENTS.md` по имени не принято; поведение OMP нельзя переносить автоматически.
3. **Фактический rendered порядок context files: user first, затем project farther→near.** Документация утверждает обратное — user last. Это статический конфликт source/docs; тест `test/system-prompt-context-dedup.test.ts:118-123` ожидает user-first. Подробный разбор — §2.2.
4. **Правила OMP дедуплицируются по `name`, а не по содержимому/пути**, и globs/`applyTo` в rulebook не enforce-ятся кодом; это advisory-подсказка. Вложенность проекта в OMP выражена depth-порядком context files, а не универсальным scoped-rule механизмом.
5. **Профиль роли OMP — Markdown-тело + YAML frontmatter**, first-wins по точному имени. Поля: `name`, `description`, `systemPrompt` (тело), `tools`, `spawns`, `model`, `thinkingLevel`, `output`, `blocking`, `autoloadSkills`, `readSummarize`, `prewalk`, `advisor`. Проектный профиль полностью выигрывает у user/bundled; field-level merge нет.
6. **Per-spawn настройка в OMP ограничена**: модель (внутренний/eval request), `effort`, `outputSchema`/`schemaMode`, `isolated`, batch `context`; общего per-spawn override `tools`/`systemPrompt`/полного профиля в wire-схеме нет. D51 требует более широкой настройки родителем.
7. **Слои конфигурации OMP точны**: defaults → global → project → `PI_CONFIG_FILES` overlays → `--config` overlays → runtime. Объекты deep-merge, массивы заменяются целиком. Project settings — только `<cwd>/.omp`, без ancestor walk.
8. **Схема настроек OMP — TypeScript-объект с необязательными `ui`-метаданными**, а не отдельная UI-независимая схема. Rust-форк содержит готовый UI-независимый consumer: `settings-schema.snapshot.json` + `config_cmd.rs` (`path`, `type`, `default`, `values`, `credential`, `ui`).
9. **OMP hooks — JS/TS-модули, загружаемые Bun import**, не декларативные shell-скрипты в runtime. Capability-комментарий называет их shell scripts, но loader требует default export функции. Язык/формат D57 остаётся открытым.
10. **OMP не имеет per-directory trust gate**: проектные extensions/config/hooks загружаются безусловно (`src/extensibility/extensions/types.ts:526-537`; `docs/extension-loading.md:251-252`). D20 требует явное доверие; поведение OMP не годится как доверенный источник trust-модели.
11. **В проверенном дереве Rust-форка нет пользовательской hook-системы**: есть host seams/трейты (`PlanModeHooks`, `ChildAdvisorHook`, steering), но не hook loader/events/tool interception. Для D57 готового Rust-runtime донора не найдено.

## 2. D48: инструкции и области

### 2.1. Что реально делает OMP

| Поверхность | Пути/порядок | Precedence/dedup | Инъекция |
|---|---|---|---|
| Context files | `~/.omp/agent/AGENTS.md`; nearest non-empty `<ancestor>/.omp/AGENTS.md`; также `.claude/CLAUDE.md`, `.codex/AGENTS.md` (user only), `.gemini/GEMINI.md`, `.github/copilot-instructions.md`, `.agent(s)`, standalone `AGENTS.md` | native 100 > claude 80 > agents/codex 70 > gemini 60 > opencode 55 > github 30 > agents-md 10. Один user file; один project file на depth; выше priority выигрывает на depth; depth-файлы сохраняются; byte-identical/contained схлопываются | `<repo-rules><file path=…>…</file>`; **source actual: user first, затем project farther→near** (см. §2.2); docs утверждает user last |
| Native project context | Только **nearest non-empty** `.omp/`; если в нём нет `AGENTS.md`, выше не ищется | native provider; project/user | в тот же блок |
| Sticky `RULES.md` | `~/.omp/agent/RULES.md`; nearest non-empty project `.omp/RULES.md` | Синтезируется rule name `RULES`, `alwaysApply: true`; user обычно перекрывает project; не конкатенируются; regular `rules/RULES.md` может перекрыть оба | always-apply rule, повторно near current turn |
| `rules/*.{md,mdc}` | native project `<cwd>/.omp/rules`, user `<agent>/rules`, плюс провайдеры `agents`, cursor, windsurf, cline, github, builtin-defaults | dedup по `rule.name`, first-wins; native 100, omp-plugins 90, agents 70, cursor/windsurf 50, cline 40, github 30, builtin 1 | TTSR (condition/astCondition) → alwaysApply → rulebook; rulebook только name+description, чтение через `rule://` |
| `SYSTEM.md` / `APPEND_SYSTEM.md` | `<cwd>/.omp`, `.claude`, `.codex`, `.gemini`; затем user bases того же порядка | CLI flag > discovered file; project > user; первый config base; **no ancestor walk** | `SYSTEM.md` заменяет bundled instruction template; `APPEND_SYSTEM.md` добавляет текст |
| `instructions/*.md` | native `<base>/instructions/*.md`; GitHub `.github/instructions/**/*.instructions.md` | capability `instructions` key = name; GitHub-файлы нормализуются в **Rule** с `applyTo` globs, а не в отдельную enforced surface | Потребитель native `instructions` в runtime не найден; GitHub `applyTo` попадает в rulebook/TTSR |

Источники: `docs/context-files.md:24-31`, `:61-95`, `:114-116`, `:173-176`; `docs/rulebook-matching-pipeline.md:42-120`, `:225-290`; `packages/coding-agent/src/discovery/builtin.ts:58-100` (native проект раньше user), `:376-427` (rules), `:641-669` (instructions), `:905-944` (context files); `src/capability/instruction.ts:16-38`.

### 2.2. Source rendering vs docs: конкретный конфликт

**Doc claim:** `docs/context-files.md:93-95` говорит: «The single surviving user-scope file sorts after project files»; «Final injection order is farther project ancestors first, then project files closer to the cwd, then the surviving user-scope file».

**Source behavior:** ровно наоборот по user-файлу.

1. `loadProjectContextFiles` сначала сортирует `depth ?? -1`, то есть намеревается положить user (нет `depth`) последним (`src/system-prompt.ts:476-482`).
2. Затем она возвращает `dedupeContainedContextFiles(files)` (`:484`).
3. `dedupeContainedContextFiles` пересортировывает массив с `depth ?? Number.POSITIVE_INFINITY` (`:425-435`). User-файл без `depth` получает `Infinity` и оказывается **первым**; project depth идут по убыванию, то есть farther → near.
4. `project-prompt.md:8-15` рендерит `contextFiles` в этом порядке без дополнительной сортировки: `{{#each contextFiles}}<file>…`.
5. Тот же `dedupeContainedContextFiles` применяется к `providedContextFiles` (`system-prompt.ts:732`) и повторно к результату `loadProjectContextFiles` (`:870-872`), поэтому оба входа дают user-first.
6. Тест `test/system-prompt-context-dedup.test.ts:118-123` («treats files without a depth as less authoritative than project files») ожидает `["/home/user/.omp/AGENTS.md", "/project/AGENTS.md"]`, то есть user первым. Тест кодирует фактический sort, а не doc-порядок.

Итог: **source/test = user first, project farther→near; docs = project farther→near, user last.** Это не «сглаженный» вывод и не доказательство runtime-бага: расхождение статическое, на уровне кода/документации; запуск не выполнялся. Для D48 это важно, потому что «later = more prominent» в docs не соответствует фактическому массиву, который получает шаблон.

Pinned evidence:
- `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/system-prompt.ts`
- `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/prompts/system/project-prompt.md`
- `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/test/system-prompt-context-dedup.test.ts`
- `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/context-files.md`

### 2.3. Области и противоречия

- **User vs project:** native provider отдаёт project item раньше user (`builtin.ts:58-75`). Для capability dedup контекстных файлов это не задаёт финальный render: `dedupeContainedContextFiles` сортирует по `depth`, а user получает `Infinity`. Для hooks/rules/context внутри native provider project-first; для generic `config.ts` helper порядок другой (user сначала, `src/config.ts:127-155`). Нельзя описывать precedence одной фразой без указания подсистемы.
- **Nested project rules:** OMP context files дают depth-порядок для standalone `AGENTS.md` и ancestor-dirs, но native `.omp/AGENTS.md`/`RULES.md` берёт только nearest non-empty `.omp`, без продолжения вверх. Native `.omp/rules` читается из `<cwd>/.omp`, не из каждого ancestor. Универсального «rule для подкаталога» с enforce-семантикой нет.
- **`applyTo`/globs:** `docs/rulebook-matching-pipeline.md:270-278` прямо говорит: globs рендерятся/показываются, TTSR использует их как global path gate, но rulebook applicability кодом не enforced. Это не выполнение D48 «включая вложенные правила для соответствующего подкаталога».
- **Parent-only scope:** настройки/поля, которое ограничивает проектные инструкции только родителем, не найдено. `disabledExtensions` умеет отключать context file по `context-file:<level>:<basename>` на всех depth, но это не parent-only и не per-agent.
- **Subagent behavior:** `structured-subagent.ts:437` фильтрует context files по `path.basename(file.path).toLowerCase() !== "agents.md"`; `rules: session.rules` передаются без фильтра (`:441`). То есть worker не получает `AGENTS.md`, но получает rules, включая потенциально проектные. Это конкретное поведение OMP, а не решение D48.
- **Role/task instructions:** профиль роли становится `§ Role` (`prompts/system/subagent-system-prompt.md:2`), выбранный контекст — `§ Context` (`:4-6`), plan — отдельный `<plan>` (`:8-14`). Это подтверждает разделение «роль/задача/контекст», но не даёт проектной области «all agents vs parent only».

### 2.4. Границы для нового продукта

Факты, которые нужно учитывать, не выбирая дизайн:
- контекстные файлы — **не** permissions и не машинный конфиг; их чтение не должно активировать hooks/MCP/команды;
- нужна явная политика, что происходит при конфликте user/project/role/task, потому что OMP-механизмы разные: context dedup по scope/depth, rules dedup по имени, profile first-wins;
- вложенность подкаталога нельзя выдавать за уже решённую: OMP показывает globs, но не enforce-ит их для rulebook;
- исключение `AGENTS.md` по basename — observed OMP behavior, но D48 его не принял.

## 3. D51: профили ролей

### 3.1. Формат и API OMP

`AgentDefinition` (`src/task/types.ts:356-381`):
```ts
name: string; description: string; systemPrompt: string;
tools?: string[]; spawns?: string[] | "*"; model?: string[];
thinkingLevel?: ConfiguredThinkingLevel; output?: unknown; blocking?: boolean;
autoloadSkills?: string[]; readSummarize?: boolean;
prewalk?: boolean | string; advisor?: boolean | string;
source: "bundled" | "user" | "project"; filePath?: string;
```

Парсинг (`src/discovery/helpers.ts:237-334`):
- `name` и `description` обязательны; иначе файл невалиден и пропускается (custom — warning, bundled — fatal).
- `tools`: CSV или YAML array; пустой array сохраняется как «нет инструментов»; при непустом allowlist автоматически добавляется `yield` (`:264-271`).
- `spawns`: `*`, CSV, array; если `spawns` отсутствует, но `tools` содержит `task`, становится `*` (`:273-291`).
- `model`: один selector, CSV или array; role aliases (`@review`) раскрываются через `modelRoles`; порядок — prioritized list.
- `output`: opaque schema; `blocking`; `autoloadSkills`; `readSummarize`; `prewalk`/`advisor` — boolean или строковый pattern.
- Тело Markdown — `systemPrompt`; frontmatter ключи нормализуются (`thinking-level` → `thinkingLevel`).

Rust-парсер (`crates/omp-cli/src/agents.rs:145-199`) повторяет поля и defaults, плюс `split_frontmatter`/`repair_ambiguous_scalars`/`camelize_keys`. Он вставляет `yield` только в непустой tools list (`:158-168`), `spawns` выводит из `task`, `model` по умолчанию пустой.

### 3.2. Discovery и precedence

TS (`src/task/discovery.ts:65-125`; `docs/task-agent-discovery.md:136-150`):
1. nearest project `.omp/agents` (first `.omp` hit; ancestor walk);
2. user `.omp/agents`;
3. OMP extension package roots: CLI `--extension`; effective `extensions:` array (project layer заменяет user layer, поэтому одновременно оба не действуют); installed npm/link plugins;
4. Claude marketplace plugin roots (project scope before user), только если `claude-plugins` включён;
5. bundled: `scout`, `designer`, `reviewer`, `security-reviewer`, `librarian`, `task`, `sonic`.

Правила:
- first-wins по точному `agent.name`, case-sensitive; файлы в каталоге — lexicographic filename order;
- project перекрывает user; extension roots перекрывают marketplace/bundled;
- нечитаемый/невалидный пользовательский файл — warning + skip, остальные продолжают;
- bundled парсятся с `level: "fatal"`, их ошибка может сорвать discovery;
- `.claude/agents`, `.codex/agents`, `.gemini/agents` напрямую не читаются: другой frontmatter contract.

Rust-форк (`crates/omp-cli/src/agents.rs:63-107`):
1. nearest project `.omp/agents` (walk up);
2. `<agent-dir>/agents`;
3. configured extension roots `<root>/agents`;
4. seven embedded prompts.

Тот же first-wins, project → user → extension → bundled. Отличия: нет Claude marketplace; `bundled_agents()` парсит embedded prompt assets; невалидный файл — warning + skip.

### 3.3. Reload, model, tools, per-spawn

Preflight каждого spawn:
```ts
await request.session.settings.reloadFromDisk();
...
const discovery = await discoverAgents(...);
const agent = getAgent(discovery.agents, agentName);
```
(`src/task/structured-subagent.ts:245-256`). То есть изменения профиля/`modelRoles` на диске подхватываются следующим spawn; уже запущенный child не перезагружается.

Model precedence (`src/config/model-resolver.ts:1182-1211`):
1. `requestModel` (внутренний/eval request);
2. `task.agentModelOverrides[agentName]`;
3. `agent.model` frontmatter (role aliases через `modelRoles`);
4. parent active model;
5. configured/default `modelRoles.default`.

`task.disabledAgents` проверяется после discovery (`structured-subagent.ts:261-269`); `spawns` parent policy, depth, blocked-self-recursion — отдельные guards. `effectiveAgent` в plan mode ограничивает tools до read-only набора.

Per-spawn wire (`src/task/types.ts:273-303`):
```ts
{ name?, agent?, task?, effort?, outputSchema?, schemaMode?, tasks?, context?, isolated? }
```
Нет полей `tools`, `systemPrompt`, `model` (кроме внутреннего request path), `autoloadSkills`, `spawns`. Значит «родитель настраивает конкретного субагента при запуске» в OMP реализовано частично: `effort`, schema, isolation, batch context, а также глобальные `task.agentModelOverrides`. Полного launch-time профиля нет.

### 3.4. Границы D51

- OMP профиль — **не** источник permissions: tools/profile не должны активировать проектные hooks/расширения (D51 прямо это запрещает; OMP trust-gate отсутствует, поэтому его поведение не переносить).
- OMP first-wins по имени не делает field-level merge; если продукту нужно «дополняют», это отдельное правило, которого в OMP нет.
- `model`/tools precedence OMP можно использовать как проверенный reference, но `requestModel` — внутренний seam, не пользовательский wire.
- Rust-форк — готовый donor парсинга/discovery, но документация (`docs/task-agent-discovery.md:170-180`) говорит, что `prewalk`, `advisor`, `autoloadSkills`, `read-summarize: true` могут отклоняться на spawn; это не проверено запуском.

## 4. Слои конфигурации и UI-независимая схема

### 4.1. Точный precedence OMP

`docs/settings.md:91-107`, `src/config/settings.ts:1281-1293`, `:2649-2652`:
```text
built-in defaults
  <- global config        ~/.omp/agent/config.yml | config.yaml
  <- project config       <cwd>/.omp/settings.json, then <cwd>/.omp/config.yml
                          + other project providers (claude/gemini/…)
  <- PI_CONFIG_FILES      platform path-list, files in order
  <- --config <file>      repeatable, later wins
  <- runtime overrides    flags/env, in memory
```
- Объекты deep-merge; скаляры и массивы заменяются целиком (`docs/settings.md:130-136`). Массивы не аппендятся.
- Unset key читается как `getDefault(path)` (`settings.ts:597-607`).
- Project settings не ходят по ancestors: только `<cwd>/.omp` (`docs/settings.md:35-37`; `settings.ts:1520-1549`). Context files и agents, наоборот, ходят.
- Native project settings мержатся через capability providers, затем native `.omp/config.yml`; native `modelRoles` повторно мержатся последними (`settings.ts:1520-1544`).
- Overlay strict: missing file, invalid YAML или non-mapping root — hard error (`settings.ts:1576-1602`).
- Invalid global/project YAML: TS перемещает в `.broken-*` и падает; Rust-форк не двигает файлы, падает на месте, bytes не трогает (`docs/config-usage.md:120-130`).
- Запись: обычные changes → global; project write только `modelRoles` при `modelRoleStorage: project` (`docs/settings.md:65-85`).

### 4.2. UI-независимая схема: что уже есть

TS `SETTINGS_SCHEMA` (`src/config/settings-schema.ts:468`) — объект `path -> def`:
```ts
{ type: "boolean"|"string"|"number"|"enum"|"array"|"record",
  default: ...,
  values?: readonly string[],       // enum
  credential?: true,
  ui?: { tab, group?, label, description, warning?, options?, secret?, ... } }
```
`ui` необязателен; `getDefault`, `getType`, `getEnumValues`, `isCredential`, `hasUi`, `getUi`, `getPathsForTab` — отдельные accessors (`:6025-6072`). То есть core (path/type/default/enum/credential) отделим от UI-метаданных, хотя физически лежит в одном объекте.

Важно: `Settings` **не валидирует весь документ** через `SETTINGS_SCHEMA`. Он парсит YAML, применяет миграции, deep-merge, а типизированные значения/дефолты читает через schema. Неизвестные ключи проходят; есть только `dropSettingsGroupShadows` против scalar, затеняющего группу (`settings.ts:196-239`). Полная schema-валидация используется `ConfigFile<T>` для других файлов (`src/config/config-file.ts:249-308`): parse → `schema(parsed)` → `ConfigError` с instancePath; tri-state `ok`/`error`/`not-found`.

Rust-донор:
- `crates/omp-cli/settings-schema.snapshot.json` — сгенерированный снимок TS-схемы: `path`, `type`, `default`/`defaultAbsent`, `values`, `credential`, `ui`.
- `crates/omp-cli/src/config_cmd.rs:85-98` — `SettingDef { path, kind, default, values, credential, ui }`.
- `merged_layers` (`:204-224`), `project_layer` (`:279-331`), `effective_value` (`:1663-1675`), `resolve_path_scoped` (`:1678-...`), `parse_value` (`:1990-...`), `merge` (`:1599-...`), `migrate_settings` (`:754-...`).
- CLI `list/get/set/reset` использует core-поля; `ui` нужен для группировки/описаний, не для значения. Это готовый пример UI-независимого consumer.

Ограничения Rust-донора:
- это **снимок**, а не runtime-схема: provenance фиксирует `gitRevision 5c3c15b9...` в `settings-schema.snapshot.json:14`, что не совпадает с pins этого отчёта. Это **provenance mismatch, не доказанный runtime bug**: сам файл — generated artifact, runtime-поведение из расхождения commit id не следует. Drift-check (`scripts/dump-settings-schema.ts`) существует, но не запускался;
- `Config::from_value` моделирует только потребляемый subset; unknown root keys игнорируются (`crates/omp-cli/src/config.rs:1-4`, `:1216-...`); это не полная валидация;
- YAML reader — in-tree subset: block mappings/sequences, flow, quotes, comments, block scalars; anchors/tags/aliases/merge keys/multi-doc не поддерживаются (`crates/omp-cli/src/yaml.rs:1-10`). `serde_yaml` намеренно отсутствует (`Cargo.toml` комментарий);
- Rust run path (`Config::apply_project_layers`, `:1138-1181`) читает только `.omp/settings.json` + `.omp/config.yml`, тогда как `config_cmd::project_layer` (`:279-331`) добавляет `.claude`, `.gemini`, opencode, cursor. Это расхождение effective-view и run-path, не проверенное запуском;
- `Config::load` fatal на invalid project/overlay YAML; malformed project `settings.json` — warning + skip.

### 4.3. Границы конфигурации

- D15 требует разделять LLM-инструкции и машинную конфигурацию. OMP это делает физически: settings vs context/rules; но project-level discovery имеет разные правила и не единый scope.
- D69 требует одни правила значений/проверок для TUI/CLI/headless. OMP core accessors + Rust snapshot — donor; но полной валидации settings-документа нет, а `ui` встроен в тот же объект.
- Нельзя обещать ancestor project settings: OMP их не читает.
- Массивы заменяются; это частая ошибка при ожидании «project дополняет global».
- Trust: OMP project config загружается без отдельного trust gate; D20 требует другого поведения.

## 5. D57: hooks

### 5.1. Текущее состояние OMP

Есть два слоя:
1. **Legacy hook subsystem** (`src/extensibility/hooks/*`): типы, loader, runner, `HookToolWrapper`.
2. **Default CLI runtime** (`docs/hooks.md:5-14`): `--hook` трактуется как alias `--extension`; JS/TS hook factories из `hookCapability` загружаются как extension modules; tools оборачиваются `ExtensionToolWrapper`, а lifecycle/context идут через `ExtensionRunner`. То есть legacy `HookToolWrapper` не обязательно активен в обычном запуске.

Факт: hook capability описывает `Hook` как `{name, path, type: "pre"|"post", tool, level}` (`src/capability/hook.ts:12-40`), но loader импортирует модуль и требует `export default function` (`src/extensibility/hooks/loader.ts:146-156`). Комментарий capability «shell scripts» не соответствует фактическому module contract.

### 5.2. Discovery и порядок

Native paths (`src/discovery/builtin.ts:671-728`):
- `<base>/hooks/pre/*`, `<base>/hooks/post/*`;
- `tool` = basename без последнего расширения; `*` — wildcard;
- `name` = полное имя entry (с расширением);
- `level` = project/user; native provider отдаёт project затем user (`builtin.ts:58-75`);
- explicit paths добавляются после discovered, dedupe по resolved absolute path (`loader.ts:220-242`).

Capability key: `${type}:${tool}:${name}` (`capability/hook.ts:31`). Providers сортируются по priority (native 100, claude 80, codex/gemini/... ниже); first-wins по key. Внутри одного provider порядок файлов из `readdir` не дополнительно сортируется (`docs/hooks.md:187-190`).

`--hook` в CLI (`cli/flag-tables.ts:209-212`) идёт в `args.hooks`, затем в `additionalExtensionPaths` вместе с `--extension`; current session startup treats it as extension path (`docs/hooks.md:9-12`; `docs/extension-loading.md:63-66`). Это не отдельный declarative shell-hook loader.

### 5.3. API и события

Hook factory:
```ts
export default function (pi: HookAPI): void
```
`HookAPI.on(event, handler)` (`src/extensibility/hooks/types.ts:476-507`). События:
- session: `session_start`, `session_before_switch`, `session_switch`, `session_before_branch`, `session_branch`, `session_before_compact`, `session.compacting`, `session_compact`, `session_shutdown`, `session_before_tree`, `session_tree`;
- context/agent: `context`, `before_agent_start`, `agent_start`, `agent_end`, `turn_start`, `turn_end`, `auto_compaction_start`, `auto_compaction_end`, `auto_retry_start`, `auto_retry_end`, `ttsr_triggered`, `todo_reminder`;
- tools: `tool_call`, `tool_result`.

Контракты:
- `tool_call` → `{ block?: boolean; reason?: string; input?: Record<string, unknown> }`; non-blocking `input` заменяет raw execution args; `computer` исключён.
- `tool_result` → `{ content?; details?; isError? }`.
- `context` → `{ messages?: Message[] }`, chain: каждый handler получает вывод предыдущего.
- `before_agent_start` → `{ message?: CustomMessagePayload }`, берётся первый.
- session `before_*` → `{ cancel?: boolean }`; `session.compacting` — результат.
- commands/renderers/`pi.exec` доступны через API; command context имеет `waitForIdle`, `newSession`, `branch`, `navigateTree` (`types.ts:206-253`).

### 5.4. Порядок, ошибки, мутации

- Runtime order: hooks array order → handler registration order (`docs/hooks.md:192-203`).
- `tool_call`: last returned result wins until `block`; первый block short-circuit; handlers не видят revision друг друга.
- `tool_result`: last override wins, no short-circuit.
- `context`: chained.
- `before_agent_start`: first message only.
- Load-time error: captured in `LoadHooksResult.errors`, другие hooks продолжают (`loader.ts:191-208`).
- `HookRunner.emit` ловит handler errors и вызывает `HookError` listeners, затем продолжает (`runner.ts:270-323`).
- `emitToolCall` **не ловит**: ошибка propagates; `HookToolWrapper` fail-closed блокирует tool (`tool-wrapper.ts:42-73`); `tool_result` на ошибке отправляется с `isError: true`, затем original error rethrow (`:77-122`).
- Timeout: в legacy runner timeout-параметра нет: `src/extensibility/hooks/runner.ts:322-326` — «No timeout»; комментарий `tool-wrapper.ts:42-43` упоминает timeout, но фактического timeout нет.
- Hooks не sandboxed, тот же runtime/process (`docs/extension-loading.md:251-252`); `pi.exec` доступен (`types.ts:573-576`) и запускает shell в процессе пользователя. **[INFERENCE]** — права вытекают из отсутствия sandbox/смены uid, отдельного теста нет.

### 5.5. Trust

`src/extensibility/extensions/types.ts:526-539` (смысл): OMP **не имеет** per-directory trust gate; `.omp/extensions`, `.omp/config.yml` и другие project-local inputs discovered/loaded unconditionally; `isProjectTrusted()` всегда `true` для совместимости. `--trusted-extension` — не project trust, а exact allowlist модулей: realpath, must be file, `disableExtensionDiscovery = true`, only these paths (`src/main.ts:1342-1364`).

Следствие: OMP hooks нельзя считать реализацией D20. Для нового продукта проектные hooks должны требовать explicit trust/owner rule; режим без подтверждений не должен это обходить.

### 5.6. Rust-донор

В `crates/omp-cli` и `crates/pi-agent` нет user hook loader/events/tool interception. Есть только host seams:
- `PlanModeHooks` (`crates/omp-cli/src/agent.rs:240-283`);
- `ChildAdvisorHook` (`crates/pi-agent/src/subagent_advisor.rs:42-56`);
- steering/close/control hooks в RPC/ACP, не пользовательские.

`--hook` в Rust CLI не найден; `args.rs` содержит только комментарий про startup hooks интерпретатора (`:1370-1372`). Поэтому для D57 Rust-runtime нужно проектировать/писать, а не «подключить готовое».

## 6. Лицензии, pins и pinned URLs

| Донор | Pin | Лицензия | Ключевой API |
|---|---|---|---|
| Original OMP (TS harness, docs) | `035e4f95c314f9aff2f5a779fa335422680f8e36` | MIT: `LICENSE` (Copyright 2025 Mario Zechner; 2025-2026 Can Bölük; 2026 Stencil Labs, Inc.); `packages/coding-agent/package.json` `"license": "MIT"` | `settings-schema.ts`, `settings.ts`, `config.ts`, `task/discovery.ts`, `task/structured-subagent.ts`, `extensibility/hooks/*` |
| Rust fork | `3008cfd0ca88bb0e28323561a165f39efa7159e1` | MIT: workspace `license = "MIT"`; `LICENSE` тот же | `crates/omp-cli/src/{config.rs,config_cmd.rs,agents.rs,prompts.rs,yaml.rs,dirs.rs}`, `settings-schema.snapshot.json`; `crates/pi-agent` host hooks |
| Snapshot schema | generated from TS; file records `gitRevision 5c3c15b9...` | derived from MIT source; generated file | `SettingDef`, `merged_layers`, `effective_value`, `parse_value` |

Pinned URLs ключевых файлов (original):
- context docs: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/context-files.md`
- context rendering: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/system-prompt.ts`
- context template: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/prompts/system/project-prompt.md`
- context order test: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/test/system-prompt-context-dedup.test.ts`
- settings docs: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/settings.md`
- config usage: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/config-usage.md`
- settings schema: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/config/settings-schema.ts`
- settings loader: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/config/settings.ts`
- task agent docs: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/task-agent-discovery.md`
- task discovery: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/task/discovery.ts`
- task policy: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/task/structured-subagent.ts`
- task types: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/task/types.ts`
- hooks docs: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/docs/hooks.md`
- hooks loader: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/extensibility/hooks/loader.ts`
- hooks runner: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/extensibility/hooks/runner.ts`
- hooks types: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/extensibility/hooks/types.ts`
- hooks wrapper: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/extensibility/hooks/tool-wrapper.ts`
- extension trust comment: `https://github.com/can1357/oh-my-pi/blob/035e4f95c314f9aff2f5a779fa335422680f8e36/packages/coding-agent/src/extensibility/extensions/types.ts`

Pinned URLs ключевых файлов (fork):
- config: `https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/omp-cli/src/config.rs`
- config command: `https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/omp-cli/src/config_cmd.rs`
- agents: `https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/omp-cli/src/agents.rs`
- prompts: `https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/omp-cli/src/prompts.rs`
- YAML subset: `https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/omp-cli/src/yaml.rs`
- schema snapshot: `https://github.com/SklUpp/omp-rust-termux/blob/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/omp-cli/settings-schema.snapshot.json`

Лицензии внешних crate/документов отдельно не аудировались; lockfile не содержит license-полей. Никакого кода Claude Code не копировалось; D57 ссылается на его docs только как на сравнение.

## 7. Границы отчёта

В scope: D48/D51/D57, слои/форматы/схема конфигурации, source facts, donor API, gaps, licenses.
Вне scope: UI layout, memory backend, shell execution engines, выбор hook language/runtime, продуктовая архитектура, реализация, benchmark, Android/Bionic verification, security design beyond trust evidence.

## 8. Непроверенное / ограничения

- Сборка, запуск, тесты, Android/Bionic не выполнялись. Все выводы — статический разбор файлов.
- Локальные файлы не сверялись побайтно с remote по pins; pins взяты из документов checkout.
- **Context order:** source/test говорят user-first, docs — user-last. Это статический конфликт; какой из них считается желаемым поведением и является ли docs или код ошибочным, не устанавливалось. Runtime-проверка не выполнялась.
- **Snapshot provenance:** `gitRevision 5c3c15b9...` не совпадает с pins; это не доказанный runtime bug, а расхождение метаданных generated artifact. Drift-check не запускался.
- `docs/hooks.md` описывает legacy subsystem и отдельно current extension-runner path; какой путь активен в конкретном host/SDK, не проверено запуском.
- Утверждение «native `instructions/*.md` не имеет runtime consumer» основано на поиске по `src`; возможно, потребитель есть вне проверенного дерева/SDK.
- `docs/task-agent-discovery.md` говорит, что Rust-форк отклоняет `prewalk`/`advisor`/`autoloadSkills`/`read-summarize: true` на spawn; код парсит поля, runtime-проверка не выполнялась.
- `Config::load` vs `config_cmd::project_layer` — расхождение по foreign providers найдено чтением, не тестом.
- Timeout hooks: комментарий `HookToolWrapper` упоминает timeout, фактического timeout в legacy runner нет; поведение extension runner не проверялось.
- Project trust: вывод основан на explicit source comment OMP; реальные SDK/хосты могут добавлять собственные gates.
- `pi.exec`/shell permissions — inference из отсутствия sandbox; не проверялось на Android.
