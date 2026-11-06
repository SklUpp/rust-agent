# Общая файловая система shell: исследование

Дата: 2026-11-06. Source review; реализации, сборок, установки и runtime-проверки sandbox не было. Это предложение реализации, не утверждение готовности.

## Назначение и согласованные границы

D82: один managed runtime root, каталоги проектов и отдельный раздел изолированных workspace; shell-state каждого агента отдельный. Все проекты общего корня видимы. Видимость не разрешает запись в чужой workspace. Runtime DB и credentials не открываются shell. Необходимые внешние пути подключаются явно; конкретный набор/права открыты. Вопрос внешнего исполнения без надёжной OS-изоляции пользователь отложил.

Общий физический root не означает передачу shell всего root через один writable mount. Служебные данные можно хранить под managed root, не экспортируя их в VFS; credentials остаются физически вне VFS по D27.

## Доноры и предлагаемая реализация

| Кандидат | Подтверждённое | Пробел |
|---|---|---|
| Original OMP 035e4f95c314f9aff2f5a779fa335422680f8e36 | pi-vfs Fs/provider, shell Host seam, маршрутизация native/provider | Не готовый jailed root; mount_file — descriptor overlay, не directory mount; subprocess/native fast paths, proc/temp обходят VFS |
| Rust fork 3008cfd0ca88bb0e28323561a165f39efa7159e1 | Сохраняемая brush shell, реальные children | В этом pin нет pi-vfs; host FS и absolute cwd не дают containment |
| Bashkit a3177376d4a23f6ae34320e9cc11674f635ab03f (workspace 0.20.3, MIT) | Несколько Bash с общей Arc<dyn FileSystem>, отдельным interpreter state; InMemoryFs/OverlayFs/MountableFs/RealFs; limits | Не запускает внешние binaries; фоновые jobs ограничены одним exec; POSIX modes не ACL; RealFs resolve затем pathname syscall, race-safe containment не доказано |
| redb 4.5.0 (MIT OR Apache-2.0) | Pure-Rust ACID KV, multi-table transactions, ranges, multimap, savepoints | Не VFS/sandbox; SQL/FTS/secondary indexes отсутствуют; shared-process modes требуют byte-range lock, Android backend не покрыт как Linux; не drop-in SQLite |

Предложение: общий host-backed storage проектов, отдельные shell instances; показываемые пути и права операций проходят единый FS policy. Read visibility — все проекты, запись — только текущий writable workspace по ownership. Fs/mount views могут различаться правами, оставаясь видами одного дерева, без копии файлов на shell. Mount-level readonly полезен, но динамические revocation/leases и guards нужны в runtime. Не полагаться на chmod VFS как на защиту.

Bashkit — серьёзный кандидат для встроенных команд/VFS, а не автоматическая замена всего execution stack. Внешние toolchains и долгоживущие процессы требуют существующего process lifecycle/launcher вне Bashkit. Подключение такого launcher не наследует гарантию no-spawn sandbox. Избегать двух несогласованных файловых представлений и незаметной потери env/cwd/functions между engines; точный мост требует отдельной проверки.

SQLite остаётся выбранным authority по D80. redb не имеет показанного преимущества для связанных session/task данных и history search; замена потребует собственных индексов и поиска. Не добавлять второй authority или DB-backed FS без конкретной потребности. Если redb использовать для обычных файлов проекта, внешние toolchains не увидят их без дополнительного слоя materialization/FS; такой слой сейчас не обоснован.

## Переписывание и границы

1. FS policy и path identity: одинаковые aliases не обходят write owner/vault exclusion; мутации через rename/copy/symlink тоже проверяются. Проверка строки/однократный canonicalize не устраняет внешний TOCTOU.
2. Донорские full-env копии не переносить; env shell явно отделён от credentials runtime.
3. Межпроцессный workspace lease нужен отдельно: process-local merge mutex не реализует D23.
4. Iso-копии — sibling-раздел managed root вне исходного repo, чтобы не копировать рабочие копии рекурсивно. Donor Git-only restrictions не становятся продуктовым требованием.
5. Scope инструкций, индексов и памяти определяется проектом/задачей, не текущим cd и не всей видимостью FS. Видеть проект не значит автоматически загрузить его инструкции.
6. Все команды, обходящие FS policy, должны быть явно классифицированы; VFS-фильтр не переносит контроль на произвольные subprocess.

## Linux и Termux

Landlock — kernel enforcement для разрешённых действий над деревьями, доступный начиная с Linux 5.13 при поддержке/включении; не виртуальный root и не полная изоляция процессов. Права и ABI проверяются отдельно, заранее открытые descriptors требуют учёта. bubblewrap зависит от доступных namespaces и выбранной policy.

На текущем ядре 5.4 нет upstream Landlock. Android seccomp/SELinux/настройки namespaces дополнительно ограничивают варианты; чужой GKI defconfig не доказывает конкретную vendor configuration. SIGSYS Landlock задокументирован у Termux на других Android-устройствах, но здесь syscall не запускали. PRoot — не принятая security boundary. cap-std помогает ограничить файловые обращения кода, который использует этот API, но не произвольные внешние программы. Универсальную enforced sandbox для Linux и unrooted Termux сейчас не обещать.

## Конфликты, проверки, решения

Архитектурного противоречия между D21/D27/D70/D76/D82 нет: внешние toolchain paths не равны нескольким проектным roots; managed root не равен целиком доступному shell root. Есть несовпадения реализации доноров с требованиями: VFS coverage, shell env, writer lease, persistent jobs, path identity и context discovery. Их устраняет продуктовый код, не новые пользовательские требования.

Проверены первичные документы/API и адресные исходники. Не доказаны сборка Linux/Android, отмена/quotas под нагрузкой, фактический escape или невозможность escape, расходы памяти, корректность сочетания engines. До принятия engine нужны разрешённые пользователем targeted smoke: две shell делят файлы, но не cwd/env; readonly чужого проекта; закрытые служебные пути; symlink/rename/stale-write; external-tool visibility; background lifecycle. Здесь такие проверки не запускались.

Открытые продуктовые решения: поведение внешних программ без enforcement (явно отложено). По лицензиям принято D83: GPL-кандидаты допустимы с проверкой конкретных обязательств, без автоматического выбора лицензии продукта. Выбор Bashkit/engine/DB остаётся предложением после source audit, не обязательным новым интервью.

## Первичные источники

- [OMP pi-vfs](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-vfs), [pi-shell](https://github.com/can1357/oh-my-pi/tree/035e4f95c314f9aff2f5a779fa335422680f8e36/crates/pi-shell).
- [Fork pi-shell](https://github.com/SklUpp/omp-rust-termux/tree/3008cfd0ca88bb0e28323561a165f39efa7159e1/crates/pi-shell).
- [Bashkit source pin](https://github.com/everruns/bashkit/tree/a3177376d4a23f6ae34320e9cc11674f635ab03f), [Fs guide](https://github.com/everruns/bashkit/blob/a3177376d4a23f6ae34320e9cc11674f635ab03f/docs/filesystem.md), [limitations](https://github.com/everruns/bashkit/blob/a3177376d4a23f6ae34320e9cc11674f635ab03f/knowledge/operations/limitations.md), [RealFs](https://github.com/everruns/bashkit/blob/a3177376d4a23f6ae34320e9cc11674f635ab03f/crates/bashkit/src/fs/realfs.rs).
- [redb 4.5.0 API](https://docs.rs/redb/4.5.0/redb/), [ConcurrencyMode](https://docs.rs/redb/4.5.0/redb/enum.ConcurrencyMode.html).
- [Landlock](https://docs.kernel.org/userspace-api/landlock.html), [Android seccomp](https://android-developers.googleblog.com/2017/07/seccomp-filter-in-android-o.html), [Termux Landlock SIGSYS issue](https://github.com/termux/termux-packages/issues/19347).
- [cap-std](https://github.com/bytecodealliance/cap-std), [bubblewrap](https://github.com/containers/bubblewrap), [Termux PRoot docs](https://wiki.termux.com/wiki/PRoot).

Внутренние подробные evidence текущей сессии: local://shared-fs-builtin.md, local://shared-fs-platform.md, local://redb-candidate.md. В local://shared-fs-conflicts.md C1/C2 отозваны parent как неверно названные конфликты; C5 — implementation gap. Этот итог имеет приоритет над первичным triage scout.
