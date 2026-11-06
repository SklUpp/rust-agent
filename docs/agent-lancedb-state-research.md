# LanceDB отдельно: состояние, память, retrieval

Дата 2026-11-06. Source-only; кандидат, не accepted dependency. SQLite D80 остаётся authority. Версия: lancedb 0.41.0, Lance =14.0.2, Apache-2.0, заявленный MSRV 1.94; Arrow 58/DataFusion 54/object_store 0.14.1. Main отличается, не смешивать с release. Runtime/build/device proof отсутствует.

## Назначение

Embedded vector/FTS/hybrid retrieval для производных chunks памяти/контекста, не замена session/history/call/delivery store. OSS embedded работает в процессе без daemon; cloud/remote optional и не нужны для этого сценария.

## Механизмы

Rust connect(path).execute открывает локальное хранилище. Arrow FixedSizeList хранит готовые векторы; embedding function не обязательна, если векторы вычисляет runtime. Есть metadata predicates, vector exact/ANN, FTS с предварительным индексом и hybrid с RRF. merge_insert позволяет keyed upsert; стабильные product IDs нужны отдельно: _rowid может измениться после compaction/update.

read_consistency_interval по умолчанию не проверяет чужие изменения; 0 проверяет перед чтением. Writes consistent по API; это не транзакция с SQLite. fast_search исключает непроиндексированные строки: разрешим только при объявленной partial completeness. Для FTS поведение после append требует отдельного proof.

Lance хранит immutable manifests/fragments и версии. Delete логически скрывает строки; освобождение старых данных требует maintenance/retention. Prune делает старые версии недоступными. Time travel не заменяет архив D80; конфликта с D80 нет, если LanceDB всегда лишь перестраиваемая проекция.

## Предложение parent

Пока не включать обязательной зависимостью. Сохранить как кандидата supplemental retrieval, когда выбран retrieval contract и показана польза на наших данных. Не менять SQLite ради наличия vector API. Для небольшого набора векторов точный поиск по сохранённым векторам тоже кандидат, не принятый потолок возможностей.

Если LanceDB выбран: сообщения/память, product ID/revision, provenance и deletion state остаются authority в SQLite. Запись source revision и pending index work в одной SQLite transaction; после этого индекс получает идемпотентный upsert/delete. Ack progress сохраняется после применения, повтор допускается контрактом проекции. Потерянный/несовместимый индекс перестраивается; watermark сам по себе не доказывает существование таблицы. Query возвращает candidate IDs+revision; runtime сверяет scope/current source/deletion, затем берёт текст из authority. Так stale index не возвращает удалённую запись. Допустимый lag, ranking и полнота остаются явным контрактом.

Не вводить обязательный отдельный writer framework: один index worker достаточно до доказанной нужды в другом. Native concurrency engine не заменяет product revision ordering; старый upsert не должен воскресить новое удаление. Embedding model/dimension/normalization/version сохраняются вне перестраиваемой таблицы. Изменение модели требует согласованной новой generation, не смешивания несовместимых векторов.

## Зависимости и проверки

Arrow/DataFusion и C codecs lz4/zstd/lzma добавляют native closure. Default features, static linking и license obligations проверяются по точному lockfile; отсутствие root NOTICE не доказывает closure. Наличие ARM64 GNU/Linux CI не доказывает Bionic. Android build, RSS/size, IVF training memory, offline build requirements и consistency backup не проверены. LOC/doc build time не доказывают длительность полной сборки.

После отдельного разрешения: CI cross-build Linux/Android; device smoke готового artifact; interrupted upsert replay; stale reader; append после FTS index; revision delete vs late upsert; rebuild после потери индекса; physical retention. Не строить тяжёлую зависимость на телефоне. /sdcard FUSE hard-link/fsync не считать эквивалентом app-private storage.

## Решения на согласование

Сейчас нового вопроса нет: обязательное включение/embedding backend не утверждены. Конкретный engine выбирать после измеримого retrieval сценария; standalone runtime допустим для embedded LanceDB, но переносимость пока не доказана.

## Источники

- https://docs.rs/lancedb/0.41.0/lancedb/
- https://docs.rs/lancedb/0.41.0/lancedb/connection/struct.ConnectBuilder.html
- https://docs.rs/crate/lancedb/0.41.0/source/Cargo.toml.orig
- https://lance.org/format/table/
- https://github.com/lancedb/lancedb
- https://github.com/lance-format/lance

Подробные первичные anchors исследовательской сессии: local://lancedb-state-research.md. Его категоричные предложения «never fast_search», «единственный primitive», оценка hours build и NOTICE closure не приняты parent; выше исправлены.
