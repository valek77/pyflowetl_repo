# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**pyflowetl** is a Python ETL pipeline library with a fluent/builder API centered on pandas DataFrames. It has a strong focus on Italian business data (codice fiscale, partita IVA, comuni/province/regioni, Italian phone normalization).

## Build & Development Commands

```bash
# Install in development mode (uses uv as package manager)
uv pip install -e .

# Build package
uv build

# Sync dependencies from lockfile
uv sync
```

No test suite or linter is currently configured. The `.gitignore` references `.ruff_cache/` and pytest cache, suggesting ruff and pytest are intended but not yet set up.

## Architecture

**Source layout**: `src/pyflowetl/` with setuptools `src/` layout. Python >=3.10.

### Core Pipeline (`pipeline.py`)

`EtlPipeline` wraps a `pandas.DataFrame` (`self.data`) and supports method chaining — all pipeline methods return `self` or a new pipeline. Key methods:

- `.extract(extractor)` / `.preprocess(preprocessor)` / `.transform(transformer)` / `.load(loader)`
- `.filter(expr)` — uses pandas `.query()` with Python engine
- `.sql_filter(sql)` — uses DuckDB SQL
- `.split(names, fn)` — splits into named sub-pipelines
- `.join_with(other)` / `.anti_join_with(other)` / `.clone()` / `.df()`

### Component Interfaces (duck-typed, no formal ABC)

| Component | Method | Location |
|---|---|---|
| Extractors | `.extract() -> DataFrame` | `extractors/` |
| Preprocessors | `.process(df) -> DataFrame` | `preprocessors/` |
| Transformers | `.transform(df) -> DataFrame` | `transformers/` |
| Loaders | `.load(df)` | `loaders/` |
| Validators | `.validate(value) -> bool` | `validators/` (ABC: `BaseValidator`) |

### Key Modules

- **Extractors**: CSV (auto-detects encoding via chardet), XLSX, PostgreSQL (SQLAlchemy), ClickHouse
- **Transformers**: ~29 transformers including Italian geo enrichment (via bundled `data/gi_comuni_cap.csv`), name parsing (nameparser/cleanco/unidecode), codice fiscale decoding, date/phone/text operations, DuckDB SQL filtering
- **Loaders**: CSV (with file splitting), XLSX, DuckDB, PostgreSQL, ClickHouse — DB loaders support insert/update/upsert modes via `config` dict with `table`, `unique_keys`, and `columns` mapping
- **Validators**: Used with `ValidateColumnsTransformer` — codice fiscale, partita IVA, date format, regex, phone, not-empty
- **Logging** (`log.py`): Singleton logger with `RotatingFileHandler` (5MB, 3 backups) + stdout. Call `set_log_file()` before first use. `log_memory_usage()` tracks RSS via psutil.

### Public API (`__init__.py`)

Exports: `EtlPipeline`, `get_logger`, `set_log_file`, `log_memory_usage`

### Bundled Data

`src/pyflowetl/data/` contains CSV reference files (Italian municipalities/CAP, IP addresses) declared in `pyproject.toml` package-data.

## Key Dependencies

pandas, duckdb, sqlalchemy, psycopg2, clickhouse-driver, openpyxl, chardet, nameparser, cleanco, spacy, unidecode, rapidfuzz, metaphone, psutil
