# Project Taxes - Tax Rate Update Detection System

An ETL (Extract, Transform, Load) pipeline that processes tax data from multiple platforms, identifies discrepancies, and generates reports indicating which tax rates need to be updated and on which platform.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [ETL Process](#etl-process)
- [Merge Logic](#merge-logic)
- [Business Logic](#business-logic)
- [Output Reports](#output-reports)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)

---

## Overview

This system automates the process of comparing tax rate data across two platforms (APEX and COMMAND) with official tax rate edits from a regulatory source. The goal is to identify:

1. **Missing jurisdictions**: Cities/states present in one platform but not the other
2. **Rate discrepancies**: Tax rates that need to be updated based on official changes
3. **Action items**: Clear instructions on which platform needs updating

---

## Architecture

```
                    ┌─────────────────────────────────────────────────────────────┐
                    │                     DATA SOURCES                            │
                    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
                    │  │    APEX     │  │   COMMAND   │  │   Tax Rate Edits    │  │
                    │  │   (.xlsx)   │  │   (.xlsx)   │  │       (.xlsx)       │  │
                    │  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘  │
                    └─────────┼────────────────┼───────────────────┼──────────────┘
                              │                │                   │
                              ▼                ▼                   ▼
                    ┌─────────────────────────────────────────────────────────────┐
                    │                      ETL LAYER                              │
                    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
                    │  │ ETL_APEX.py │  │ETL_COMMAND  │  │   ETL_EDITS.py      │  │
                    │  │             │  │    .py      │  │                     │  │
                    │  │ - Block     │  │ - Simple    │  │ - State code        │  │
                    │  │   parsing   │  │   normalize │  │   conversion        │  │
                    │  │ - Extract   │  │ - Extract   │  │ - Rate extraction   │  │
                    │  │   city/state│  │   city/state│  │ - Build key         │  │
                    │  │ - Build key │  │ - Build key │  │                     │  │
                    │  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘  │
                    └─────────┼────────────────┼───────────────────┼──────────────┘
                              │                │                   │
                              │    city_state_key (join key)       │
                              ▼                ▼                   ▼
                    ┌─────────────────────────────────────────────────────────────┐
                    │                    MERGE LAYER                              │
                    │                    MERGING.py                               │
                    │                                                             │
                    │   Step 1: OUTER JOIN (APEX vs COMMAND)                      │
                    │   ┌─────────┐         ┌─────────┐                           │
                    │   │  APEX   │◄───────►│ COMMAND │                           │
                    │   │ (left)  │  outer  │ (right) │                           │
                    │   └────┬────┘  join   └────┬────┘                           │
                    │        │                   │                                │
                    │        └───────┬───────────┘                                │
                    │                ▼                                            │
                    │   ┌──────────────────────────────────┐                      │
                    │   │ Determines: update_platform      │                      │
                    │   │ - left_only  → ADD_TO_COMMAND    │                      │
                    │   │ - right_only → ADD_TO_APEX       │                      │
                    │   │ - both       → BOTH platforms    │                      │
                    │   └────────────────┬─────────────────┘                      │
                    │                    │                                        │
                    │   Step 2: INNER JOIN (with EDITS)                           │
                    │                    │                                        │
                    │   ┌────────────────▼─────────────────┐                      │
                    │   │        Platform Data             │                      │
                    │   │     (APEX + COMMAND merged)      │                      │
                    │   └────────────────┬─────────────────┘                      │
                    │                    │ inner join                             │
                    │   ┌────────────────▼─────────────────┐                      │
                    │   │          EDITS Data              │                      │
                    │   │   (old_rate, new_rate, dates)    │                      │
                    │   └────────────────┬─────────────────┘                      │
                    │                    │                                        │
                    │   Step 3: Filter by Business Rules                          │
                    │   - rate_change != 0                                        │
                    │   - change_type != 'Expired'                                │
                    └────────────────────┼────────────────────────────────────────┘
                                         │
                                         ▼
                    ┌─────────────────────────────────────────────────────────────┐
                    │                   REPORTING LAYER                           │
                    │                   Reporting.py                              │
                    │                                                             │
                    │   ┌─────────────────────────────────────────────────────┐   │
                    │   │                 Final Report                        │   │
                    │   │  - Records requiring update                         │   │
                    │   │  - Platform to update (APEX/COMMAND/BOTH)           │   │
                    │   │  - Old rate vs New rate                             │   │
                    │   │  - Action required                                  │   │
                    │   └─────────────────────────────────────────────────────┘   │
                    │                                                             │
                    │   Output formats: Excel (.xlsx), CSV, Console               │
                    └─────────────────────────────────────────────────────────────┘
```

---

## Data Sources

### 1. APEX File (`Tax Code Report_APEX.xlsx`)
Block-based format containing tax jurisdiction data:
- **Structure**: Blocks starting with `TaxCode` header row
- **Key data**: Tax code (column B), City/State (column C), Total Rate
- **Format example**: `ADDISON, TX`

### 2. COMMAND File (`Tax Code Report-COMMNAD.xlsx`)
Simple tabular format:
- **Columns**: Tax code, Description, Short description
- **Description format**: `CITY, ST` (e.g., `PLEASANTON, TX`)

### 3. Tax Rate Edits (`Tax Rate Edits.xlsx`)
Official tax rate changes from regulatory source:
- **Columns**: State, Jurisdiction Name, Old Rate, New Rate, Effective Date, Change Type
- **State format**: Full name (e.g., `Texas`, `Colorado`)
- **Jurisdiction format**: `City Name (Type)` (e.g., `Gilbert (City)`)

---

## ETL Process

Each source file has a dedicated ETL module that:
1. **Extracts** raw data from Excel
2. **Transforms** data into a normalized format
3. **Builds** a `city_state_key` for joining datasets

### ETL_APEX.py
Handles the complex block-based parsing:

```python
# Block structure detection
if tax_code_col == 'TaxCode':
    # Start of new block
    current_tax_code = row.iloc[1]   # Column B (ACT)
    current_location = row.iloc[2]   # Column C (CITY, STATE)

elif tax_code_col == 'Total Rate':
    # End of block - capture rate
    current_total_rate = float(row.iloc[1])
```

**City/State Parsing** uses regex pattern:
```python
pattern = r'^(.+?),\s*([A-Z]{2})$'
# Matches: "ADDISON, TX" → city="ADDISON", state="TX"
```

### ETL_COMMAND.py
Simple extraction from tabular data:
```python
tax_code = row['Tax code']
description = row['Description']  # "PLEASANTON, TX"
city, state = parse_city_state(description)
```

### ETL_EDITS.py
Converts state names to codes and extracts rates:
```python
# State conversion: "Texas" → "TX"
state_code = STATE_CODES.get(state_name.strip().upper())

# Rate extraction
old_rate = float(row['Old Rate'])
new_rate = float(row['New Rate'])
rate_change = new_rate - old_rate
```

### The `city_state_key`

All ETL modules create a normalized join key:
```python
def build_city_state_key(city: str, state: str) -> str:
    city_normalized = re.sub(r'\s+', ' ', city.strip().upper())
    state_normalized = state.strip().upper()
    return f"{city_normalized}_{state_normalized}"

# Example: "ADDISON_TX", "PLEASANTON_TX"
```

---

## Merge Logic

The merge process is the core of the business logic, implemented in `MERGING.py`.

### Step 1: OUTER JOIN (APEX vs COMMAND)

```python
df_merged = pd.merge(
    df_apex,
    df_command,
    on='city_state_key',
    how='outer',
    indicator=True  # Creates '_merge' column
)
```

**Why OUTER JOIN?**

An OUTER JOIN returns ALL records from BOTH tables:

| Scenario | `_merge` value | Meaning |
|----------|---------------|---------|
| City in APEX only | `left_only` | Missing from COMMAND |
| City in COMMAND only | `right_only` | Missing from APEX |
| City in both | `both` | Present in both platforms |

**Comparison with INNER JOIN:**

```
INNER JOIN: Only returns cities that exist in BOTH platforms
            → Would miss cities that need to be ADDED

OUTER JOIN: Returns ALL cities from BOTH platforms
            → Allows us to identify missing jurisdictions
```

**Visual example:**

```
APEX                    COMMAND               OUTER JOIN Result
┌──────────┐           ┌──────────┐          ┌────────────────────────┐
│ Austin   │           │ Austin   │          │ Austin    (both)       │
│ Dallas   │           │ Houston  │    →     │ Dallas    (left_only)  │
│ San Antonio│         │ San Antonio│        │ Houston   (right_only) │
└──────────┘           └──────────┘          │ San Antonio (both)     │
                                             └────────────────────────┘
```

### Step 2: Determine Update Platform

Based on the `_merge` indicator:

```python
def determine_update_platform(row):
    if row['_merge'] == 'left_only':
        return 'ADD_TO_COMMAND'  # Exists in APEX, add to COMMAND
    elif row['_merge'] == 'right_only':
        return 'ADD_TO_APEX'     # Exists in COMMAND, add to APEX
    else:
        return 'BOTH'            # May need rate update on both
```

### Step 3: INNER JOIN with EDITS

```python
df_merged = pd.merge(
    df_platforms,
    df_edits,
    on='city_state_key',
    how='inner'  # Only keep jurisdictions with rate changes
)
```

**Why INNER JOIN here?**

We only want jurisdictions that:
1. Exist in our platform data (APEX/COMMAND)
2. AND have pending rate changes (EDITS)

Jurisdictions without rate changes are not included in the final report.

---

## Business Logic

### Filtering Rules

Records are filtered based on:

1. **Rate change exists**: `rate_change != 0`
2. **Not expired**: `change_type != 'Expired'`

### Action Determination

```python
def determine_action(row):
    if row['update_platform'] == 'ADD_TO_COMMAND':
        return 'Add to COMMAND'
    elif row['update_platform'] == 'ADD_TO_APEX':
        return 'Add to APEX'
    elif row['rate_change'] > 0:
        return 'Rate increase'
    elif row['rate_change'] < 0:
        return 'Rate decrease'
    else:
        return 'No change'
```

---

## Output Reports

### Report Content

The final report includes:

| Column | Description |
|--------|-------------|
| `city_state_key` | Unique identifier |
| `city` | City name |
| `state` | State code (2 letters) |
| `tax_code_apex` | Tax code in APEX |
| `tax_code_command` | Tax code in COMMAND |
| `old_rate` | Previous tax rate |
| `new_rate` | Updated tax rate |
| `rate_change` | Difference (new - old) |
| `action_required` | What needs to be done |
| `effective_date` | When change takes effect |
| `update_platform` | Which platform(s) to update |

### Export Formats

- **Excel** (`.xlsx`): Multi-sheet workbook with updates and summary
- **CSV**: Simple flat file for data processing
- **Console**: Formatted output for quick review

---

## Installation

### Requirements

```bash
pip install pandas openpyxl
```

### Dependencies

- Python 3.7+
- pandas
- openpyxl (for Excel file handling)

---

## Usage

### Run the Full Pipeline

```bash
cd code_py
python Main.py
```

### Run Individual Modules

```bash
# Test APEX ETL
python ETL_APEX.py

# Test COMMAND ETL
python ETL_COMMAND.py

# Test EDITS ETL
python ETL_EDITS.py

# Test Merge Logic
python MERGING.py

# Validate Configuration
python config.py
```

### Expected Output

```
############################################################
#   SISTEMA ETL DE IMPUESTOS - PROJECT TAXES               #
############################################################

============================================================
LOADING DOCUMENTS
============================================================
  ✓ APEX: Tax Code Report_APEX.xlsx
  ✓ COMMAND: Tax Code Report-COMMNAD.xlsx
  ✓ EDITS: Tax Rate Edits.xlsx

============================================================
EXECUTING ETL
============================================================
[1/3] Processing APEX...
      → 367 records extracted

[2/3] Processing COMMAND...
      → 367 records extracted

[3/3] Processing EDITS...
      → 1119 records extracted

============================================================
EXECUTING MERGE (BUSINESS LOGIC)
============================================================
Step 1: OUTER JOIN APEX vs COMMAND...
  - Records after OUTER JOIN: 420

Step 2: INNER JOIN with EDITS...
  - Records after INNER JOIN: 6

Step 3: Filtering records requiring update...
  - Records requiring update: 6

============================================================
GENERATING REPORTS
============================================================
Report exported to: output/tax_update_report_YYYYMMDD_HHMMSS.xlsx
```

---

## Project Structure

```
Project Taxes/
├── Base de datos/                    # Data sources
│   ├── Tax Code Report_APEX.xlsx
│   ├── Tax Code Report-COMMNAD.xlsx
│   └── Tax Rate Edits.xlsx
│
├── code_py/                          # Source code
│   ├── config.py                     # Centralized configuration
│   ├── ETL_APEX.py                   # APEX extraction/transformation
│   ├── ETL_COMMAND.py                # COMMAND extraction/transformation
│   ├── ETL_EDITS.py                  # EDITS extraction/transformation
│   ├── MERGING.py                    # Business logic (joins)
│   ├── Reporting.py                  # Report generation
│   └── Main.py                       # Pipeline orchestrator
│
├── output/                           # Generated reports
│   └── tax_update_report_*.xlsx
│
└── README.md                         # This file
```

---

## Configuration

All settings are centralized in `config.py`:

```python
# File paths
FILES = {
    'APEX': DATA_DIR / "Tax Code Report_APEX.xlsx",
    'COMMAND': DATA_DIR / "Tax Code Report-COMMNAD.xlsx",
    'EDITS': DATA_DIR / "Tax Rate Edits.xlsx"
}

# State code mapping (USPS standard)
STATE_CODES = {
    'TEXAS': 'TX',
    'COLORADO': 'CO',
    # ... all 50 states + DC
}

# Report settings
REPORT_CONFIG = {
    'export_excel': True,
    'export_csv': True,
    'print_console': True
}
```

---

## Key Concepts Summary

| Concept | Implementation | Purpose |
|---------|---------------|---------|
| `city_state_key` | `"CITY_STATE"` format | Unique identifier for joining |
| OUTER JOIN | `pd.merge(how='outer')` | Find ALL jurisdictions |
| INNER JOIN | `pd.merge(how='inner')` | Match only with rate changes |
| `_merge` indicator | pandas merge indicator | Determine data source |
| Block parsing | State machine in ETL_APEX | Handle APEX file format |
| State codes | `STATE_CODES` dict | Convert full names to codes |

---

## License

Internal project - Not for distribution.
