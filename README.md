# Project — ADF + Databricks Pipeline: Chicago Crime Dataset


## Overview

An end-to-end data engineering pipeline that ingests the **Chicago Crime Dataset** from the City of Chicago Open Data Portal, orchestrates data movement using **Azure Data Factory (ADF)**, and performs layered transformations using **Azure Databricks (PySpark)** following the **Medallion Architecture** (Raw → Bronze → Silver → Gold).

---

## Architecture

```
Chicago Data Portal (HTTP)
        │
        ▼
┌─────────────────────┐
│  ADF Copy Activity  │  ← Ingests 50,000 records via HTTP
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│     Raw Layer       │  ← chicago-crime/raw/chicago_crime_raw.csv
│     (CSV)           │
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  ADF Notebook       │
│  Activity (Bronze)  │
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│   Bronze Layer      │  ← chicago-crime/bronze/ (Parquet)
│   (Raw as Parquet)  │
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  ADF Notebook       │
│  Activity (Silver)  │
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│   Silver Layer      │  ← chicago-crime/silver/ (Cleaned Parquet)
│   (Cleaned)         │
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  ADF Notebook       │
│  Activity (Gold)    │
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│    Gold Layer       │  ← chicago-crime/gold/ (Aggregations)
│  (Aggregations)     │
└─────────────────────┘
        │
        ▼
   Power BI Dashboard
```

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Orchestration | Azure Data Factory (ADF) |
| Compute | Azure Databricks (PySpark) |
| Storage | Azure Data Lake Storage Gen2 (ADLS) |
| Secret Management | Azure Key Vault + Databricks Secret Scope |
| File Format | CSV (Raw) → Parquet (Bronze/Silver/Gold) |
| Visualization | Power BI |
| Language | Python (PySpark) |

---

## Dataset

| Detail | Info |
|--------|------|
| Source | City of Chicago Open Data Portal |
| URL | `https://data.cityofchicago.org/resource/ijzp-q8t2.csv?$limit=50000` |
| Full Dataset Size | ~8.2 million records |
| Records Used | 50,000 (development subset) |
| Columns | 22 |
| Format | CSV |

---

## ADLS Container Structure

```
chicago-crime/
├── raw/
│   └── chicago_crime_raw.csv
├── bronze/
│   └── *.parquet
├── silver/
│   └── *.parquet
└── gold/
    ├── crimes_by_type/
    ├── crimes_by_year/
    ├── crimes_by_district/
    ├── arrest_rate_by_type/
    └── top_crime_locations/
```

---

## ADF Pipeline

### Pipeline: `PL_ChicagoCrime_Ingest`

| Activity | Type | Description |
|----------|------|-------------|
| `Copy_ChicagoCrime_Raw` | Copy Activity | HTTP → ADLS Raw (CSV) |
| `ACT_Notebook_Bronze` | Notebook Activity | Triggers Bronze notebook |
| `ACT_Notebook_Silver` | Notebook Activity | Triggers Silver notebook |
| `ACT_Notebook_Gold` | Notebook Activity | Triggers Gold notebook |

### Linked Services

| Name | Type | Purpose |
|------|------|---------|
| `LS_HTTP_ChicagoCrime` | HTTP | Connect to Chicago Data Portal |
| `LS_ADLS_ChicagoCrime` | ADLS Gen2 | Connect to storage account |
| `LS_Databricks_ChicagoCrime` | Azure Databricks | Trigger notebooks |

### Datasets

| Name | Type | Purpose |
|------|------|---------|
| `DS_HTTP_ChicagoCrime_CSV` | HTTP (DelimitedText) | Source dataset |
| `DS_ADLS_ChicagoCrime_Raw` | ADLS Gen2 (DelimitedText) | Sink dataset |

---

## Databricks Notebooks

### `NB_ChicagoCrime_Bronze`

| Cell | Purpose |
|------|---------|
| Cell 1 | Configure ADLS paths + Key Vault secret |
| Cell 2 | Read raw CSV from ADLS |
| Cell 3 | Limit to 50,000 records |
| Cell 4 | Write as Parquet to Bronze layer |

### `NB_ChicagoCrime_Silver`

| Cell | Purpose |
|------|---------|
| Cell 1 | Configure ADLS paths + Key Vault secret |
| Cell 2 | Read Bronze Parquet |
| Cell 3 | Type casting (Date/Timestamp) + trim strings + drop redundant columns |
| Cell 4 | Null count analysis |
| Cell 5 | Null handling (fillna) + verification |
| Cell 6 | Write cleaned data to Silver layer |

**Transformations Applied:**

| Transformation | Column | Action |
|---------------|--------|--------|
| Type Cast | `Date` | String → Timestamp |
| Type Cast | `Updated On` | String → Timestamp |
| Trim | `Primary Type`, `Description`, `Location Description` | Remove whitespace |
| Drop | `Location` | Redundant with Lat/Long |
| Fill Null | `Location Description` | → `"Unknown"` |
| Fill Null | `Ward`, `Community Area` | → `0` |
| Fill Null | `X Coordinate`, `Y Coordinate` | → `0` |
| Fill Null | `Latitude`, `Longitude` | → `0.0` |

### `NB_ChicagoCrime_Gold`

| Cell | Purpose |
|------|---------|
| Cell 1 | Configure ADLS paths + Key Vault secret |
| Cell 2 | Read Silver Parquet |
| Cell 3 | Re-apply null handling |
| Cell 4 | Aggregation — Crimes by Primary Type |
| Cell 5 | Aggregation — Crimes by Year |
| Cell 6 | Aggregation — Crimes by District |
| Cell 7 | Aggregation — Arrest Rate by Crime Type |
| Cell 8 | Aggregation — Top 10 Crime Locations |
| Cell 9 | Write all aggregations to Gold layer |

---

## Gold Layer — Key Insights

### Top 5 Crime Types
| Primary Type | Total Crimes |
|-------------|-------------|
| THEFT | 10,885 |
| BATTERY | 8,080 |
| CRIMINAL DAMAGE | 5,287 |
| MOTOR VEHICLE THEFT | 5,097 |
| ASSAULT | 4,168 |

### Top 5 Districts by Crime
| District | Total Crimes |
|---------|-------------|
| 8 | 3,416 |
| 6 | 3,014 |
| 12 | 3,013 |
| 4 | 2,948 |
| 19 | 2,717 |

### Highest Arrest Rates
| Crime Type | Arrest Rate % |
|-----------|--------------|
| GAMBLING | 100.0% |
| NARCOTICS | 97.65% |
| LIQUOR LAW VIOLATION | 93.10% |

### Top Crime Locations
| Location | Total Crimes |
|---------|-------------|
| STREET | 14,213 |
| APARTMENT | 9,104 |
| RESIDENCE | 6,244 |

---

## Security

| Component | Implementation |
|-----------|---------------|
| Storage Key | Stored in Azure Key Vault |
| Secret Access | Databricks Secret Scope (Key Vault backed) |
| Key never exposed | Retrieved via `dbutils.secrets.get()` at runtime |

---

## Known Issues & Notes

| Issue | Root Cause | Resolution |
|-------|-----------|------------|
| 2023 data skew (47K/50K records) | Socrata API default sort returns newest records first | Expected for dev subset; use `$order` param in production |
| Spark 3.x timestamp parse error | New strict datetime parser in Spark >= 3.0 | Set `spark.sql.legacy.timeParserPolicy = LEGACY` |
| Spatial nulls (1,248 rows) | Crimes recorded without location data | Filled with 0 — rows preserved |

---

## How to Run

1. Ensure Databricks cluster is running
2. Verify Key Vault secret `adls-storage-key` is accessible
3. Go to ADF Studio → `PL_ChicagoCrime_Ingest`
4. Click **Add Trigger → Trigger Now** (or Debug for testing)
5. Monitor pipeline run in **Monitor** tab
6. Verify Gold layer outputs in ADLS `chicago-crime/gold/`

---

## Author

**Rajkumar** — QA Automation Architect  
*Azure Data Engineering Portfolio Project*
