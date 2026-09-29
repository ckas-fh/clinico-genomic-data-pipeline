# Clinico-Genomic Data Pipeline

A hands-on demonstration of clinical and genomic data processing, quality control, cohort development, and analysis using Databricks, PySpark, SQL, and Python.

> **Status:** In progress. This project uses **synthetic data only**. No real patient data or PHI is stored in this repository.

## Overview

Clinico-genomic datasets link patients' clinical records (diagnoses, medications, labs, outcomes) with their genomic results (variants, genes, test reports). This project walks through an end-to-end pipeline for bringing those sources together into analysis-ready cohorts.

## Pipeline Stages

1. **Ingestion:** Load raw clinical and genomic source data into Databricks.
2. **Processing:** Clean, standardize, and harmonize clinical and genomic data with PySpark and SQL.
3. **Quality Control:** Check completeness, validity, consistency, and duplicates, and report data quality metrics.
4. **Cohort Development:** Define patient cohorts using clinical criteria and genomic findings.
5. **Analysis:** Explore and summarize cohorts, including variant prevalence and clinical characteristics.

## Tech Stack

- **Databricks:** notebooks and compute environment
- **PySpark:** distributed data processing
- **SQL:** transformations and cohort queries
- **Python:** analysis and visualization

## Repository Structure

```
notebooks/
  01_data_ingestion.py        # load sample CSVs and VCF into raw_* tables
  02_data_quality.py          # QC checks, cleaning, unit standardization -> clean_* tables
  03_omop_transformation.py   # simplified OMOP person, condition_occurrence, measurement
  04_genomic_processing.py    # parse VCF into genomic_variants (one row per patient per variant)
  05_build_cohort.py          # join clinical + genomic data -> clinico_genomic_cohort
  06_analysis.py              # LDL by LDLR carrier status, simple linear regression
data/sample/                  # small synthetic input files
```

Notebooks are stored in Databricks source format (`.py`) and open as notebooks in a Databricks Git folder. Run them in order.

## Planned Improvements

- **Cohort grain (05):** joining per-variant genomic rows and per-measurement LDL rows can create multiple rows per person. Aggregate to one row per person (for example, carrier status per gene and a single LDL value) before joining, so counts in 06 reflect people, not rows.
- **Unit conversion (02):** the mmol/L to mg/dL factor (38.67) is applied to every lab reported in mmol/L. Scope it to cholesterol labs and add lab-specific factors (for example, glucose uses 18) as more labs are added.
- **VCF sample columns (04):** patient columns P001–P004 are hardcoded. Read sample IDs from the `#CHROM` header line so any number of patients is supported.
- **Regression inputs (06):** `VectorAssembler` and `LinearRegression` fail on null `variant_carrier` or `ldl_mg_dl`. Filter or impute missing values before fitting.
- **Duplicate cell (02):** the missing patient ID / diagnosis code check appears twice.

## Getting Started

_Coming soon._

## Author

**Caroline Kasman**, [carolinekasman.com](http://carolinekasman.com) · [LinkedIn](https://linkedin.com/in/carolinekasman)
