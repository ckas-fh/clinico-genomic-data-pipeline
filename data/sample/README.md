# Sample Data (Synthetic)

**All files in this folder are synthetic.** They were made up for demonstration and do not describe real people. They contain no PHI and are safe to share publicly.

| File | Contents |
|---|---|
| `patients.csv` | Patient demographics (birth year, sex) |
| `conditions.csv` | Diagnoses (ICD-10 codes) |
| `labs.csv` | LDL cholesterol lab results |
| `variants.vcf` | Genotypes for one LDLR variant across the same patients |

Patients `P001`–`P004` link across all four files.

## Intentional Data-Quality Issues

`labs.csv` contains deliberate problems for the quality control step (`02_data_quality.ipynb`) to detect and fix:

- **Unit mismatch:** `P003` LDL is reported in `mmol/L`, while the others use `mg/dL`.
- **Missing value:** One `P004` LDL row has no value.
- **Duplicate row:** One `P004` LDL row (`125.0 mg/dL`) appears twice.
