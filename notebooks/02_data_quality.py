# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Data Quality
# MAGIC
# MAGIC This notebook reads the raw ingested tables, checks for common data-quality issues, and creates cleaned clinical datasets for downstream transformation.

# COMMAND ----------

# load the persistent raw tables created by the 01 ingestion notebook (spark df aren't persistent)

patients_df = spark.table("raw_patients")
conditions_df = spark.table("raw_conditions")
labs_df = spark.table("raw_labs")

# COMMAND ----------

# confirm the raw tables loaded successfully

for name, df in [
    ("patients", patients_df),
    ("conditions", conditions_df),
    ("labs", labs_df),
]:
    print(f"{name}: {df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC Identifying messy data each table

# COMMAND ----------

# MAGIC %md
# MAGIC labs table

# COMMAND ----------

# Identify lab rows with missing result values 

display(
    labs_df.filter(
        labs_df.value.isNull()
    )
)

# COMMAND ----------

# Identify exact duplicate lab rows

display(
    labs_df
    .groupBy("patient_id", "lab_name", "value", "unit")
    .count()
    .filter("count > 1")
)

# COMMAND ----------

# Inspect the units used for each lab test

display(
    labs_df
    .select("lab_name", "unit")
    .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC patient table

# COMMAND ----------

# Check for missing patient IDs

display(
    patients_df.filter(
        patients_df.patient_id.isNull()
    )
)

# COMMAND ----------

# Check for duplicate patient IDs

display(
    patients_df
    .groupBy("patient_id")
    .count()
    .filter("count > 1")
)

# COMMAND ----------

# Check for unexpected sex values

display(
    patients_df
    .select("sex")
    .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC conditions table

# COMMAND ----------

# Check for missing patient IDs or diagnosis codes

display(
    conditions_df.filter(
        conditions_df.patient_id.isNull()
        | conditions_df.diagnosis_code.isNull()
    )
)

# COMMAND ----------

# Check for missing patient IDs or diagnosis codes

display(
    conditions_df.filter(
        conditions_df.patient_id.isNull()
        | conditions_df.diagnosis_code.isNull()
    )
)

# COMMAND ----------

# Find condition records whose patient does not exist in the patient table

display(
    conditions_df.join(
        patients_df,
        on="patient_id",
        how="left_anti"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC cleaning labs data

# COMMAND ----------

from pyspark.sql.functions import col, when

# Remove unusable and duplicate records, then standardize LDL units to mg/dL

labs_clean_df = (
    labs_df
    .filter(col("value").isNotNull())
    .dropDuplicates()
    .withColumn(
        "value_standardized",
        when(
            col("unit") == "mmol/L",
            col("value") * 38.67
        ).otherwise(col("value"))
    )
    .withColumn(
        "unit_standardized",
        when(
            col("lab_name") == "LDL",
            "mg/dL"
        ).otherwise(col("unit"))
    )
)

display(labs_clean_df)

# COMMAND ----------

#create cleaned patient and condition datasets

patients_clean_df = (
    patients_df
    .filter(col("patient_id").isNotNull())
    .dropDuplicates(["patient_id"])
)

conditions_clean_df = (
    conditions_df
    .filter(
        col("patient_id").isNotNull()
        & col("diagnosis_code").isNotNull()
    )
    .dropDuplicates()
)

# COMMAND ----------

# Persist cleaned clinical datasets for downstream pipeline stages

patients_clean_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("clean_patients")

conditions_clean_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("clean_conditions")

labs_clean_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("clean_labs")

# COMMAND ----------

# MAGIC %sql
# MAGIC --verfiy
# MAGIC SHOW TABLES;