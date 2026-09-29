# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Build Clinico-Genomic Cohort
# MAGIC
# MAGIC This notebook combines simplified OMOP clinical tables with processed genomic data to create an analysis-ready clinico-genomic cohort.
# MAGIC
# MAGIC Output:
# MAGIC - `clinico_genomic_cohort`

# COMMAND ----------

# Load processed clinical and genomic tables

person_df = spark.table("omop_person")
condition_df = spark.table("omop_condition_occurrence")
measurement_df = spark.table("omop_measurement")
genomic_df = spark.table("genomic_variants")

# COMMAND ----------

# Import PySpark functions used in this notebook (col() and when() are functions)
from pyspark.sql.functions import col, when

# COMMAND ----------

# Identify people with a hyperlipidemia diagnosis

hyperlipidemia_df = (
    condition_df
    .filter(col("condition_source_name") == "Hyperlipidemia")
    .select("person_id")
    .distinct()
)

# COMMAND ----------

# Select LDL measurements

ldl_df = (
    measurement_df
    .filter(col("measurement_source_value") == "LDL")
    .select(
        "person_id",
        col("value_as_number").alias("ldl_mg_dl")
    )
)

# COMMAND ----------

# Join patient, genomic, diagnosis, and LDL data into one cohort

cohort_df = (
    person_df
    .join(
        genomic_df.select(
            "person_id",
            "gene",
            "genotype",
            "variant_carrier"
        ),
        on="person_id",
        how="left"
    )
    .join(
        hyperlipidemia_df.withColumn(
            "hyperlipidemia",
            when(col("person_id").isNotNull(), 1)
        ),
        on="person_id",
        how="left"
    )
    .join(
        ldl_df,
        on="person_id",
        how="left"
    )
    .fillna(
        {"hyperlipidemia": 0}
    )
)

display(cohort_df)

# COMMAND ----------

# Select the final analysis-ready cohort fields

cohort_final_df = cohort_df.select(
    "person_id",
    "year_of_birth",
    "gender_concept_id",
    "gene",
    "genotype",
    "variant_carrier",
    "hyperlipidemia",
    "ldl_mg_dl"
)

display(cohort_final_df)

# COMMAND ----------

# Save the final clinico-genomic cohort as a persistent Databricks table

cohort_final_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("clinico_genomic_cohort")

# COMMAND ----------

# MAGIC %sql
# MAGIC --verify
# MAGIC
# MAGIC SELECT *
# MAGIC FROM clinico_genomic_cohort
# MAGIC ORDER BY person_id;