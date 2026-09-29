# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — OMOP Transformation
# MAGIC
# MAGIC This notebook transforms cleaned synthetic clinical data into simplified OMOP-style tables for downstream research use.
# MAGIC
# MAGIC Outputs:
# MAGIC - `omop_person`
# MAGIC - `omop_condition_occurrence`
# MAGIC - `omop_measurement`
# MAGIC
# MAGIC This is a simplified educational implementation and is not intended to represent a complete production OMOP CDM.

# COMMAND ----------

# Load cleaned clinical tables from the previous pipeline stage

patients_df = spark.table("clean_patients")
conditions_df = spark.table("clean_conditions")
labs_df = spark.table("clean_labs")

# COMMAND ----------

from pyspark.sql.functions import col, when, regexp_extract

# Transform cleaned patient data into a simplified OMOP PERSON table, making OMOP style fields

person_df = (
    patients_df
    .withColumn(
        "person_id",
        regexp_extract(col("patient_id"), r"(\d+)", 1).cast("integer")
    )
    .withColumn(
        "gender_concept_id",
        when(col("sex") == "F", 8532)
        .when(col("sex") == "M", 8507)
        .otherwise(0)
    )
    .select(
        "person_id",
        "gender_concept_id",
        col("birth_year").alias("year_of_birth"),
        col("patient_id").alias("person_source_value"),
        col("sex").alias("gender_source_value")
    )
)

display(person_df)

# COMMAND ----------

from pyspark.sql.functions import to_date

# Transform cleaned diagnosis data into a simplified OMOP CONDITION_OCCURRENCE table

condition_occurrence_df = (
    conditions_df
    .withColumn(
        "person_id",
        regexp_extract(col("patient_id"), r"(\d+)", 1).cast("integer")
    )
    .withColumn(
        "condition_start_date",
        to_date(col("diagnosis_date"))
    )
    .select(
        "person_id",
        "condition_start_date",
        col("diagnosis_code").alias("condition_source_value"),
        col("diagnosis_name").alias("condition_source_name")
    )
)

display(condition_occurrence_df)

# COMMAND ----------

# Transform cleaned lab data into a simplified OMOP MEASUREMENT table

measurement_df = (
    labs_df
    .withColumn(
        "person_id",
        regexp_extract(col("patient_id"), r"(\d+)", 1).cast("integer")
    )
    .select(
        "person_id",
        col("lab_name").alias("measurement_source_value"),
        col("value_standardized").alias("value_as_number"),
        col("unit_standardized").alias("unit_source_value")
    )
)

display(measurement_df)

# COMMAND ----------

# Persist simplified OMOP tables for downstream pipeline stages

person_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("omop_person")

condition_occurrence_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("omop_condition_occurrence")

measurement_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("omop_measurement")

# COMMAND ----------

# MAGIC %sql
# MAGIC --verify
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

