# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Data Ingestion
# MAGIC
# MAGIC This notebook ingests synthetic clinical and genomic source files into Spark.
# MAGIC
# MAGIC Inputs:
# MAGIC - `patients.csv`
# MAGIC - `conditions.csv`
# MAGIC - `labs.csv`
# MAGIC - `variants.vcf`
# MAGIC
# MAGIC All data in this repository are synthetic and contain no PHI.

# COMMAND ----------

#import utilities for working with file paths and find the Git repo
import os
import glob

# find the databricks workspace path for this repo
repo_matches = glob.glob(
    "/Workspace/Users/*/clinico-genomic-data-pipeline"
)

print(repo_matches)

# COMMAND ----------

#use the matched databricks repo path and point it to the sample data folder
repo_path = repo_matches[0]
sample_data_path = f"{repo_path}/data/sample"

# COMMAND ----------

#confirm the expected source files are available

print(os.listdir(sample_data_path))

# COMMAND ----------

# Verify that the source file exists
patient_path = f"{sample_data_path}/patients.csv"

print(patient_path)
print(os.path.exists(patient_path))

# COMMAND ----------

# Define the expected schema for patient source data
patient_schema = """
    patient_id STRING,
    birth_year INT,
    sex STRING
"""

# Read patient data using the defined schema
patients_raw_df = (
    spark.read
    .option("header", True)
    .schema(patient_schema)
    .csv(f"file:{sample_data_path}/patients.csv")
)

# COMMAND ----------

# ## read the sample patient data csv into a spark df

# patients_raw_df = (
#     spark.read
#     .option("header", True)
#     .option("inferSchema", True)
#     .csv(f"file:{sample_data_path}/patients.csv")
# )

# # display(patients_raw_df)

# COMMAND ----------

# # Read sample condition data into a spark db

# conditions_raw_df = (
#     spark.read
#     .option("header", True)
#     .option("inferSchema", True)
#     .csv(f"file:{sample_data_path}/conditions.csv")
# )

# #display(conditions_raw_df)

# COMMAND ----------

condition_schema = """
    patient_id STRING,
    diagnosis_code STRING,
    diagnosis_name STRING,
    diagnosis_date STRING
"""

# COMMAND ----------

# # read sample lab data into a spark db

# labs_raw_df = (
#     spark.read
#     .option("header", True)
#     .option("inferSchema", True)
#     .csv(f"file:{sample_data_path}/labs.csv")
# )

# display(labs_raw_df)

# COMMAND ----------

lab_schema = """
    patient_id STRING,
    lab_name STRING,
    value DOUBLE,
    unit STRING
"""

# COMMAND ----------

conditions_raw_df = (
    spark.read
    .option("header", True)
    .schema(condition_schema)
    .csv(f"file:{sample_data_path}/conditions.csv")
)

labs_raw_df = (
    spark.read
    .option("header", True)
    .schema(lab_schema)
    .csv(f"file:{sample_data_path}/labs.csv")
)

# COMMAND ----------

# read the synthetic VCF as raw text
# parsing will happen later)

vcf_raw_df = spark.read.text(
    f"file:{sample_data_path}/variants.vcf"
)

display(vcf_raw_df)

# COMMAND ----------

# confirm source row counts and schemas as sanity check

for name, df in [
    ("patients", patients_raw_df),
    ("conditions", conditions_raw_df),
    ("labs", labs_raw_df),
    ("variants_vcf", vcf_raw_df),
]:
    print(f"\n{name}: {df.count()} rows")
    df.printSchema()

# COMMAND ----------

# Persist raw clinical datasets as Databricks tables,
# replacing both existing data and schema

patients_raw_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("raw_patients")

conditions_raw_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("raw_conditions")

labs_raw_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("raw_labs")

# COMMAND ----------

vcf_raw_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("raw_variants_vcf")

# COMMAND ----------

# MAGIC %sql
# MAGIC --verify with SQL
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC --verify with SQL
# MAGIC select * from raw_labs limit 10