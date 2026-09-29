# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Genomic Processing
# MAGIC
# MAGIC This notebook parses the synthetic VCF data and transforms it into an analysis-ready genomic table.
# MAGIC
# MAGIC Output:
# MAGIC - `genomic_variants`
# MAGIC
# MAGIC All genomic data are synthetic and intended only for demonstration.

# COMMAND ----------

 # Load the raw VCF text persisted during ingestion

vcf_raw_df = spark.table("raw_variants_vcf")

display(vcf_raw_df)

# COMMAND ----------

from pyspark.sql.functions import col

# Remove VCF metadata/header rows beginning with #

vcf_data_df = vcf_raw_df.filter(
    ~col("value").startswith("#")
)

display(vcf_data_df)

# COMMAND ----------

from pyspark.sql.functions import split

# Split the VCF row into its tab-delimited fields

vcf_split_df = vcf_data_df.withColumn(
    "fields",
    split(col("value"), "\t")
)

display(vcf_split_df)

# COMMAND ----------

# Extract VCF fields into named columns

vcf_parsed_df = vcf_split_df.select(
    col("fields")[0].alias("chrom"),
    col("fields")[1].alias("pos"),
    col("fields")[3].alias("ref"),
    col("fields")[4].alias("alt"),
    col("fields")[7].alias("info"),
    col("fields")[8].alias("format"),
    col("fields")[9].alias("P001"),
    col("fields")[10].alias("P002"),
    col("fields")[11].alias("P003"),
    col("fields")[12].alias("P004")
)

display(vcf_parsed_df)

# COMMAND ----------

# Convert patient genotype columns into one row per patient

genomic_long_df = vcf_parsed_df.selectExpr(
    "chrom",
    "pos",
    "ref",
    "alt",
    "info",
    """
    stack(
        4,
        'P001', P001,
        'P002', P002,
        'P003', P003,
        'P004', P004
    ) as (patient_id, genotype)
    """
)

display(genomic_long_df)

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, when

# Extract gene annotation, create person_id, cast position,
# and derive simple carrier status

genomic_df = (
    genomic_long_df
    .withColumn(
        "gene",
        regexp_extract(col("info"), r"GENE=([^;]+)", 1)
    )
    .withColumn(
        "pos",
        col("pos").cast("integer")
    )
    .withColumn(
        "person_id",
        regexp_extract(col("patient_id"), r"(\d+)", 1).cast("integer")
    )
    .withColumn(
        "variant_carrier",
        when(col("genotype") == "0/0", 0)
        .when(col("genotype").isNull(), None)
        .otherwise(1)
    )
    .select(
        "person_id",
        "patient_id",
        "chrom",
        "pos",
        "ref",
        "alt",
        "gene",
        "genotype",
        "variant_carrier"
    )
)

display(genomic_df)

# COMMAND ----------

# Persist the analysis-ready genomic table

genomic_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("genomic_variants")

# COMMAND ----------

# MAGIC %sql
# MAGIC --verify
# MAGIC
# MAGIC SELECT *
# MAGIC FROM genomic_variants
# MAGIC ORDER BY person_id;

# COMMAND ----------

# MAGIC %md