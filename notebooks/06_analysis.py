# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Clinico-Genomic Analysis
# MAGIC
# MAGIC This notebook performs a simple descriptive analysis of the synthetic clinico-genomic cohort.
# MAGIC
# MAGIC Research question:
# MAGIC Do carriers of the synthetic LDLR variant have higher LDL cholesterol than non-carriers?
# MAGIC
# MAGIC Because the dataset is synthetic and contains only four people, results are illustrative only and should not be interpreted as scientific evidence.

# COMMAND ----------

# Load the final analysis-ready cohort

cohort_df = spark.table("clinico_genomic_cohort")

display(cohort_df)

# COMMAND ----------

# Compare LDL levels and hyperlipidemia prevalence by carrier status

from pyspark.sql.functions import avg, count

summary_df = (
    cohort_df
    .groupBy("variant_carrier")
    .agg(
        count("*").alias("n"),
        avg("ldl_mg_dl").alias("avg_ldl_mg_dl"),
        avg("hyperlipidemia").alias("hyperlipidemia_rate")
    )
)

display(summary_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC --verifying same thing in sql
# MAGIC
# MAGIC SELECT
# MAGIC     variant_carrier,
# MAGIC     COUNT(*) AS n,
# MAGIC     AVG(ldl_mg_dl) AS avg_ldl_mg_dl,
# MAGIC     AVG(hyperlipidemia) AS hyperlipidemia_rate
# MAGIC FROM clinico_genomic_cohort
# MAGIC GROUP BY variant_carrier
# MAGIC ORDER BY variant_carrier;

# COMMAND ----------

# Prepare the predictor for Spark's regression model

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=["variant_carrier"],
    outputCol="features"
)

model_df = assembler.transform(
    cohort_df
).select(
    "features",
    "ldl_mg_dl"
)

display(model_df)

# COMMAND ----------

# Fit a simple linear regression:
# LDL cholesterol ~ variant carrier status

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(
    featuresCol="features",
    labelCol="ldl_mg_dl"
)

lr_model = lr.fit(model_df)

print("Intercept:", lr_model.intercept)
print("Carrier coefficient:", lr_model.coefficients[0])

# COMMAND ----------

# MAGIC %md
# MAGIC In this synthetic dataset, non-carriers average about 117.5 mg/dL LDL, and carriers average about 69 mg/dL higher.