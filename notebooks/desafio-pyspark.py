# Databricks notebook source
df = spark.read.json("dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json")

# COMMAND ----------

import pyspark.pandas as ps

# COMMAND ----------

df = df.toPandas()
df.head()

# COMMAND ----------

df = df.drop(columns=["imagens", "usuario"])

# COMMAND ----------

df["id"] = df["anuncio"][0]["id"]

# COMMAND ----------

df_bronze = df

# COMMAND ----------

df_silver = spark.createDataFrame(df_bronze)

# COMMAND ----------

display(df_silver.select("anuncio.*", "anuncio.endereco.*"))

# COMMAND ----------

df_silver = df_silver.select("anuncio.*", "anuncio.endereco.*")

# COMMAND ----------

df_silver = df_silver.drop("endereco", "caracteristicas")

# COMMAND ----------

display(df_silver)

# COMMAND ----------


