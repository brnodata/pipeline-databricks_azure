// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC ##Verifica camada inbound

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/inbound")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Lê arquivo json

// COMMAND ----------

val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados = spark.read.json(path)

// COMMAND ----------

display(dados)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ## Dropa colunas que não são necessarias

// COMMAND ----------

val dados_anuncio = dados.drop("imagens","usuario")
display(dados_anuncio)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ## Importa método apache para criar um coluna

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ## Cria coluna utilizando método col

// COMMAND ----------

val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Salva arquivo com a primeira transformação na camada bronze

// COMMAND ----------

val path = "/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------


