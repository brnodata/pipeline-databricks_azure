// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC ## Lendo arquivo delta da camada bronze

// COMMAND ----------

val url = "/mnt/dados/bronze/dataset_imoveis/"
val df = spark.read.format("delta").load(url)


// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # lendo o campo"Anuncio" que está como tipo objeto e separando cada campo em uma coluna

// COMMAND ----------

display(df.select("anuncio.*"))

// COMMAND ----------

display(
  df.select("anuncio.*","anuncio.endereco.*")
  )

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ## Salvando os campos separados em um novo Dataframe

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*","anuncio.endereco.*") 

// COMMAND ----------

display(dados_detalhados)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ## Dropando colunas que não queremos que façam parte do dados que vão para camada Silver

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas","endereco")
display(df_silver)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ## Salvando o dataframe final na camada Silver em formato delta.

// COMMAND ----------

val url_save = "/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(url_save)

// COMMAND ----------


