// Databricks notebook source
val url = "/mnt/dados/bronze/dataset_imoveis/"
val df = spark.read.format("delta").load(url)


// COMMAND ----------

display(df)

// COMMAND ----------

display(df.select("anuncio.*"))

// COMMAND ----------

display(
  df.select("anuncio.*","anuncio.endereco.*")
  )

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*","anuncio.endereco.*") 

// COMMAND ----------

display(dados_detalhados)

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas","endereco")
display(df_silver)

// COMMAND ----------

val url_save = "/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(url_save)

// COMMAND ----------


