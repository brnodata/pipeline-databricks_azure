# Databricks notebook source
# MAGIC %md
# MAGIC ##Lendo os dados na camada bronze:

# COMMAND ----------

url = '/mnt/dados/bronze/dataset_imoveis_python'
url_dest = '/mnt/dados/silver/dataset_imoveis_python'
df = spark.read.format('delta').load(url)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Transformando os campos do json em colunas:

# COMMAND ----------

df =df.select("anuncio.*")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Removendo colunas:

# COMMAND ----------

df = df.drop("endereco","caracteristicas")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Salvando na camada silver:

# COMMAND ----------

df.write.format('delta').mode('overwrite').save(url_dest)

# COMMAND ----------

display(dbutils.fs.ls(url_dest))

# COMMAND ----------


