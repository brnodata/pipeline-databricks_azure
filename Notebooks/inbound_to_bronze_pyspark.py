# Databricks notebook source
# MAGIC %md
# MAGIC ##Lendo os dados na camada inbound:

# COMMAND ----------

url = '/mnt/dados/inbound/dados_brutos_imoveis.json'
url_dest = '/mnt/dados/bronze/dataset_imoveis_python'
df = spark.read.json(url)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Removendo as colunas:

# COMMAND ----------

df = df.drop("imagens","usuario")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criando a coluna de identificação:

# COMMAND ----------

from pyspark.sql.functions import col

df = df.withColumn('id', col('anuncio.id'))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Salvando na camada bronze:

# COMMAND ----------

df.write.format('delta').mode('overwrite').save(url_dest)

# COMMAND ----------

display(dbutils.fs.ls(url_dest))

# COMMAND ----------


