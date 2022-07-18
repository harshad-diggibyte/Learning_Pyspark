#Pyspark GroupBy And Aggregate Functions

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Agg').getOrCreate()
spark

df_pyspark=spark.read.csv('sample.csv',header=True,inferSchema=True)
df_pyspark.show()

df_pyspark.printSchema()

# Groupby and aggregate functions work together
# Grouped to find the maximum age

df_pyspark.groupBy('Age').sum().show()

df_pyspark.groupBy('Age').avg().show()

# Groupby country  which gives maximum age
df_pyspark.groupBy('Country').sum().show()

df_pyspark.groupBy('Country').mean().show()

df_pyspark.groupBy('Country').count().show()

df_pyspark.agg({'Age':'sum'}).show()

