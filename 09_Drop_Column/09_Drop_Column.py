import pyspark

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practise').getOrCreate()

spark

import pandas as pd
df_pyspark=spark.read.csv('sample.csv')

spark.read.option('header','true').csv('sample.csv').show()

#DROP COLUMN IN PySpark DataFrame
df_pyspark.drop('Column_Name')

df_pyspark.drop('Column_Name').show()
