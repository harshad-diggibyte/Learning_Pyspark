import pyspark

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practise').getOrCreate()

spark

import pandas as pd
df_pyspark=spark.read.csv('sample.csv')

spark.read.option('header','true').csv('sample.csv').show()

##check column and Indexing
df_pyspark.columns

#WILL SHOW YOU FIRST ROW
df_pyspark.head()

#WILL SHOW SPECIFIC COLUMN - "Column_Name" is a e.g.
df_pyspark.select('Column_Name').show()

#WILL SHOW type of COLUMN - "Column_Name" is a e.g.
type(df_pyspark.select('Column_Name'))

#WILL SHOW what is COLUMN - "Column_Name" is a e.g.
df_pyspark['Column_Name']
