import pyspark

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practise').getOrCreate()

spark

import pandas as pd
df_pyspark=spark.read.csv('sample.csv')

spark.read.option('header','true').csv('sample.csv').show()

#type
type(df_pyspark)