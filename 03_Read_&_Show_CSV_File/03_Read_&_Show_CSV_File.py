import pyspark

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practise').getOrCreate()

spark

#Read & show the CSV File - for that we have to import Pandas file
import pandas as pd
df_pyspark=spark.read.csv('sample.csv')

spark.read.option('header','true').csv('sample.csv').show()

