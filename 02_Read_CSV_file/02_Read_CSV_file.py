import pyspark

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practise').getOrCreate()

spark

#Read the CSV File - for that we have to import Pandas file
import pandas as pd
pd.read_csv('sample.csv')



