#Importing pyspark Lib.
import pyspark

#Making spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practise').getOrCreate()

#About Spark
spark
