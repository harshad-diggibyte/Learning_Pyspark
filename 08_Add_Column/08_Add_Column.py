import pyspark

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practise').getOrCreate()

spark

import pandas as pd
df_pyspark=spark.read.csv('sample.csv')

spark.read.option('header','true').csv('sample.csv').show()

#ADD NEW COLUMN IN PySpark DataFrame
#For e.g. we are creating new column 'Experience after 2 year', so 'existing_column_name' is "Experience" and +2 is value will be added for each row in experience.

df_pyspark.withcolumn('New_column_name',df_pyspark['existing_column_name']+2)

df_pyspark.withcolumn('New_column_name',df_pyspark['existing_column_name']+2).show()

#ALSO YOU CAN ASSIGN
df_pyspark = df_pyspark.withcolumn('New_column_name',df_pyspark['existing_column_name']+2).show()