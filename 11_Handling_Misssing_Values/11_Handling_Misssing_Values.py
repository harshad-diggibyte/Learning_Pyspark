# Pyspark DataFrame - Handling Misssing Values
""" Dropping Columns
Dropping Rows
Various Parameter In Dropping Functionlities
Handling Missing Values by MEAN, MEDIAN, MODE """

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Practise').getOrCreate()

df_pyspark=spark.read.csv('sample.csv',header=True,inferSchema=True)

df_pyspark.printSchema()

df_pyspark.show()

#drop the columns
df_pyspark.drop('Column_Name').show()

df_pyspark.show()

#Dropping Nan Values - it will delete all null values
df_pyspark.na.drop().show()

"""
df_pyspark.na.drop (how='any', thresh=None, subset=None)
how: str, optional
	'any' or 'all'.
	If 'any', drop a row if it contains any nulls.
	If 'all', drop a row only if all its values are null.

thresh: int, optional
	default None
	If specified, drop rows that have less than thresh` non-null values.
	This overwrites the 'how parameter.

subset : str, tuple or list, optional
	optional list of column names to consider.

"""

# any==how
df_pyspark.na.drop(how="any").show()

#threshold - if there is 3 nun values in the roe keep this, otherwise delete it!
df_pyspark.na.drop(how="any",thresh=3).show()

#Subset - delete nun values from specific column
df_pyspark.na.drop(how="any",subset=['Column_Name']).show()


# Filling the Missing Value
# df_pyspark.na.fill(value, subset=None)
df_pyspark.na.fill('Missing Values',['Column_Name','Another_Column_Name']).show()

df_pyspark.show()


df_pyspark.printSchema()

#Imputer Function
#We can set strategy as MEAN MEDIAN MODE
from pyspark.ml.feature import Imputer

imputer = Imputer(
    inputCols=['Column_Name1', 'Column_Name2', 'Column_Name3'],
    outputCols=["{}_imputed".format(c) for c in ['Column_Name1', 'Column_Name2', 'Column_Name3']]
    ).setStrategy("median")

# Add imputation cols to df
imputer.fit(df_pyspark).transform(df_pyspark).show()

