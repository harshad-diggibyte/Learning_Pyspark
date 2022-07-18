"""
Pyspark Dataframes
Filter Operation
&,|,==    [and or equal to]
~     [Inverse filter operation]
"""

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('dataframe').getOrCreate()
df_pyspark=spark.read.csv('sample.csv',header=True,inferSchema=True)
df_pyspark.show()

#for retrieving records filter operations are useful! there are diffrent ways are shown here!
#Age of the people more than or equal to 35
df_pyspark.filter("Age>=35").show()

df_pyspark.filter("Age>=35").select(['Gender','Country','State']).show()

#Another way
df_pyspark.filter(df_pyspark['Age']>=35).show()

df_pyspark.filter((df_pyspark['Age']>=35) & (df_pyspark['Age']<=40)).show()

df_pyspark.filter((df_pyspark['Age']>=35) | (df_pyspark['Age']<=40)).show()

df_pyspark.filter(~(df_pyspark['Age']<=35)).show()
