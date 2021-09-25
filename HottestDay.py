import pandas as pd
import findspark
findspark.init()
findspark.find()
import pyspark
findspark.find()
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf 
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StructType
from pyspark.sql.functions import *


conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
sql_sc = SQLContext(sc)     
pdf = pd.read_csv ('C://Users//Harsha//Documents//Testing//Input//*.csv') 
#Defining Schema String
schemaString='f1 f2 Date f4 f5 f6 f7 f8 f9 f10 f11 f12 f13 Region Country'
#Defining Schema type
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

df = spark.createDataFrame(pdf, schema)

#Modifying Schema type for compuational purpose
df0 = df.withColumn('Temperature',df['f8'].cast(IntegerType())).withColumn('WindSpeed', df['f5'].cast(IntegerType()))

#Selecting hot temperature based on Maximum Screen Temerature and Minimun Wind Speed
df_region = df0.groupby('Region','Country','Date').agg(max('Temperature').alias('Temperature'), min('WindSpeed').alias('WindSpeed')).orderBy(col('Temperature').desc())
df_exact= df_region.filter(df_region.WindSpeed > 0).collect()[0]
#Writing the output in parquet file
df_exact.wirte.parquet("C://Users//Harsha//Documents//Testing//Input//result.parquet")

sc.stop()
