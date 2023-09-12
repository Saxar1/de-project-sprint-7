import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType

spark = (SparkSession
        .builder
        .master('yarn')
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", 2)
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.executor.instances", "1")
        .config("spark.dynamicAllocation.minExecutors", "1")
        .config("spark.dynamicAllocation.maxExecutors", "5")
        .appName("Project7") 
        .getOrCreate())

base_path = "/user/master/data/geo/events/date=2022-05-20"

def change_dec_sep(df, column):
    df = df.withColumn(column, F.regexp_replace(column, ',', '.'))
    df = df.withColumn(column, df[column].cast("float"))
    return df

# Загрузка данных из файла geo.csv
geo_df = spark.read.csv("/user/saxarr0k/data/geo/geo_2.csv",
                        sep=';',
                        header=True, 
                        inferSchema=True)

geo_df = change_dec_sep(geo_df, 'lat')
geo_df = change_dec_sep(geo_df, 'lng')

# Входные данные
events_df = spark.read.parquet(base_path)

