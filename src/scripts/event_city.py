import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
import numpy as np

spark = SparkSession \
        .builder \
        .master("yarn") \
        .config("spark.driver.cores", "3") \
        .config("spark.driver.memory", "3g") \
        .appName("Project 7") \
        .getOrCreate()



# Функция для вычисления расстояния между двумя координатами
def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Радиус Земли в километрах
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2) * np.sin(dlat/2) + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2) * np.sin(dlon/2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    distance = R * c
    return distance