import sys
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import datetime
from pyspark.sql.types import FloatType, IntegerType, ArrayType, StringType

def change_dec_sep(df, column):
    df = df.withColumn(column, F.regexp_replace(column, ',', '.'))
    df = df.withColumn(column, df[column].cast("float"))
    return df

def main():
   # Входные параметры
    date = sys.argv[1]
    path_to_geo_events = sys.argv[2]
    path_to_geo_city = sys.argv[3]
    output_base_path = sys.argv[4]
 
    # Создаем подключение
    conf = SparkConf().setAppName(f"Project-sp7")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    # Читаем фрейм с координатами городов
    geo_df = sql.read.csv(path_to_geo_city,
                       sep=';', 
                       header=True,
                       inferSchema=True)
    
    # Приводим lat, lng к типу float, предварительно изменяя ',' на '.'
    geo_df = change_dec_sep(geo_df, 'lat')
    geo_df = change_dec_sep(geo_df, 'lng')

    # Читаем фрейм с событиями и их координатами
    events_df = sql.read.parquet(f'{path_to_geo_events}/date={date}')
    
    events_df = events_df.select(F.col('event'), F.col('event_type'), F.col('event.user').alias('user_id'), 'lat', 'lon').where(F.col('lat').isNotNull() & F.col('lon').isNotNull())

    df = events_df.crossJoin(geo_df.select(F.col('id').alias('zone_id'), 
                                        F.col('city'), 
                                        F.col('lat').alias('lat_city'),
                                        F.col('lng').alias('lon_city'),
                                        F.col('timezone')))

    df = df.withColumn("distance", F.lit(2) * F.lit(6371) 
                    * F.asin(F.sqrt(F.pow(F.sin((F.radians(F.col('lat')) - F.radians(F.col('lat_city')))/F.lit(2)), 2) 
                                    + F.cos(F.radians(F.col('lat'))) * F.cos(F.radians(F.col('lat_city'))) 
                                    * F.pow(F.sin((F.radians(F.col('lon')) - F.radians(F.col('lon_city')))/F.lit(2)), 2))))

    df = df.join(df.select('event', 'distance').groupBy('event').agg(F.min('distance')), 'event')

    df = df.where(F.col('distance') == F.col('min(distance)'))

    # Найдем пользователей, которые не переписывались друг с другом
    users_interacted = df.select("event.message_from", "event.message_to")
    users_interacted = users_interacted.filter(users_interacted.message_from.isNotNull() & users_interacted.message_to.isNotNull())
    users_interacted = users_interacted.withColumn("user_pair", F.array(users_interacted.message_from, users_interacted.message_to))
    users_interacted = users_interacted.groupBy("user_pair").agg(F.count("*").alias("interaction_count"))
    users_interacted = users_interacted.filter(users_interacted.interaction_count < 1)
    users_interacted = users_interacted.withColumn("user_left", users_interacted.user_pair[0].cast("string")).withColumn("user_right", users_interacted.user_pair[1].cast("string"))
    users_interacted = users_interacted.select('user_left', 'user_right')

    # Найдем пользователей, которые состоят в одном канале
    user_sub = df.select('event.subscription_channel', 'event.user').filter(df.event.subscription_channel.isNotNull() &
                                                                        df.event.user.isNotNull())

    users_recommendation = users_interacted.join(user_sub, users_interacted.user_left == user_sub.user, "left") \
                .withColumn("ul_channel", F.col("subscription_channel")) \
                .drop("subscription_channel", "user") \
                .join(user_sub, users_interacted.user_right == user_sub.user, "left") \
                .withColumnRenamed("subscription_channel", "ur_channel") \
                .drop("user")


    users_recommendation = users_recommendation.select('user_left', 'user_right').filter(users_recommendation.ul_channel == users_recommendation.ur_channel)

    # Найдем пользователей, которые находятся на расстоянии 1 км друг от друга

    user_geo = df.select('event.user', 'lat', 'lon')

    joined_df = users_recommendation.join(user_geo.select('user', F.col('lat').alias('ul_lat'), F.col('lon').alias('ul_lon')), 
                                        users_recommendation.user_left == user_geo.user, "left") \
                                    .join(user_geo.select('user', F.col('lat').alias('ur_lat'), F.col('lon').alias('ur_lon')), 
                                        users_recommendation.user_right == user_geo.user, "left")

    joined_df = joined_df.withColumn("distance", F.lit(2) * F.lit(6371) 
                                    * F.asin(F.sqrt(F.pow(F.sin((F.radians(F.col('ul_lat')) - F.radians(F.col('ur_lat')))/F.lit(2)), 2) 
                                        + F.cos(F.radians(F.col('ul_lat'))) * F.cos(F.radians(F.col('ur_lat'))) 
                                        * F.pow(F.sin((F.radians(F.col('ul_lon')) - F.radians(F.col('ur_lon')))/F.lit(2)), 2))))

    users_recommendation = joined_df.select('user_left', 'user_right').filter(F.col('distance') <= 1)

    # Добавление атрибутов витрины
    users_recommendation = users_recommendation.withColumn("processed_dttm", F.current_timestamp())

    user_zone = df.select('event.user',  'zone_id') 
    users_recommendation = users_recommendation.join(user_zone, F.col("user_left") == F.col("user"), "left") \
                                            .drop('user')


    local_time_df = df.select('zone_id', 'timezone', F.date_format('event.datetime', "HH:mm:ss").alias('time_utc'))
    users_recommendation = users_recommendation.join(local_time_df, users_recommendation.zone_id == local_time_df.zone_id)
    users_recommendation = users_recommendation.withColumn("local_time", F.from_utc_timestamp(F.col("time_utc"), F.col("timezone"))) \
                                            .drop('local_time_df.zone_id', 'timezone', 'time_utc')

    # Вывод результата
    users_recommendation.write.mode("overwrite").parquet(f"{output_base_path}/date={date}")

if __name__ == "__main__":
    main()


# !/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/friends_datamart.py 2022-05-25 /user/master/data/geo/events /user/saxarr0k/data/geo/geo_2.csv /user/saxarr0k/analytics/friends_datamart

