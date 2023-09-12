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
    
    # Соединяем два датафрейма
    df = events_df.crossJoin(geo_df.select(F.col('id').alias('zone_id'), 
                                        F.col('city'), 
                                        F.col('lat').alias('lat_city'),
                                        F.col('lng').alias('lon_city'),
                                        F.col('timezone')))
    
    # Считаем дистанцию между координатами отправленного сообщения и координатами города
    df = df.withColumn("distance", 
                                     F.lit(2) * F.lit(6371) 
                                     * F.asin(F.sqrt(F.pow(F.sin((F.radians(F.col('lat')) - F.radians(F.col('lat_city')))/F.lit(2)), 2) 
                                                     + F.cos(F.radians(F.col('lat'))) 
                                                     * F.cos(F.radians(F.col('lat_city'))) 
                                                     * F.pow(F.sin((F.radians(F.col('lon')) - F.radians(F.col('lon_city')))/F.lit(2)), 2))))

    df = df.select('event', 'event_type', 'zone_id', 'city', 'distance')
    df = df.join(df.select('event', 'distance').groupBy('event').agg(F.min('distance')), 'event')
    df = df.filter((F.col('distance') == F.col('min(distance)')) & (F.col('event.user').isNotNull())) 

    # Определение актуального адреса (города) для каждого пользователя
    windowSpec = Window.partitionBy("event.user").orderBy(F.desc("event.datetime"))
    df = df.withColumn("row_number", F.row_number().over(windowSpec))
    act_city_df = df.filter(df.row_number == 1).select(F.col('event.user').alias('user_id'), 'city')
    df = df.drop("row_number")

    # Определение домашнего адреса (города) для каждого пользователя
    window_spec = Window.partitionBy('event.user').orderBy(F.col('event.datetime').desc())
    df = df.withColumn('prev_city', F.lag('city').over(window_spec))
    df = df.filter(df['city'] != df['prev_city'])
    df = df.withColumn('prev_date', F.lag('event.datetime').over(window_spec))
    df = df.withColumn('days_since_prev_change', F.datediff(F.col('event.datetime'), F.col('prev_date')))
    df_filtered = df.filter(df['days_since_prev_change'] >= 27)
    df_home_city = df_filtered.withColumn('rank', F.row_number().over(window_spec)).filter(F.col('rank') == 1).select(F.col('event.user').alias('user_id'), F.col('city').alias('home_city'))

    # Создание витрины пользователей
    users_vitrina_df = act_city_df.join(df_home_city, "user_id", "left")

    # Вычисление количества посещенных городов и списка городов в порядке посещения для каждого пользователя
    window_spec2 = Window.partitionBy('event.user').orderBy('event.datetime')
    df_with_window = df.withColumn('lag_city', F.lag('city').over(window_spec2))
    df_with_window = df_with_window.withColumn('lead_city', F.lead('city').over(window_spec2))
    df_border_cities = df_with_window.filter((df_with_window['lag_city'] != df_with_window['city']) | (df_with_window['lead_city'] != df_with_window['city']))
    df_travel_count = df_border_cities.groupBy(F.col('event.user').alias('user_id')).agg(F.count('*').alias('travel_count'))
    df_travel_array = df_border_cities.groupBy(F.col('event.user').alias('user_id')).agg(F.collect_list('city').alias('travel_array'))

    # Присоединяем столбцы df_travel_array и df_travel_count к итоговой витрине
    users_vitrina_df = users_vitrina_df.join(df_travel_count, 'user_id', 'left') \
                                   .join(df_travel_array, 'user_id', 'left')
    
    # Вычисление местного времени для каждого пользователя
    local_time_df = act_city_df.join(events_df.select(F.col('event.user').alias('user_id'), 
                                                  F.date_format('event.datetime', "HH:mm:ss")\
                                                  .alias('time_utc')), 'user_id', "left")
    local_time_df = local_time_df.join(geo_df, local_time_df['act_city'] == geo_df['city'], "left")
    local_time_df = local_time_df.withColumn("local_time", F.from_utc_timestamp(F.col("time_utc"), F.col("timezone")))
    local_time_df = local_time_df.select('user_id', 'local_time')

    # Добавим местное время в итоговую витрину
    users_vitrina_df = users_vitrina_df.join(local_time_df, 'user_id', 'left')

    # Вывод результата
    users_vitrina_df.write.mode("overwrite").parquet(f"{output_base_path}/date={date}")

if __name__ == "__main__":
    main()

# !/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/event_datamart.py 2022-05-25 /user/master/data/geo/events /user/saxarr0k/data/geo/geo_2.csv /user/saxarr0k/analytics/user_datamart
