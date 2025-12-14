"""
Скрипт для создания витрины данных в разрезе пользователей.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UserMartBuilder:
    
    EARTH_RADIUS_KM = 6371.0
    
    def __init__(self, spark):
        self.spark = spark
    
    def load_events(self, events_path):
        events_df = self.spark.read.parquet(events_path)
        logger.info(f"Загружено {events_df.count()} событий")
        return events_df
    
    def load_cities(self, cities_path):
        cities_df = self.spark.read.csv(cities_path, header=True, inferSchema=True)
        logger.info(f"Загружено {cities_df.count()} городов")
        return cities_df
    
    def calculate_distance(self, lat1, lon1, lat2, lon2):
        # Формула гаверсинуса для вычисления расстояния на сфере
        lat1_rad = F.radians(lat1)
        lat2_rad = F.radians(lat2)
        delta_lat = F.radians(lat2 - lat1)
        delta_lon = F.radians(lon2 - lon1)
        
        a = (
            F.pow(F.sin(delta_lat / 2), 2) +
            F.cos(lat1_rad) * F.cos(lat2_rad) * F.pow(F.sin(delta_lon / 2), 2)
        )
        c = 2 * F.asin(F.sqrt(a))
        distance = self.EARTH_RADIUS_KM * c
        
        return distance
    
    def enrich_events_with_cities(self, events_df, cities_df):
        
        # Подготовка данных о городах
        cities_prep = cities_df.select(
            F.col("id").alias("city_id"),
            F.col("city").alias("city_name"),
            F.col("lat").alias("city_lat"),
            F.col("lng").alias("city_lon"),
            F.col("timezone")
        )
        
        # Фильтруем события с координатами (исходящие сообщения)
        events_with_coords = events_df.filter(
            F.col("lat").isNotNull() & F.col("lon").isNotNull()
        )
        
        # Создаем декартово произведение для вычисления расстояний
        events_cross_cities = events_with_coords.crossJoin(cities_prep)
        
        # Вычисляем расстояние от координат события до центра города
        events_cross_cities = events_cross_cities.withColumn(
            "distance",
            self.calculate_distance(
                F.col("lat"),
                F.col("lon"),
                F.col("city_lat"),
                F.col("city_lon")
            )
        )
        
        # Находим ближайший город для каждого события
        window_spec = Window.partitionBy("event.message_id").orderBy("distance")
        events_with_city = events_cross_cities.withColumn(
            "row_num",
            F.row_number().over(window_spec)
        ).filter(
            F.col("row_num") == 1
        ).select(
            F.col("event.message_id").alias("message_id"),
            F.col("event.message_from").alias("user_id"),
            F.col("event.message_to").alias("message_to"),
            F.col("event.message_ts").alias("message_ts"),
            F.col("event.event_type").alias("event_type"),
            F.col("lat"),
            F.col("lon"),
            F.col("city_id").alias("zone_id"),
            F.col("city_name").alias("city"),
            F.col("timezone")
        )
        
        # Для событий без координат получаем последние координаты пользователя
        events_without_coords = events_df.filter(
            F.col("lat").isNull() | F.col("lon").isNull()
        )
        
        # Получаем последние координаты для каждого пользователя из событий с координатами
        last_coords = events_with_coords.groupBy("event.message_from").agg(
            F.max("event.message_ts").alias("max_ts")
        ).join(
            events_with_coords,
            on=[
                F.col("event.message_from") == F.col("event.message_from"),
                F.col("max_ts") == F.col("event.message_ts")
            ],
            how="left"
        ).select(
            F.col("event.message_from").alias("user_id"),
            F.col("lat").alias("user_lat"),
            F.col("lon").alias("user_lon")
        )
        
        # Присваиваем координаты событиям без координат
        events_without_coords_enriched = events_without_coords.join(
            last_coords,
            on=F.col("event.message_from") == F.col("user_id"),
            how="left"
        ).withColumn(
            "lat",
            F.coalesce(F.col("lat"), F.col("user_lat"))
        ).withColumn(
            "lon",
            F.coalesce(F.col("lon"), F.col("user_lon"))
        ).select(
            F.col("event.message_id").alias("message_id"),
            F.col("event.message_from").alias("user_id"),
            F.col("event.message_to").alias("message_to"),
            F.col("event.message_ts").alias("message_ts"),
            F.col("event.event_type").alias("event_type"),
            F.col("lat"),
            F.col("lon")
        )
        
        # Определяем города для событий без координат (используя присвоенные координаты)
        events_without_coords_cross_cities = events_without_coords_enriched.filter(
            F.col("lat").isNotNull() & F.col("lon").isNotNull()
        ).crossJoin(cities_prep).withColumn(
            "distance",
            self.calculate_distance(
                F.col("lat"),
                F.col("lon"),
                F.col("city_lat"),
                F.col("city_lon")
            )
        )
        
        window_spec2 = Window.partitionBy("message_id").orderBy("distance")
        events_without_coords_with_city = events_without_coords_cross_cities.withColumn(
            "row_num",
            F.row_number().over(window_spec2)
        ).filter(
            F.col("row_num") == 1
        ).select(
            F.col("message_id"),
            F.col("user_id"),
            F.col("message_to"),
            F.col("message_ts"),
            F.col("event_type"),
            F.col("lat"),
            F.col("lon"),
            F.col("city_id").alias("zone_id"),
            F.col("city_name").alias("city"),
            F.col("timezone")
        )
        
        # Объединяем все события
        all_events_enriched = events_with_city.union(events_without_coords_with_city)
        
        logger.info(f"Обогащено {all_events_enriched.count()} событий")
        return all_events_enriched
    
    def calculate_act_city_and_local_time(self, events_enriched_df):
        # Используем оконную функцию для получения последнего сообщения без джойна
        window_spec = Window.partitionBy("user_id").orderBy(F.desc("message_ts"))
        last_events = events_enriched_df.filter(
            F.col("event_type") == "message"
        ).withColumn(
            "row_num",
            F.row_number().over(window_spec)
        ).filter(
            F.col("row_num") == 1
        ).select(
            F.col("user_id"),
            F.col("city").alias("act_city"),
            F.col("message_ts"),
            F.col("timezone")
        )
        
        # Вычисляем локальное время
        act_city_and_time_df = last_events.withColumn(
            "time_utc",
            F.from_unixtime(F.col("message_ts"))
        ).withColumn(
            "local_time",
            F.from_utc_timestamp(
                F.col("time_utc"),
                F.coalesce(F.col("timezone"), F.lit("UTC"))
            )
        ).select(
            "user_id",
            "act_city",
            "local_time"
        )
        
        return act_city_and_time_df
    
    def calculate_home_city(self, events_enriched_df):
        # Находим город, где пользователь был дольше 27 дней подряд
        events_with_date = events_enriched_df.filter(
            F.col("event_type") == "message"
        ).withColumn(
            "date",
            F.to_date(F.from_unixtime(F.col("message_ts")))
        ).select(
            "user_id",
            "city",
            "date",
            "message_ts"
        ).distinct()
        
        user_city_periods = events_with_date.groupBy("user_id", "city").agg(
            F.min("date").alias("period_start"),
            F.max("date").alias("period_end"),
            F.countDistinct("date").alias("days_count")
        ).withColumn(
            "period_duration",
            F.datediff("period_end", "period_start") + 1
        )
        
        user_city_periods_valid = user_city_periods.join(
            events_with_date,
            on=[
                F.col("user_id") == F.col("user_id"),
                F.col("date") >= F.col("period_start"),
                F.col("date") <= F.col("period_end")
            ],
            how="left"
        ).groupBy(
            "user_id",
            "city",
            "period_start",
            "period_end",
            "period_duration"
        ).agg(
            F.countDistinct("city").alias("cities_in_period")
        ).filter(
            (F.col("cities_in_period") == 1) & (F.col("period_duration") >= 27)
        )
        
        # Для каждого пользователя выбираем последний такой период
        window_spec = Window.partitionBy("user_id").orderBy(F.desc("period_end"))
        home_cities = user_city_periods_valid.withColumn(
            "row_num",
            F.row_number().over(window_spec)
        ).filter(
            F.col("row_num") == 1
        ).select(
            F.col("user_id"),
            F.col("city").alias("home_city")
        )
        
        return home_cities
    
    def calculate_travel_info(self, events_enriched_df):
        user_cities_ordered = events_enriched_df.filter(
            F.col("event_type") == "message"
        ).select(
            "user_id",
            "city",
            "message_ts"
        ).orderBy("user_id", "message_ts")
        
        # Используем оконную функцию для получения предыдущего города
        window_spec = Window.partitionBy("user_id").orderBy("message_ts")
        user_cities_with_prev = user_cities_ordered.withColumn(
            "prev_city",
            F.lag("city").over(window_spec)
        )
        
        # Фильтруем только те строки, где город отличается от предыдущего
        # (или это первая строка для пользователя, где prev_city is null)
        travel_cities = user_cities_with_prev.filter(
            (F.col("city") != F.col("prev_city")) | F.col("prev_city").isNull()
        )
        
        # Группируем и собираем города в список
        travel_info = travel_cities.groupBy("user_id").agg(
            F.collect_list("city").alias("travel_array")
        ).withColumn(
            "travel_count",
            F.size(F.col("travel_array"))
        ).select(
            "user_id",
            "travel_count",
            "travel_array"
        )
        
        return travel_info
    
    
    def build_mart(self, events_path, cities_path, output_path):
        events_df = self.load_events(events_path)
        cities_df = self.load_cities(cities_path)
        
        events_enriched_df = self.enrich_events_with_cities(events_df, cities_df)
        
        act_city_and_time_df = self.calculate_act_city_and_local_time(events_enriched_df)
        home_city_df = self.calculate_home_city(events_enriched_df)
        travel_info_df = self.calculate_travel_info(events_enriched_df)
        
        user_mart = act_city_and_time_df.join(
            home_city_df,
            on="user_id",
            how="outer"
        ).join(
            travel_info_df,
            on="user_id",
            how="outer"
        )
        
        user_mart.write.mode("overwrite").parquet(output_path)
        return user_mart


def main():
    events_path = os.getenv("EVENTS_PATH", "/user/master/data/geo/events")
    cities_path = os.getenv("CITIES_PATH", "/user/master/data/geo/cities")
    output_path = os.getenv("OUTPUT_PATH", "/user/master/data/geo/user_mart")
    
    if len(sys.argv) > 1:
        events_path = sys.argv[1]
    if len(sys.argv) > 2:
        cities_path = sys.argv[2]
    if len(sys.argv) > 3:
        output_path = sys.argv[3]
    
    spark = SparkSession.builder.appName("UserMartBuilder").getOrCreate()
    
    try:
        builder = UserMartBuilder(spark)
        builder.build_mart(events_path, cities_path, output_path)
    except Exception as e:
        logger.error(f"Ошибка: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
