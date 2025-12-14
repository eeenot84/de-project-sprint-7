"""
Скрипт для создания витрины данных для рекомендации друзей.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FriendsRecommendationMartBuilder:
    
    EARTH_RADIUS_KM = 6371.0
    MAX_DISTANCE_KM = 1.0
    
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
            # Преобразование градусов в радианы
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
    
    def get_user_locations(self, events_df):
        user_locations = events_df.filter(
            F.col("lat").isNotNull() & F.col("lon").isNotNull()
        ).groupBy("event.message_from").agg(
            F.max("event.message_ts").alias("max_ts")
        ).join(
            events_df,
            on=[
                F.col("event.message_from") == F.col("event.message_from"),
                F.col("max_ts") == F.col("event.message_ts")
            ],
            how="left"
        ).select(
            F.col("event.message_from").alias("user_id"),
            F.col("lat"),
            F.col("lon")
        ).dropDuplicates(["user_id"])
        
        return user_locations
    
    def get_common_channels(self, events_df):
        subscriptions = events_df.filter(
            F.col("event.event_type") == "subscription"
        ).select(
            F.col("event.message_from").alias("user_id"),
            F.col("event.message_to").alias("channel_id")
        ).distinct()
        
        common_channels = subscriptions.alias("s1").join(
            subscriptions.alias("s2"),
            on=F.col("s1.channel_id") == F.col("s2.channel_id"),
            how="inner"
        ).filter(
            F.col("s1.user_id") < F.col("s2.user_id")
        ).select(
            F.col("s1.user_id").alias("user_left"),
            F.col("s2.user_id").alias("user_right"),
            F.col("s1.channel_id").alias("channel_id")
        ).distinct()
        
        return common_channels
    
    def filter_existing_conversations(self, common_channels_df, events_df):
        conversations = events_df.filter(
            F.col("event.event_type") == "message"
        ).select(
            F.when(
                F.col("event.message_from") < F.col("event.message_to"),
                F.col("event.message_from")
            ).otherwise(F.col("event.message_to")).alias("user1"),
            F.when(
                F.col("event.message_from") < F.col("event.message_to"),
                F.col("event.message_to")
            ).otherwise(F.col("event.message_from")).alias("user2")
        ).distinct()
        
        pairs_without_conversations = common_channels_df.join(
            conversations,
            on=[
                F.col("user_left") == F.col("user1"),
                F.col("user_right") == F.col("user2")
            ],
            how="left_anti"
        )
        
        return pairs_without_conversations
    
    def filter_by_distance(self, pairs_df, user_locations_df):
        pairs_with_locations = pairs_df.join(
            user_locations_df.alias("ul1"),
            on=F.col("user_left") == F.col("ul1.user_id"),
            how="inner"
        ).join(
            user_locations_df.alias("ul2"),
            on=F.col("user_right") == F.col("ul2.user_id"),
            how="inner"
        ).select(
            F.col("user_left"),
            F.col("user_right"),
            F.col("ul1.lat").alias("lat1"),
            F.col("ul1.lon").alias("lon1"),
            F.col("ul2.lat").alias("lat2"),
            F.col("ul2.lon").alias("lon2")
        )
        
        pairs_with_distance = pairs_with_locations.withColumn(
            "distance",
            self.calculate_distance(
                F.col("lat1"),
                F.col("lon1"),
                F.col("lat2"),
                F.col("lon2")
            )
        ).filter(
            F.col("distance") <= self.MAX_DISTANCE_KM
        )
        
        return pairs_with_distance
    
    def enrich_with_zone_and_time(self, pairs_df, events_df, cities_df):
        user_locations = self.get_user_locations(events_df)
        
        # Определяем зону для пары (берем зону первого пользователя)
        # Для этого нужно найти ближайший город к координатам пользователя
        cities_prep = cities_df.select(
            F.col("id").alias("city_id"),
            F.col("lat").alias("city_lat"),
            F.col("lng").alias("city_lon"),
            F.col("timezone")
        )
        
        pairs_with_zones = pairs_df.join(
            user_locations,
            on=F.col("user_left") == F.col("user_id"),
            how="left"
        ).crossJoin(cities_prep).withColumn(
            "distance",
            self.calculate_distance(
                F.col("lat"),
                F.col("lon"),
                F.col("city_lat"),
                F.col("city_lon")
            )
        )
        
        window_spec = Window.partitionBy("user_left", "user_right").orderBy("distance")
        pairs_with_zone = pairs_with_zones.withColumn(
            "row_num",
            F.row_number().over(window_spec)
        ).filter(
            F.col("row_num") == 1
        ).select(
            F.col("user_left"),
            F.col("user_right"),
            F.col("city_id").alias("zone_id"),
            F.col("timezone")
        )
        
        # Получаем последнее событие для определения локального времени
        last_events = events_df.groupBy("event.message_from").agg(
            F.max("event.message_ts").alias("last_message_ts")
        ).join(
            events_df,
            on=[
                F.col("event.message_from") == F.col("event.message_from"),
                F.col("last_message_ts") == F.col("event.message_ts")
            ],
            how="left"
        ).select(
            F.col("event.message_from").alias("user_id"),
            F.col("event.message_ts").alias("message_ts")
        ).dropDuplicates(["user_id"])
        
        # Присоединяем информацию о времени
        pairs_enriched = pairs_with_zone.join(
            last_events.alias("le1"),
            on=F.col("user_left") == F.col("le1.user_id"),
            how="left"
        ).join(
            last_events.alias("le2"),
            on=F.col("user_right") == F.col("le2.user_id"),
            how="left"
        ).withColumn(
            "last_ts",
            F.greatest(
                F.coalesce(F.col("le1.message_ts"), F.lit(0)),
                F.coalesce(F.col("le2.message_ts"), F.lit(0))
            )
        ).withColumn(
            "time_utc",
            F.from_unixtime(F.col("last_ts"))
        ).withColumn(
            "local_time",
            F.from_utc_timestamp(
                F.col("time_utc"),
                F.coalesce(F.col("timezone"), F.lit("UTC"))
            )
        ).select(
            F.col("user_left"),
            F.col("user_right"),
            F.col("zone_id"),
            F.col("local_time")
        )
        
        return pairs_enriched
    
    def build_mart(self, events_path, cities_path, output_path):
        events_df = self.load_events(events_path)
        cities_df = self.load_cities(cities_path)
        
        user_locations_df = self.get_user_locations(events_df)
        common_channels_df = self.get_common_channels(events_df)
        pairs_without_conversations = self.filter_existing_conversations(
            common_channels_df,
            events_df
        )
        
        # Фильтрация по расстоянию
        pairs_within_distance = self.filter_by_distance(
            pairs_without_conversations,
            user_locations_df
        )
        
        # Обогащение информацией о зоне и времени
        friends_recommendation_mart = self.enrich_with_zone_and_time(
            pairs_within_distance.select("user_left", "user_right"),
            events_df,
            cities_df
        )
        
        # Добавляем дату расчёта
        friends_recommendation_mart = friends_recommendation_mart.withColumn(
            "processed_dttm",
            F.current_timestamp()
        ).select(
            "user_left",
            "user_right",
            "processed_dttm",
            "zone_id",
            "local_time"
        )
        
        # Сохранение витрины
        logger.info(f"Сохранение витрины в {output_path}")
        friends_recommendation_mart.write.mode("overwrite").partitionBy("processed_dttm").parquet(output_path)
        
        logger.info("Витрина рекомендации друзей успешно построена")
        return friends_recommendation_mart


def main():
    # Параметры по умолчанию
    events_path = os.getenv("EVENTS_PATH", "/user/master/data/geo/events")
    cities_path = os.getenv("CITIES_PATH", "/user/master/data/geo/cities")
    output_path = os.getenv("OUTPUT_PATH", "/user/master/data/geo/friends_recommendation_mart")
    
    # Парсинг аргументов командной строки
    if len(sys.argv) > 1:
        events_path = sys.argv[1]
    if len(sys.argv) > 2:
        cities_path = sys.argv[2]
    if len(sys.argv) > 3:
        output_path = sys.argv[3]
    
    # Создание Spark сессии
    spark = SparkSession.builder \
        .appName("FriendsRecommendationMartBuilder") \
        .getOrCreate()
    
    try:
        # Построение витрины
        builder = FriendsRecommendationMartBuilder(spark)
        builder.build_mart(events_path, cities_path, output_path)
        
        logger.info("Скрипт успешно выполнен")
    except Exception as e:
        logger.error(f"Ошибка при выполнении скрипта: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

