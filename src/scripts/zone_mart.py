"""
Скрипт для создания витрины данных в разрезе зон.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ZoneMartBuilder:
    
    EARTH_RADIUS_KM = 6371.0
    
    def __init__(self, spark):
        self.spark = spark
    
    def load_events(self, events_path: str):
        """
        Загрузка данных о событиях.
        
        Args:
            events_path: путь к данным событий в HDFS
            
        Returns:
            DataFrame с событиями
        """
        logger.info(f"Загрузка событий из {events_path}")
        events_df = self.spark.read.parquet(events_path)
        logger.info(f"Загружено {events_df.count()} событий")
        return events_df
    
    def load_cities(self, cities_path):
        cities_df = self.spark.read.csv(
            cities_path,
            header=True,
            sep=";",
            inferSchema=True
        )
        cities_df = cities_df.withColumn("lat", F.regexp_replace(F.col("lat"), ",", ".").cast("double"))
        cities_df = cities_df.withColumn("lng", F.regexp_replace(F.col("lng"), ",", ".").cast("double"))
        logger.info(f"Загружено {cities_df.count()} городов")
        return cities_df
    
    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """
        Вычисление расстояния между двумя точками на сфере по формуле гаверсинуса.
        
        Args:
            lat1, lon1: координаты первой точки (в градусах)
            lat2, lon2: координаты второй точки (в градусах)
            
        Returns:
            Расстояние в километрах
        """
        # Преобразование градусов в радианы
        lat1_rad = F.radians(lat1)
        lat2_rad = F.radians(lat2)
        delta_lat = F.radians(lat2 - lat1)
        delta_lon = F.radians(lon2 - lon1)
        
        # Формула гаверсинуса
        a = (
            F.pow(F.sin(delta_lat / 2), 2) +
            F.cos(lat1_rad) * F.cos(lat2_rad) * F.pow(F.sin(delta_lon / 2), 2)
        )
        c = 2 * F.asin(F.sqrt(a))
        distance = self.EARTH_RADIUS_KM * c
        
        return distance
    
    def enrich_events_with_cities(self, events_df, cities_df):
        """
        Обогащение событий информацией о городах.
        
        Для событий с координатами определяется ближайший город.
        Для событий без координат присваиваются координаты последнего сообщения пользователя.
        
        Args:
            events_df: DataFrame с событиями
            cities_df: DataFrame с городами
            
        Returns:
            DataFrame с обогащенными событиями
        """
        logger.info("Обогащение событий информацией о городах")
        
        # Подготовка данных о городах
        cities_prep = cities_df.select(
            F.col("id").alias("city_id"),
            F.col("city").alias("city_name"),
            F.col("lat").alias("city_lat"),
            F.col("lng").alias("city_lon")
        )
        
        # Фильтруем события с координатами (исходящие сообщения)
        events_with_coords = events_df.filter(
            F.col("lat").isNotNull() & F.col("lon").isNotNull()
        )
        
        # Определяем города для событий с координатами
        events_cross_cities = events_with_coords.crossJoin(cities_prep).withColumn(
            "distance",
            self.calculate_distance(
                F.col("lat"),
                F.col("lon"),
                F.col("city_lat"),
                F.col("city_lon")
            )
        )
        
        window_spec = Window.partitionBy("event.message_id").orderBy("distance")
        events_with_city = events_cross_cities.withColumn(
            "row_num",
            F.row_number().over(window_spec)
        ).filter(
            F.col("row_num") == 1
        ).select(
            F.col("event.message_id").alias("message_id"),
            F.col("event.message_from").alias("user_id"),
            F.col("event.message_ts").alias("message_ts"),
            F.col("event.event_type").alias("event_type"),
            F.col("city_id").alias("zone_id")
        )
        
        # Для событий без координат получаем последние координаты пользователя
        events_without_coords = events_df.filter(
            F.col("lat").isNull() | F.col("lon").isNull()
        )
        
        # Получаем последние координаты для каждого пользователя
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
            F.col("event.message_ts").alias("message_ts"),
            F.col("event.event_type").alias("event_type"),
            F.col("lat"),
            F.col("lon")
        )
        
        # Определяем города для событий без координат
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
            F.col("message_ts"),
            F.col("event_type"),
            F.col("city_id").alias("zone_id")
        )
        
        # Объединяем все события
        all_events_enriched = events_with_city.union(events_without_coords_with_city)
        
        logger.info(f"Обогащено {all_events_enriched.count()} событий")
        return all_events_enriched
    
    def identify_registration_events(self, events_enriched_df):
        # Регистрация определяется по первому сообщению пользователя
        first_messages = events_enriched_df.filter(
            F.col("event_type") == "message"
        ).groupBy("user_id").agg(
            F.min("message_ts").alias("first_message_ts")
        )
        
        # Помечаем события регистрации
        events_with_registration = events_enriched_df.join(
            first_messages,
            on=[
                F.col("user_id") == F.col("user_id"),
                F.col("message_ts") == F.col("first_message_ts")
            ],
            how="left"
        ).withColumn(
            "is_registration",
            F.when(F.col("first_message_ts").isNotNull(), 1).otherwise(0)
        )
        
        return events_with_registration
    
    def build_mart(self, events_path, cities_path, output_path):
        events_df = self.load_events(events_path)
        cities_df = self.load_cities(cities_path)
        
        # Обогащение событий
        events_enriched_df = self.enrich_events_with_cities(events_df, cities_df)
        
        # Определение событий регистрации
        events_with_registration = self.identify_registration_events(events_enriched_df)
        
        # Преобразуем timestamp в дату и определяем неделю и месяц
        events_with_dates = events_with_registration.withColumn(
            "date",
            F.to_date(F.from_unixtime(F.col("message_ts")))
        ).withColumn(
            "month",
            F.date_format(F.col("date"), "yyyy-MM")
        ).withColumn(
            "week",
            F.date_format(
                F.date_sub(
                    F.col("date"),
                    F.dayofweek(F.col("date")) - 1
                ),
                "yyyy-MM-dd"
            )
        )
        
        # Агрегация по неделям
        week_agg = events_with_dates.groupBy("week", "zone_id").agg(
            F.sum(
                F.when(F.col("event_type") == "message", 1).otherwise(0)
            ).alias("week_message"),
            F.sum(
                F.when(F.col("event_type") == "reaction", 1).otherwise(0)
            ).alias("week_reaction"),
            F.sum(
                F.when(F.col("event_type") == "subscription", 1).otherwise(0)
            ).alias("week_subscription"),
            F.sum(F.col("is_registration")).alias("week_user")
        )
        
        # Агрегация по месяцам
        month_agg = events_with_dates.groupBy("month", "zone_id").agg(
            F.sum(
                F.when(F.col("event_type") == "message", 1).otherwise(0)
            ).alias("month_message"),
            F.sum(
                F.when(F.col("event_type") == "reaction", 1).otherwise(0)
            ).alias("month_reaction"),
            F.sum(
                F.when(F.col("event_type") == "subscription", 1).otherwise(0)
            ).alias("month_subscription"),
            F.sum(F.col("is_registration")).alias("month_user")
        )
        
        # Объединение агрегаций по неделям и месяцам
        # Добавляем месяц к недельным агрегациям для объединения
        week_agg_with_month = week_agg.withColumn(
            "month",
            F.substring(F.col("week"), 1, 7)
        )
        
        # Объединяем недельные и месячные агрегации
        zone_mart = week_agg_with_month.join(
            month_agg,
            on=["month", "zone_id"],
            how="full"
        ).select(
            F.coalesce(
                F.col("week_agg_with_month.month"),
                F.col("month_agg.month")
            ).alias("month"),
            F.col("week"),
            F.coalesce(
                F.col("week_agg_with_month.zone_id"),
                F.col("month_agg.zone_id")
            ).alias("zone_id"),
            F.coalesce(F.col("week_message"), F.lit(0)).alias("week_message"),
            F.coalesce(F.col("week_reaction"), F.lit(0)).alias("week_reaction"),
            F.coalesce(F.col("week_subscription"), F.lit(0)).alias("week_subscription"),
            F.coalesce(F.col("week_user"), F.lit(0)).alias("week_user"),
            F.coalesce(F.col("month_message"), F.lit(0)).alias("month_message"),
            F.coalesce(F.col("month_reaction"), F.lit(0)).alias("month_reaction"),
            F.coalesce(F.col("month_subscription"), F.lit(0)).alias("month_subscription"),
            F.coalesce(F.col("month_user"), F.lit(0)).alias("month_user")
        )
        
        # Сохранение витрины
        logger.info(f"Сохранение витрины в {output_path}")
        zone_mart.write.mode("overwrite").partitionBy("month", "week").parquet(output_path)
        
        logger.info("Витрина зон успешно построена")
        return zone_mart


def main():
    """Основная функция для запуска скрипта."""
    # Параметры по умолчанию
    events_path = os.getenv("EVENTS_PATH", "/user/master/data/geo/events")
    cities_path = os.getenv("CITIES_PATH", "/user/master/data/geo/cities")
    output_path = os.getenv("OUTPUT_PATH", "/user/master/data/geo/zone_mart")
    
    # Парсинг аргументов командной строки
    if len(sys.argv) > 1:
        events_path = sys.argv[1]
    if len(sys.argv) > 2:
        cities_path = sys.argv[2]
    if len(sys.argv) > 3:
        output_path = sys.argv[3]
    
    # Создание Spark сессии
    spark = SparkSession.builder \
        .appName("ZoneMartBuilder") \
        .getOrCreate()
    
    try:
        # Построение витрины
        builder = ZoneMartBuilder(spark)
        builder.build_mart(events_path, cities_path, output_path)
        
        logger.info("Скрипт успешно выполнен")
    except Exception as e:
        logger.error(f"Ошибка при выполнении скрипта: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

