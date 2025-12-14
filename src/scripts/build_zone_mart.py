import pyspark.sql.functions as F  # type: ignore
from pyspark.sql.window import Window  # type: ignore
from spark_app import SparkApp


class GeoStats(SparkApp):
    EVENTS = 'message subscription reaction user'.split()

    def __init__(self):
        super().__init__(
            "GeoStats",
            "GeoStats"
        )

    def run(self, args):
        date = args[0]
        events_path = args[1]
        cities_path = args[2]
        geo_stats_path = args[3]

        end_date = F.to_date(F.lit(date), "yyyy-MM-dd")
        events_users = self.geo_events_source(events_path, end_date, cities_path).persist()

        week_stats = self.pivot_agg_stats(
            events_users \
                .groupBy('week', 'month', 'zone_id', 'event_type'),
            'week'
        )

        month_stats = self.pivot_agg_stats(
            events_users \
                .groupBy('month', 'zone_id', 'event_type'),
            'month'
        )

        joint_stats = week_stats \
            .join(month_stats, on=['month', 'zone_id'], how='full')

        joint_stats.write.mode("overwrite").partitionBy("month", "week").parquet(f'{geo_stats_path}/dt={date}')

    def pivot_agg_stats(self, grouped_events, prefix):
        agg_stats = grouped_events \
            .pivot('event_type', self.EVENTS) \
            .count('user_id') \
            .select(
                *[col for col in grouped_events.columns if col != 'event_type'],
                *[F.coalesce(F.col(event), F.lit(0)).alias(f'{prefix}_{event}') for event in self.EVENTS]
            )

        for event in self.EVENTS:
            if f'{prefix}_{event}' not in agg_stats.columns:
                agg_stats = agg_stats.withColumn(f'{prefix}_{event}', F.lit(0))

        return agg_stats

    def geo_events_source(self, path, end_date, cities_path):
        cities_df = self.load_cities(cities_path)
        
        events_df = self.spark.read.parquet(path)
        
        cities_prep = cities_df.select(
            F.col("id").alias("city_id"),
            F.col("lat").alias("city_lat"),
            F.col("lng").alias("city_lon")
        )

        events_with_coords = events_df.filter(
            F.col("lat").isNotNull() & F.col("lon").isNotNull()
        )

        events_cross_cities = events_with_coords.crossJoin(cities_prep).withColumn(
            "distance",
            self.coord_dist_col(
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

        events_without_coords = events_df.filter(
            F.col("lat").isNull() | F.col("lon").isNull()
        )

        last_coords_all = events_with_coords.groupBy("event.message_from").agg(
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
        ).dropDuplicates(["user_id"])

        events_without_coords_enriched = events_without_coords.join(
            last_coords_all,
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

        events_without_coords_cross_cities = events_without_coords_enriched.filter(
            F.col("lat").isNotNull() & F.col("lon").isNotNull()
        ).crossJoin(cities_prep).withColumn(
            "distance",
            self.coord_dist_col(
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

        all_events_enriched = events_with_city.union(events_without_coords_with_city)

        first_messages = all_events_enriched.filter(
            F.col("event_type") == "message"
        ).groupBy("user_id").agg(
            F.min("message_ts").alias("first_message_ts")
        )

        events_with_registration = all_events_enriched.join(
            first_messages,
            on=[
                F.col("user_id") == F.col("user_id"),
                F.col("message_ts") == F.col("first_message_ts")
            ],
            how="left"
        ).withColumn(
            "event_type",
            F.when(F.col("first_message_ts").isNotNull(), F.lit("user"))
            .otherwise(F.col("event_type"))
        )

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

        return events_with_dates

    def load_cities(self, cities_path):
        cities_df = self.spark.read.csv(
            cities_path,
            header=True,
            sep=";",
            inferSchema=True
        )
        cities_df = cities_df.withColumn("lat", F.regexp_replace(F.col("lat"), ",", ".").cast("double"))
        cities_df = cities_df.withColumn("lng", F.regexp_replace(F.col("lng"), ",", ".").cast("double"))
        return cities_df

    def coord_dist_col(self, a_lat, a_lon, b_lat, b_lon):
        return F.acos(
            F.sin(F.radians(a_lat)) * F.sin(F.radians(b_lat))
            + F.cos(F.radians(a_lat))
            * F.cos(F.radians(b_lat))
            * F.cos(F.radians(a_lon - b_lon))
        ) * F.lit(6371.0)


if __name__ == '__main__':
    GeoStats().main()
