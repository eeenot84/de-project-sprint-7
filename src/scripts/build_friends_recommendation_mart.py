import pyspark.sql.functions as F  # type: ignore
from pyspark.sql.window import Window  # type: ignore
from spark_app import SparkApp


class UserRecommendations(SparkApp):
    def __init__(self):
        super().__init__(
            "UserRecommendations",
            "UserRecommendations"
        )

    def run(self, args):
        date = args[0]
        events_path = args[1]
        cities_path = args[2]
        user_recommendations = args[3]

        end_date = F.to_date(F.lit(date), "yyyy-MM-dd")
        events_users = self.geo_events_source(events_path, end_date).persist()

        subscriptions = self.prepare_subscriptions(events_users)
        current_friends = self.prepare_current_friends(events_users)
        user_actual_geo = self.prepare_user_actual_geo(events_users, cities_path)

        channel_neighbours = self.get_channel_neighbours(subscriptions)
        channel_neighbours_recom = channel_neighbours \
            .join(
                current_friends,
                on=['user_left', 'user_right'],
                how='left_anti'
            )

        dist_col = 'user_dist_km'
        result = self.get_recom_geo_distances(
                channel_neighbours_recom,
                user_actual_geo,
                dist_col
            ) \
            .filter(f'{dist_col} < 1') \
            .select(
                'user_left',
                'user_right',
                end_date.alias('processed_dttm'),
                F.col('zone_left').alias('zone_id'),
                F.col('local_time_left').alias('local_time')
            )

        result.write.mode("overwrite").partitionBy("processed_dttm").parquet(f'{user_recommendations}/dt={date}')

    def get_recom_geo_distances(self, user_recoms, user_geo, dist_col):
        distance_recom = user_recoms \
            .join(
                self.get_join_side_get(user_geo, 'left'),
                on='user_left'
            ) \
            .join(
                self.get_join_side_get(user_geo, 'right'),
                on='user_right'
            ) \
            .withColumn(
                dist_col,
                self.coord_dist_col(
                    'city_lat_left', 'city_lon_left',
                    'city_lat_right', 'city_lon_right'
                )
            )
        return distance_recom

    def get_join_side_get(self, geo_users, side):
        return geo_users \
            .select(
                F.col('user_id').alias(f'user_{side}'),
                F.col('zone_id').alias(f'zone_{side}'),
                F.col('city_lat').alias(f'city_lat_{side}'),
                F.col('city_lon').alias(f'city_lon_{side}'),
                F.col('local_time').alias(f'local_time_{side}')
            )

    def get_channel_neighbours(self, subscriptions):
        channel_neighbours = subscriptions \
            .withColumnRenamed('user_id', 'user_left') \
            .join(
                subscriptions \
                    .withColumnRenamed('user_id', 'user_right'),
                on='channel_id'
            ) \
            .filter('user_left < user_right')
        return channel_neighbours

    def prepare_subscriptions(self, events):
        subs = events.where("event.event_type = 'subscription'") \
            .select(
                F.col("event.message_from").alias("user_id"),
                F.col("event.message_to").alias("channel_id")
            ) \
            .distinct()
        return subs

    def prepare_current_friends(self, events):
        users_pair = F.array([
            F.col("event.message_from"),
            F.col("event.message_to")
        ])
        messages = events \
            .where(
                "event.message_from is not null and event.message_to is not null and event.event_type = 'message'"
            ) \
            .select(
                F.array_min(users_pair).alias('user_left'),
                F.array_max(users_pair).alias('user_right')
            ) \
            .distinct()
        return messages

    def _filter_user_last_events(self, geo_events):
        last_travel_wnd = Window().partitionBy('user_id').orderBy(F.desc('message_ts'))
        return geo_events \
            .withColumn('rn', F.row_number().over(last_travel_wnd)) \
            .filter('rn = 1') \
            .drop('rn')

    def prepare_user_actual_geo(self, geo_events, cities_path):
        cities_df = self.load_cities(cities_path)
        
        events_with_coords = geo_events.filter(
            F.col("lat").isNotNull() & F.col("lon").isNotNull()
        ).select(
            F.col("event.message_from").alias("user_id"),
            F.col("event.message_ts").alias("message_ts"),
            F.col("lat"),
            F.col("lon")
        )

        cities_prep = cities_df.select(
            F.col("id").alias("city_id"),
            F.col("lat").alias("city_lat"),
            F.col("lng").alias("city_lon"),
            F.col("timezone")
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

        window_spec = Window.partitionBy("user_id").orderBy("distance")
        events_with_city = events_cross_cities.withColumn(
            "row_num",
            F.row_number().over(window_spec)
        ).filter(
            F.col("row_num") == 1
        ).select(
            F.col("user_id"),
            F.col("city_id").alias("zone_id"),
            F.col("city_lat").alias("city_lat"),
            F.col("city_lon").alias("city_lon"),
            F.col("timezone"),
            F.col("message_ts")
        )

        user_last_travels = self._filter_user_last_events(events_with_city) \
            .withColumn(
                "time_utc",
                F.from_unixtime(F.col("message_ts"))
            ) \
            .withColumn(
                "local_time",
                F.from_utc_timestamp(
                    F.col("time_utc"),
                    F.coalesce(F.col("timezone"), F.lit("UTC"))
                )
            ) \
            .select(
                'user_id',
                'zone_id',
                'local_time',
                'city_lat',
                'city_lon'
            )

        return user_last_travels

    def geo_events_source(self, path, end_date):
        return self.spark.read.parquet(path)

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
    UserRecommendations().main()
