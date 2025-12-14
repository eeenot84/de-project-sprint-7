import sys
import logging

from abc import ABC, abstractmethod
from pyspark.sql import SparkSession  # type: ignore


class SparkApp(ABC):
    def __init__(self, app_name, logger_name):
        self.app_name = app_name
        self.logger_name = logger_name

    @abstractmethod
    def run(self, args):
        pass

    def main(self):
        # init spark
        self.spark = SparkSession \
            .builder.appName(f"{self.app_name}") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .getOrCreate()

        # init logger
        self.logger = logging.getLogger(self.logger_name)
        logging.basicConfig(level=logging.INFO)

        # init args
        args = sys.argv[1:]

        self.logger.info(f'Starting app {self.app_name}')

        try:
            self.run(args)
            self.logger.info(f'Finishing app {self.app_name}')
        except Exception as e:
            self.logger.error(f'Stopping app {self.app_name}')
            self.logger.exception(e)
            raise
        finally:
            self.spark.stop()

