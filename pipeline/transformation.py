"""
# transformation for project pyspark_boilerlpat_beginners
# Created by @Sanjeet Shukla at 12:14 AM 9/27/2021 using PyCharm
"""
import pyspark
from pyspark.sql import SparkSession
import logging
import logging.config
from utils import logger

class Transformation:
    def __init__(self, spark):
        self.spark = spark

    def transform(self, df):
        logger = logging.getLogger("Transformation")
        logger.info("Transforming Data")
        logger.warning("Warning in transforming Data")
        df1 = df.na.fill("Unknown",["author_name"])
        df2 = df1.na.fill("0", ["no_of_reviews"])
        return df2
