"""
# extraction for project pyspark_boilerlpat_beginners
# Created by @Sanjeet Shukla at 12:14 AM 9/27/2021 using PyCharm
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import logging
import logging.config
import psycopg2
import pandas.io.sql as sqlio
from utils import logger



class Extraction:
    def __init__(self, spark):
        self.spark = spark

    def extract_data(self):
        logger = logging.getLogger("Extraction")
        logger.info("Ingesting data from csv")
        # salary_df = self.spark.read.csv("./_input_data/adult.csv", header=True)
        book_df = self.spark.table("review.book_review")
        logger.info("Dataframe created")
        logger.warning("Dummy Warning Dataframe created")
        return book_df
    def readFrompgSql(self):
        connection = psycopg2.connect(user='postgres', password='admin', host='localhost', database ='postgres')
        cursor = connection.cursor()
        sql_query = "SELECT * from pgreview.course_review"
        poDF = sqlio.read_sql_query(sql_query, connection)
        sparkDF = self.spark.createDataFrame(poDF)
        sparkDF.show()
        return sparkDF

    def readFrompg_using_dbbc_driver(self):
        # disabled currently as throwing error: java.sql.SQLException: No suitable driver.
        # will use above readFrompgSql function.
        jdbcDF = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres") \
            .option("dbtable", "pgreview.book_review") \
            .option("user", "postgres") \
            .option("password", "admin") \
            .load()
        jdbcDF.show()
        return jdbcDF