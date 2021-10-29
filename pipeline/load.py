"""
# load for project pyspark_boilerlpat_beginners
# Created by @Sanjeet Shukla at 12:15 AM 9/27/2021 using PyCharm
"""
import sys

import psycopg2
import pyspark
from pyspark.sql import SparkSession
import logging
import logging.config
import configparser
from utils import logger


class Load:
	def __init__(self, spark):
		self.spark = spark

	def load_target(self, df):
		try:
			logger = logging.getLogger("Load")
			logger.info("Loading target table")
			config = configparser.ConfigParser()
			config.read('pipeline/resources/pipeline.ini')
			target_table = config.get('HIVE_CONFIGS', 'TARGET_PG_TABLE')
			logger.info('PG Target table is:' + str(target_table))
			# df.write.option("header", "true").csv("./_data/transformed_salary_csv/")
			df.write.mode("append").format("jdbc").\
				option("url", "jdbc:postgresql://localhost:5432/postgres")\
				.option("dbtable","pgreview.course_review")\
				.option("user", "postgres")\
				.option("password","admin")\
				.save()
			logger.info("Target table load complete" )

		except Exception as exp:
			logger.error("An error occured while persisting data > " + str(exp))
			# store log in db table or
			# send email
			raise Exception("HDFS Directory Already Exists")

	def insert_data_to_pg(self):
		connection = psycopg2.connect(user='postgres', password='admin', host='localhost', database='postgres')
		cursor = connection.cursor()
		insert_query = "INSERT INTO pgreview.book_review (course_id, course_name, author_name, course_section, creation_date) VALUES (%s, %s, %s, %s,%s)"
		insert_tuple = (3, 'Machine Learning', 'FutureX', '{}', '2020-10-20')
		cursor.execute(insert_query, insert_tuple)
		cursor.close()
		connection.commit()

	def write_to_hive_table(self, df):
		self.spark.sql("create database if not exists hive_db")
		df.write.mode("overwrite").saveAsTable("hive_db.book_review")
