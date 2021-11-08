"""
author @Sanjeet Shukla
"""
import sys

from pyspark.sql import SparkSession
from pipeline import extraction, load, transformation
import logging
import logging.config
from utils import logger


class Pipeline:
    def run_pipeline(self):
        try:
            logger = logging.getLogger("Transformation")
            logger.info("run pipeline method started")
            extract_process = extraction.Extraction(self.spark)  # Instantiate using file_name.class_name
            hive_df = extract_process.extract_data()
            hive_df.show()
            df =extract_process.readFrompgSql()
            df.show()
            extract_process.readFrompg_using_dbbc_driver()
            book_df = extract_process.extract_data()
            book_df.show()
            transform_process = transformation.Transformation(self.spark)
            transformed_df = transform_process.transform(df)
            transformed_df.show()
            load_process = load.Load(self.spark)
            load_process.write_to_hive_table(df)

            logger.info("run pipeline method ended")
        except Exception as exp:
            logger.error("An error occured while running the pipeline > " + str(exp))
            # send email notification or
            # log error to db
            sys.exit(1)
        return

    def create_spark_session(self):
        self.spark = SparkSession.builder\
            .appName("my pyspark app")\
            .config("spark.driver.extraClassPath", "postgresql-42.2.18.jar")\
            .enableHiveSupport()\
            .getOrCreate()

if __name__ == '__main__':
    logging.info("Application Started")
    pipeline = Pipeline()
    pipeline.create_spark_session()
    logging.info("Spark Session Created")
    # pipeline.createHiveTable()
    pipeline.run_pipeline()
    logging.info("Pipeline Executed successfully")
    # C:\python\Lib\site-packages\pyspark
