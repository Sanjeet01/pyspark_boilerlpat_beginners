"""
# test_transformer for project pyspark_boilerlpat_beginners
# Created by @Sanjeet Shukla at 8:17 AM 10/2/2021 using PyCharm
"""
import unittest

from pyspark.sql import SparkSession
from pipeline import transformation


class Transform_Test(unittest.TestCase):
	def test_transform_should_replace_null_value(self):
		spark = SparkSession.builder.appName("test app").enableHiveSupport().getOrCreate()
		df = spark.read\
			.option("header", "true")\
			.option("inferSchema", "true")\
			.csv("./input_data/mock_course_data.csv")

		df.show()
		transform_process = transformation.Transformation(spark)
		transformed_df = transform_process.transform(df)
		transformed_df.show()

		cms_author = transformed_df.filter("course_id='2'").select("author_name").collect()[0].author_name
		print("CMS Author Name is: " + str(cms_author))
		spark.stop()
		self.assertEqual("Unknown", str(cms_author))

	# def test_should_throw_type_error(self):
	# 	spark = SparkSession.builder.appName("test app").enableHiveSupport().getOrCreate()
	# 	transform_process = transformation.Transformation(spark)
	# 	with self.assertRaises(AttributeError): transform_process.transform(None)




if __name__ == "__main__":
	unittest.main()


