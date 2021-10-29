"""
# logger for project pyspark_boilerlpat_beginners
# Created by @Sanjeet Shukla at 12:52 AM 10/2/2021 using PyCharm
"""
import logging
import logging.config

logging.config.fileConfig("pipeline/resources/configs/logging.conf", defaults ={'logfilename': 'log/mylog.log'},
                          disable_existing_loggers=False)
logger = logging.getLogger(__name__)


