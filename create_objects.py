from pyspark.sql import SparkSession
import logging
import logging.config

logging.config.fileConfig(fname="../util/logs.conf")
logger = logging.getLogger("create_objects")

def get_spark_object(envn, appName):
    try:
        if envn == "TEST":
            master = 'local'
        else:
            master = 'yarn'
        spark = SparkSession.builder.appName(appName).master(master).getOrCreate()
        return spark
    except Exception as exp:
        logger.error("Error in method get_spark_object() " + str(exp), exc_info=True)

