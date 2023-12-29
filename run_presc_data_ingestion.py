import logging
import logging.config

logging.config.fileConfig("../util/logs.conf")

# Get custom logger
logger = logging.getLogger("run_data_ingestion")
def load_files(spark, file_format, file_dir, header, inferSchema):
    logger.info("load_files() function is started . . .")
    try:
        logger.info("load_files() function is started . . .")
        if file_format == "parquet":
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == "csv":
            df = spark.read.format(file_format).options(header = header, inferSchema = inferSchema).load(file_dir)
        return df
    except Exception as exp:
        logger.error(str(exp), exc_info=True)
        raise