# Import all the modules
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date
from run_presc_data_ingestion import load_files
from run_presc_data_preprocessing import data_preprocessing

import logging
import logging.config
import os

logging.config.fileConfig(fname="../util/logs.conf")

def main():
    try:
        logging.info("main() is started . . ")
        # SparkSession
        spark = get_spark_object(gav.envn, gav.appName)
        # Validation of spark object
        op = get_curr_date(spark)

        # Data Ingestion Call 1 - Load City Dimension Table
        for file in os.listdir(gav.path_dim_city):
            print("File is: " + file)
            file_dir = gav.path_dim_city + "\\" + file
            #print(file_dir)

            if file.split('.')[1] == "csv":
                file_format= "csv"
                header = gav.header
                inferSchema = gav.inferSchema
            elif file.split('.')[1] == "parquet":
                file_format = "parquet"
                header = "NA"
                inferSchema = "NA"
                # load files
        df_city = load_files(spark, file_format, file_dir, header, inferSchema)
        df_city.show(5)

        # Data Ingestion Call 2 - Load Prescriber Fact Table
        for file in os.listdir(gav.path_fact):
            print("File is: " + file)
            file_dir = gav.path_fact + "\\" + file
            #print(file_dir)

            if file.split('.')[1] == "csv":
                file_format= "csv"
                header = gav.header
                inferSchema = gav.inferSchema
            elif file.split('.')[1] == "parquet":
                file_format = "parquet"
                header = "NA"
                inferSchema = "NA"
        df_presc_fact = load_files(spark, file_format, file_dir, header, inferSchema)
        df_presc_fact.show(5)

        # Data Cleaning Call
        df_city, df_presc_fact = data_preprocessing(df_city, df_presc_fact)
        df_city.show(5)
        df_presc_fact.show(5)

    except Exception as exp:
        logging.error('Error in main(), check dependencies: ' + str(exp), exc_info=True)

if __name__ == "__main__":
    logging.info("Pipeline run_pres_pipeline is started . . ")
    main()
