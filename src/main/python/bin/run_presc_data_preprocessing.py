import logging
import logging.config
from pyspark.sql.functions import upper, lit, regexp_extract, col, concat_ws, avg, round, coalesce
from pyspark.sql.window import try_remote_window

logging.config.fileConfig(fname="../util/logs.conf")
logger = logging.getLogger("run_data_preprocessing")

def data_preprocessing(df, df2):
    try:
        logger.info("data_preprocessing() function is started . . .")

        # df_city - Selecting required columns and converting columns to upper case
        df_city_cleaned = df.select(
                        upper(df.city).alias("city"),
                        df.state_id,
                        upper(df.state_name).alias("state_name"),
                        upper(df.county_name).alias("county_name"),
                        df.population,
                        df.zips
        )

        # df_fact - Cleaning
        df_fact_cleaned = df2.select(
                        df2.npi.alias("presc_id"), df2.nppes_provider_first_name.alias("presc_firstname"),
                        df2.nppes_provider_last_org_name.alias("presc_lastname"), df2.nppes_provider_city.alias("presc_city"),
                        df2.nppes_provider_state.alias("presc_state"), df2.specialty_description.alias("presc_speciality"),
                        df2.year_exp.alias("years_of_exp"), df2.drug_name, df2.total_claim_count.alias("trx_cnt"),
                        df2.total_day_supply, df2.total_drug_cost
        )
        # Using lit() to add a static country_name column with value 'USA'
        df_fact_cleaned = df_fact_cleaned.withColumn("country_name", lit("USA"))
        # Clean years_of_exp field
        pattern = '\\d+'
        idx=0
        df_fact_cleaned = df_fact_cleaned.withColumn("years_of_exp", regexp_extract("years_of_exp", pattern, idx))
        df_fact_cleaned = df_fact_cleaned.withColumn("years_of_exp", col("years_of_exp").cast('int'))
        # Combine firstname and lastname
        df_fact_cleaned = df_fact_cleaned.withColumn("presc_fullname", concat_ws(" ", "presc_firstname", "presc_lastname"))
        df_fact_cleaned = df_fact_cleaned.drop("presc_firstname", "presc_lastname")
        df_fact_cleaned = df_fact_cleaned.dropna(subset="presc_id")
        df_fact_cleaned = df_fact_cleaned.dropna(subset="drug_name")

        spec = try_remote_window.partitionBy("presc_id")
        df_fact_cleaned = df_fact_cleaned.withColumn('trx_cnt', coalesce("trx_cnt",round(avg("trx_cnt").over(spec))))
        df_fact_cleaned=df_fact_cleaned.withColumn("trx_cnt",col("trx_cnt").cast('integer'))


    except Exception as exp:
        logger.error(str(exp), exc_info=True)
        raise
    return df_city_cleaned, df_fact_cleaned