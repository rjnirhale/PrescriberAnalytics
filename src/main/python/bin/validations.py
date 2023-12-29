def get_curr_date(spark):
    try:
        opDF = spark.sql(""" SELECT current_date  """)
        print("Validating spark object by printing current date - " + str(opDF.collect()))
    except Exception as exp:
        print("Error in method :", str(exp))
        raise
