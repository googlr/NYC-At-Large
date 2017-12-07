from pyspark.sql.functions import substring

crime_df_hour = crime_df.select(substring(crime_df.CMPLNT_FR_TM,1,2).alias('hour')).collect()