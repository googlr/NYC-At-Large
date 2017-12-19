
from pyspark.sql.functions import substring

# csvfile = sc.textFile('/user/xg626/NYPD_Complaint_Data_Historic.csv')

# Time Distribution
## CMPLNT_FR_TM
#>>> types = [f.dataType for f in crime_df.schema.fields]
#>>> types
# [StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType, StringType]



hour = sqlContext.sql("SELECT substring(CMPLNT_FR_TM,1,2) AS CMPLNT_FR_HOUR FROM crime ")
hour.registerTempTable("crime_df_hour")

cmplnt_fr_hour_count = sqlContext.sql("SELECT CMPLNT_FR_HOUR, COUNT(*) AS count FROM crime_df_hour GROUP BY CMPLNT_FR_HOUR")
cmplnt_fr_hour_count.toPandas().to_csv('cmplnt_fr_hour_count.csv')

# days
days = sqlContext.sql("SELECT substring(RPT_DT,1,2) AS MONTH, substring(RPT_DT,4,5) AS DAY, substring(RPT_DT,7,10) AS YEAR, COUNT(*) FROM crime GROUP BY RPT_DT")
days.toPandas().to_csv('cmplnt_fr_days_count.csv')
