
from pyspark.sql.functions import substring

# csvfile = sc.textFile('/user/xg626/NYPD_Complaint_Data_Historic.csv')

# Time Distribution
## CMPLNT_FR_TM
hour = sqlContext.sql("SELECT substring(CMPLNT_FR_TM,1,2) AS CMPLNT_FR_HOUR FROM crime ")
hour.registerTempTable("crime_df_hour")

cmplnt_fr_hour_count = sqlContext.sql("SELECT CMPLNT_FR_HOUR, COUNT(*) AS count FROM crime_df_hour GROUP BY CMPLNT_FR_HOUR")
cmplnt_fr_hour_count.toPandas().to_csv('cmplnt_fr_hour_count.csv')
