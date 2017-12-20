# open file
import csv
import pandas
from pyspark.sql.functions import substring
csvfile = sc.textFile('/user/cw2661/proj/crime.csv')
crime = csvfile.map(lambda line: next(csv.reader(line.splitlines(), skipinitialspace=True)))
crime.first()

weatherfile = sc.textFile('/user/cw2661/proj/1147564.csv')
weather = weatherfile.map(lambda line: next(csv.reader(line.splitlines(), skipinitialspace=True)))
weather.first()



# RDD to DataFrame
header = crime.first()
data = crime.filter(lambda line: line != header)
crime_df = sqlContext.createDataFrame(data, header)
crime_df.registerTempTable("crime")
sqlContext.sql("SELECT * FROM crime").show(10)

header = weather.first()
data = weather.filter(lambda line: line != header)
weather_df = sqlContext.createDataFrame(data, header)
weather_df.registerTempTable("weather")
sqlContext.sql("SELECT * FROM weather").show(10)

# crime summary
date_count = sqlContext.sql("SELECT COUNT(*) as count, RPT_DT FROM crime group by RPT_DT")
date_count.registerTempTable("date_count")


# change time format
sqlContext.sql("SELECT TO_DATE(CAST(UNIX_TIMESTAMP(RPT_DT, 'MM/dd/yyyy') AS TIMESTAMP)) AS DATE, count FROM date_count").registerTempTable("date_count")

sqlContext.sql("SELECT TO_DATE(CAST(UNIX_TIMESTAMP(DATE, 'yyyy-MM-dd') AS TIMESTAMP)) AS DATE, AWND, FMTM, PGTM, PRCP, TMAX, TMIN FROM weather").registerTempTable("weather")

# join
merge_count_weather = sqlContext.sql("SELECT * FROM date_count, weather WHERE date_count.DATE = weather.DATE")
merge_count_weather.toPandas().to_csv('merge.csv')

# place summary
sqlContext.sql("SELECT COUNT(*),LOC_OF_OCCUR_DESC FROM crime GROUP BY LOC_OF_OCCUR_DESC").show()
# crime summary without missing data
date_count_all = sqlContext.sql("SELECT COUNT(*) as a, RPT_DT FROM crime WHERE LOC_OF_OCCUR_DESC <> '' group by RPT_DT")
date_count_all.registerTempTable("date_count_all")
date_count_inside = sqlContext.sql("SELECT COUNT(*) as i, RPT_DT FROM crime WHERE LOC_OF_OCCUR_DESC = 'INSIDE' group by RPT_DT")
date_count_inside.registerTempTable("date_count_inside")

date_count = sqlContext.sql("SELECT i / a as count, a as all, date_count_inside.RPT_DT FROM date_count_all, date_count_inside WHERE date_count_all.RPT_DT=date_count_inside.RPT_DT")
date_count.registerTempTable("date_count")
sqlContext.sql("SELECT TO_DATE(CAST(UNIX_TIMESTAMP(RPT_DT, 'MM/dd/yyyy') AS TIMESTAMP)) AS DATE, count FROM date_count").registerTempTable("date_count")

sqlContext.sql("SELECT TO_DATE(CAST(UNIX_TIMESTAMP(DATE, 'yyyy-MM-dd') AS TIMESTAMP)) AS DATE, AWND, FMTM, PGTM, PRCP, TMAX, TMIN FROM weather").registerTempTable("weather")

# join
merge_count_weather = sqlContext.sql("SELECT * FROM date_count, weather WHERE date_count.DATE = weather.DATE")
merge_count_weather.toPandas().to_csv('merge_rate.csv')