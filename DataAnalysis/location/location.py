# open file
import csv
import pandas
from pyspark.sql.functions import substring
csvfile = sc.textFile('/user/cw2661/proj/crime.csv')
crime = csvfile.map(lambda line: next(csv.reader(line.splitlines(), skipinitialspace=True)))
crime.first()
# RDD to DataFrame
header = crime.first()
data = crime.filter(lambda line: line != header)
crime_df = sqlContext.createDataFrame(data, header)
crime_df.registerTempTable("crime")
sqlContext.sql("SELECT * FROM crime").show(10)

# get location information
coor_infor = sqlContext.sql("SELECT substring(RPT_DT,7, 10) as Y, CMPLNT_NUM, Latitude, Longitude  FROM crime WHERE Latitude != '' AND Longitude != ''")
coor_infor.show(100)
coor_infor.registerTempTable("crime_location")
year_count = sqlContext.sql("SELECT Y, COUNT(*)  FROM crime_location GROUP BY Y")
year_count.show(100)

year_2006 = sqlContext.sql("SELECT Latitude, Longitude, Y  FROM crime_location WHERE Y = 2006")
year_2006.toPandas().to_csv('year_2006.csv')

year_2011 = sqlContext.sql("SELECT Latitude, Longitude, Y  FROM crime_location WHERE Y = 2011")
year_2011.toPandas().to_csv('year_2011.csv')

year_2016 = sqlContext.sql("SELECT Latitude, Longitude, Y  FROM crime_location WHERE Y = 2016")
year_2016.toPandas().to_csv('year_2016.csv')

# get felony
law_count = sqlContext.sql("SELECT LAW_CAT_CD, count(*) FROM crime GROUP BY LAW_CAT_CD")
law_count.show()
coor_infor = sqlContext.sql("SELECT substring(RPT_DT,7, 10) as Y, LAW_CAT_CD, CMPLNT_NUM, Latitude, Longitude  FROM crime WHERE Latitude != '' AND Longitude != ''")
coor_infor.registerTempTable("crime_location")
FELONY_2015 = sqlContext.sql("SELECT Latitude, Longitude, Y  FROM crime_location WHERE Y > 2014 AND LAW_CAT_CD = 'FELONY'")
MISDEMEANOR_2015 = sqlContext.sql("SELECT Latitude, Longitude, Y  FROM crime_location WHERE Y > 2014 AND LAW_CAT_CD = 'MISDEMEANOR'")
VIOLATION_2015 = sqlContext.sql("SELECT Latitude, Longitude, Y  FROM crime_location WHERE Y > 2014 AND LAW_CAT_CD = 'MISDEMEANOR'")
FELONY_2015.toPandas().to_csv('FELONY_2015.csv')
MISDEMEANOR_2015.toPandas().to_csv('MISDEMEANOR_2015.csv')
VIOLATION_2015.toPandas().to_csv('VIOLATION_2015.csv')

# count missing 195868
sqlContext.sql("SELECT COUNT(*) FROM crime WHERE Lat_Lon = ''").show(10)
# count all 5580035
sqlContext.sql("SELECT COUNT(*) FROM crime").show(10)
