import csv
import datetime
import time
import pandas

csvfile = sc.textFile('/user/cl4062/crime.csv')
crime = csvfile.map(lambda line: next(csv.reader(line.splitlines(), skipinitialspace=True)))
header = crime.first()
data = crime.filter(lambda line: line != header)

#get exact datetime of occurence for each reported event
def getTime(line):
	if line[1]='':
		#return an invalid value if date does not exit
		return [0,0,0,10]
    date=line[1]
    t=time.strptime(line[1],"%m/%d/%Y")
    weekday=datetime.datetime(t[0],t[1],t[2]).weekday()
	return (t[0],t[1],t[2],weekday)
data_time=data.map(lambda line:getTime(line))
schema=["year","month","day","weekday"]
#RDD to DataFrame
crime_df_time=sqlContext.createDataFrame(data,header)
crime_df_time.registerTempTable("crime_time")

#total number of years
sqlContext.sql("SELECT count(distinct year) AS num_year FROM crime_time WHERE year!=0").show()
#89
sqlContext.sql("SELECT year,count(*) AS total FROM crime_time WHERE year!=0 GROUP BY year").show()

# +----+------+
# |year| total|
# +----+------+
# |1015|     9|
# |1016|    15|
# |1026|     5|
# |1900|     5|
# |1905|     2|
# |1906|     1|
# |1908|     3|
# |1909|     3|
# |1910|     9|
# |1911|     7|
# |1912|    10|
# |1913|     9|
# |1914|    11|
# |1915|     8|
# |1916|     6|
# |1919|     1|
# |1920|     6|
# |1922|     1|
# |1929|     1|
# |1930|     1|
# |1938|     1|
# |1940|     1|
# |1941|     2|
# |1942|     2|
# |1945|     2|
# |1946|     2|
# |1948|     1|
# |1950|     3|
# |1954|     2|
# |1955|     3|
# |1956|     1|
# |1958|     1|
# |1959|     2|
# |1960|    10|
# |1961|     1|
# |1962|     3|
# |1964|     1|
# |1965|     5|
# |1966|    27|
# |1967|    13|
# |1968|    10|
# |1969|     7|
# |1970|     6|
# |1971|     3|
# |1972|     6|
# |1973|     7|
# |1974|     9|
# |1975|     8|
# |1976|     5|
# |1977|     9|
# |1978|     6|
# |1979|     9|
# |1980|    14|
# |1981|     8|
# |1982|     7|
# |1983|     6|
# |1984|     8|
# |1985|    21|
# |1986|    30|
# |1987|    16|
# |1988|    17|
# |1989|    25|
# |1990|    35|
# |1991|    29|
# |1992|    45|
# |1993|    46|
# |1994|    64|
# |1995|    75|
# |1996|   122|
# |1997|   134|
# |1998|   224|
# |1999|   342|
# |2000|   908|
# |2001|  1008|
# |2002|  1047|
# |2003|  1547|
# |2004|  2116|
# |2005| 10797|
# |2006|539084|
# |2007|537242|
# |2008|528744|
# |2009|511014|
# |2010|509853|
# |2011|498381|
# |2012|504334|
# |2013|495304|
# |2014|491131|
# |2015|477031|
# |2016|468290|
# +----+------+

sqlContext.sql("SELECT day,count(*) AS total FROM crime_time WHERE day!=0 GROUP BY day ORDER BY day ASC").show()

# +---+------+
# |day| total|
# +---+------+
# |  0|   655|
# |  1|202576|
# |  2|155643|
# |  3|157460|
# |  4|156980|
# |  5|160023|
# |  6|157218|
# |  7|155202|
# |  8|157328|
# |  9|155822|
# | 10|163026|
# | 11|156913|
# | 12|159434|
# | 13|158654|
# | 14|156085|
# | 15|167017|
# | 16|155626|
# | 17|159882|
# | 18|157885|
# | 19|155810|
# | 20|166023|
# | 21|157341|
# | 22|158089|
# | 23|154997|
# | 24|154634|
# | 25|153748|
# | 26|152366|
# | 27|155147|
# | 28|153894|
# | 29|145233|
# | 30|141604|
# | 31| 89401|
# +---+------+

sqlContext.sql("SELECT month,count(*) AS total FROM crime_time WHERE month!=0 GROUP BY month ORDER BY month ASC").show()
# +-----+------+
# |month| total|
# +-----+------+
# |    1|454690|
# |    2|398039|
# |    3|461557|
# |    4|455745|
# |    5|490070|
# |    6|482081|
# |    7|497806|
# |    8|498578|
# |    9|477888|
# |   10|486605|
# |   11|442196|
# |   12|434125|
# +-----+------+

sqlContext.sql("SELECT weekday,count(*) AS total FROM crime_time WHERE weekday!=10 GROUP BY weekday ORDER BY weekday ASC").show()
# +-------+------+
# |weekday| total|
# +-------+------+
# |      0|748319|
# |      1|800960|
# |      2|822849|
# |      3|813352|
# |      4|867344|
# |      5|806756|
# |      6|719800|
# +-------+------+

# +-------------+
# |      BORO_NM|
# +-------------+
# |    MANHATTAN|
# |       QUEENS|
# |STATEN ISLAND|
# |     BROOKLYN|
# |             |
# |        BRONX|
# +-------------+

crime_df_borough=sqlContext.createDataFrame(data,header)
crime_df_borough.registerTempTable("crime")

#group data by PREM_TYP_DESC(specific description of premises) for each borough
sqlContext.sql("SELECT PREM_TYP_DESC,count(*) FROM crime WHERE BORO_NM='STATEN ISLAND' AND PREM_TYP_DESC<>'' GROUP BY PREM_TYP_DESC").toPandas().to_csv('Staten Island.csv')
sqlContext.sql("SELECT PREM_TYP_DESC,count(*) FROM crime WHERE BORO_NM='BRONX' AND PREM_TYP_DESC<>'' GROUP BY PREM_TYP_DESC").toPandas().to_csv('Bronx.csv')
sqlContext.sql("SELECT PREM_TYP_DESC,count(*) FROM crime WHERE BORO_NM='MANHATTAN' AND PREM_TYP_DESC<>'' GROUP BY PREM_TYP_DESC").toPandas().to_csv('Manhattan.csv')
sqlContext.sql("SELECT PREM_TYP_DESC,count(*) FROM crime WHERE BORO_NM='QUEENS' AND PREM_TYP_DESC<>'' GROUP BY PREM_TYP_DESC").toPandas().to_csv('Queens.csv')
sqlContext.sql("SELECT PREM_TYP_DESC,count(*) FROM crime WHERE BORO_NM='BROOKLYN' AND PREM_TYP_DESC<>'' GROUP BY PREM_TYP_DESC").toPandas().to_csv('Brooklyn.csv')

#group data by KY_CD(three digit offense classification code) for each borough
sqlContext.sql("SELECT KY_CD,count(*) FROM crime WHERE BORO_NM='QUEENS' GROUP BY KY_CD").toPandas().to_csv('Queenscrime.csv')
sqlContext.sql("SELECT KY_CD,count(*) FROM crime WHERE BORO_NM='BRONX' GROUP BY KY_CD").toPandas().to_csv('Bronxcrime.csv')
sqlContext.sql("SELECT KY_CD,count(*) FROM crime WHERE BORO_NM='BROOKLYN' GROUP BY KY_CD").toPandas().to_csv('Brooklyncrime.csv')
sqlContext.sql("SELECT KY_CD,count(*) FROM crime WHERE BORO_NM='STATEN ISLAND' GROUP BY KY_CD").toPandas().to_csv('Staten Islandcrime.csv')
sqlContext.sql("SELECT KY_CD,count(*) FROM crime WHERE BORO_NM='MANHATTAN' GROUP BY KY_CD").toPandas().to_csv('Manhattancrime.csv')


