latlnt = sqlContext.sql("SELECT CMPLNT_NUM,LATITUDE,LONGITUDE FROM crime WHERE LATITUDE != '' AND LONGITUDE != '' ")                                      
latlnt.toPandas().to_csv('latlnt.csv')
