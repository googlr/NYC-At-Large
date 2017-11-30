# open file
import csv
import pandas
csvfile = sc.textFile('/user/cw2661/proj/crime.csv')
crime = csvfile.map(lambda line: next(csv.reader(line.splitlines(), skipinitialspace=True)))
crime.first()
# RDD to DataFrame
header = crime.first()
data = crime.filter(lambda line: line != header)
crime_df = sqlContext.createDataFrame(data, header)
crime_df.registerTempTable("crime")
sqlContext.sql("SELECT * FROM crime").show(10)
# ky_cd aggragreate
ky_classification = sqlContext.sql("SELECT COUNT(*), KY_CD FROM crime GROUP BY KY_CD")
ky_classification.show()
# ky_classification.toPandas().to_csv('ky.csv')

# pd_cd aggragreate
pd_classification = sqlContext.sql("SELECT COUNT(*),PD_CD FROM crime GROUP BY PD_CD")
pd_classification.show()
# pd_classification.toPandas().to_csv('pd.csv')

# Criminal with date
date_classification = sqlContext.sql("SELECT COUNT(*),RPT_DT FROM crime GROUP BY RPT_DT")
date_classification.show()
# date_classification.toPandas().to_csv('date.csv')

# ky_cd with date
ky_date_classification = sqlContext.sql("SELECT COUNT(*),KY_CD, RPT_DT FROM crime GROUP BY RPT_DT, KY_CD")
ky_date_classification.show()
# ky_date_classification.toPandas().to_csv('ky_date.csv')

# pd_cd with date
pd_date_classification = sqlContext.sql("SELECT COUNT(*),PD_CD, RPT_DT FROM crime GROUP BY RPT_DT, PD_CD")
pd_date_classification.show()
# pd_date_classification.toPandas().to_csv('pd_date.csv')

# date with the number of criminal types using pd
pd_date_classification.registerTempTable("pd_date")
pd_date_count = sqlContext.sql("SELECT RPT_DT, COUNT(*) FROM pd_date GROUP BY RPT_DT")
pd_date_count.show()
# pd_date_count.toPandas().to_csv('pd_date_count.csv')

# date with the number of criminal types using ky
ky_date_classification.registerTempTable("ky_date")
ky_date_count = sqlContext.sql("SELECT RPT_DT, COUNT(*) FROM ky_date GROUP BY RPT_DT")
ky_date_count.show()
# ky_date_count.toPandas().to_csv('ky_date_count.csv')

# ky with the number of date it present
ky_num_date_count = sqlContext.sql("SELECT KY_CD, COUNT(*) FROM ky_date GROUP BY KY_CD")
ky_num_date_count.show()
# ky_num_date_count.toPandas().to_csv('ky_num_date_count.csv')

# pd with the number of date it present
pd_num_date_count = sqlContext.sql("SELECT PD_CD, COUNT(*) FROM pd_date GROUP BY PD_CD")
pd_num_date_count.show()
# pd_num_date_count.toPandas().to_csv('pd_num_date_count.csv')

# ky_cd with description
ky_desc = sqlContext.sql("SELECT KY_CD, OFNS_DESC  FROM crime GROUP BY KY_CD, OFNS_DESC")
ky_desc.show()
# ky_desc.toPandas().to_csv('ky_desc.csv')

# pd_cd with description
pd_desc = sqlContext.sql("SELECT PD_CD, PD_DESC  FROM crime GROUP BY PD_CD, PD_DESC")
pd_desc.show()
# pd_desc.toPandas().to_csv('pd_desc.csv')

