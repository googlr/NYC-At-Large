# open file
import csv

csvfile = sc.textFile('/user/cl4062/crime.csv')
crime = csvfile.map(lambda line: next(csv.reader(line.splitlines(), skipinitialspace=True)))

# RDD to DataFrame
header = crime.first()
data = crime.filter(lambda line: line != header)
crime_df_invalid_value=sqlContext.createDataFrame(data,header)
crime_df_invalid_value.registerTempTable("crime_invalid_value")

#find invalid value for each column
def invalidEachCol():
	num=[]
	for x in range(0,24):
		num_col=sqlContext.sql("SELECT count(*) FROM crime_invalid_value WHERE "+attributes[x]+"='N/A' or "+attributes[x]+"='NULL' or "+attributes[x]+"='USPECIFIED' or "+attributes[x]+"='NULL'").collect()[0][0]
		num.append(num_col)
	return num
invalid_num=invalidEachCol()


#replace invalid value with empty value
def removeInvalidValue(row):
	for x in range(0,24):
		if row[x]=='N/A' or row[x]=='NULL' or row[x]=='UNSPECIFIED' or row[x]=='TBA':
			row[x]=''
	return row
data_cleaned=data.map(lambda row:removeInvalidValue(row))


#add a new attribute
attributes=crime.first()
attributes.append('emptyValue')
#find number of empty values in row
def addCol(row):
	count=0
	for x in range(0,24):
		if row[x]='':
			count+=1
	newrow=list(row)
	newrow.append(count)
	return newrow
	
#add a new attribute(number of empty value in this tuple) for each row
newdata=data_cleaned.map(lambda row:addCol(row)) 

crime_df = sqlContext.createDataFrame(newdata, attributes)
crime_df.registerTempTable("crime")
sqlContext.sql("SELECT * FROM crime").show(10)


#average number of empty value 
sqlContext.sql("SELECT avg(emptyVaue) FROM crime").show()
#total number of empty value  
sqlContext.sql("SELECT sum(emptyVaue) FROM crime").show()

#find total number of empty values for each column
def emptyEachCol():
	num=[]
	for x in range(0,24):
		count=sqlContext.sql("SELECT count(*) FROM crime WHERE "+attributes[x]+"=''").collect()[0][0]
		num.append(count)
	return num
empty_num=emptyEachCol()

