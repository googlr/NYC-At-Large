from sklearn.cluster import KMeans
import numpy as np
import timeit                                                               
import csv
import pandas

# csvfile = sc.textFile('/user/cw2661/proj/crime.csv')
csvfile = sc.textFile('/user/xg626/NYPD_Complaint_Data_Historic.csv')
crime = csvfile.map(lambda line: next(csv.reader(line.splitlines(), skipinitialspace=True)))

# RDD to DataFrame
header = crime.first()
data = crime.filter(lambda line: line != header)
crime_df = sqlContext.createDataFrame(data, header)
crime_df.registerTempTable("crime")
# Load the data before running the code below

latlnt = sqlContext.sql("SELECT CMPLNT_NUM,LATITUDE,LONGITUDE FROM crime WHERE LATITUDE != '' AND LONGITUDE != '' ")                                      
# latlnt.toPandas().to_csv('latlnt.csv')

X = np.array(latlnt.select(latlnt.LATITUDE,latlnt.LONGITUDE).collect())
# X.shape
# kmeans = KMeans(n_clusters=100, random_state=0).fit(X)

def k_means():
     start = timeit.default_timer()
     kmeans = KMeans(n_clusters=100, random_state=0).fit(X)
     # print(kmeans.cluster_centers_)
     stop = timeit.default_timer()
     print(stop - start)
     return kmeans

    
kmeans = k_means()
kmeans_labels = kmeans.labels_
kmeans_cluster_centers = kmeans.cluster_centers_
np.savetxt('kmeans_labels.out', kmeans_labels, delimiter=',')   # X is an array
np.savetxt('kmeans_cluster_centers.out', kmeans_cluster_centers, delimiter=',')   # X is an array
