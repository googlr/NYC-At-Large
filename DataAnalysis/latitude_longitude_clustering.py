from sklearn.cluster import KMeans
import numpy as np
import timeit                                                               

# Load the data before running the code below

latlnt = sqlContext.sql("SELECT CMPLNT_NUM,LATITUDE,LONGITUDE FROM crime WHERE LATITUDE != '' AND LONGITUDE != '' ")                                      
# latlnt.toPandas().to_csv('latlnt.csv')

X = np.array(latlnt.select(latlnt.LATITUDE,latlnt.LONGITUDE).collect())
# X.shape
# kmeans = KMeans(n_clusters=100, random_state=0).fit(X)

def k_means():
     start = timeit.default_timer()
     kmeans = KMeans(n_clusters=100, random_state=0).fit(X)
     print(kmeans.cluster_centers_)
     stop = timeit.default_timer()
     print(stop - start)
     return kmeans

    
kmeans = k_means()
kmeans_labels = kmeans.labels_
kmeans_cluster_centers = kmeans.cluster_centers_
