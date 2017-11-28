# Login to Dumbo

# Set up environment:
# module load python/gnu/3.4.4
# export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
# export PYTHONHASHSEED=0
# export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0

# Install pygeocoder package
# $ pip install pygeocoder

# pyspark

import csv
import time
from pygeocoder import Geocoder

csvfile = sc.textFile('/user/xg626/NYPD_Complaint_Data_Historic.csv')

crime = csvfile.map(lambda line: next(csv.reader(line.splitlines(), skipinitialspace=True)))

header = crime.first()
data = crime.filter(lambda line: line != header)

# header
data.take(2)

# refer to: https://docs.python.org/3/library/time.html#time.strptime
def validate_date( s ):
    try:
        valid_date = time.strptime( s, '%m/%d/%Y')
        return True
	except ValueError:
		return False 

def validate_time( s ):
    try:
        valid_date = time.strptime( s, '%H:%M:%S')
        return True
	except ValueError:
		return False

# Validate x[1] = CMPLNT_FR_DT and x[5] = RPT_DT
# validDateData = data.filter(lambda x: validate_date(x[1])==True and validate_date(x[5])==True )
valid_prt_dt = data.filter(lambda x: validate_date(x[5])==True )

# Compare the result of data and validDateData
data.count() 
# 5580035
valid_prt_dt.count() 
# 5580035





# Following code need further validation

# Reverse Geocoding
# Refer to: https://chrisalbon.com/python/geocoding_and_reverse_geocoding.html
#			https://bitbucket.org/xster/pygeocoder/wiki/Home
#			https://developers.google.com/maps/documentation/geocoding/start?csw=1#ReverseGeocoding
# Convert longitude and latitude to a location
geo_address = []
geo_street_number = []
geo_route = []
geo_locality = []
geo_administrative_area_level_2 = []
geo_administrative_area_level_1 = []
geo_country = []
geo_postal_code = []
def reverse_geocoding(lat, lng):
	# "results" contains an array of geocoded address information and geometry information.
	# Generally, only one entry in the "results" array is returned for address lookups,
	# though the geocoder may return several results when address queries are ambiguous.
	results = Geocoder.reverse_geocode(float(lat), float(lng))
	# (if applicable)
	geo_address.append( results[0] )
	geo_street_number.append( results[0].street_number )
	geo_route.append( results[0].route )
	geo_locality.append( results[0].locality )
	geo_administrative_area_level_2.append( results[0].administrative_area_level_2 )
	geo_administrative_area_level_1.append( results[0].administrative_area_level_1 )
	geo_country.append( results[0].country )
	geo_postal_code.append( results[0].postal_code )
	return True


data_with_lat_lng = data.filter(lambda x: x[21] != '' and x[22] != '').filter(lambda x: reverse_geocoding(x[21], x[22])).collect()
# Append address to dataframe



