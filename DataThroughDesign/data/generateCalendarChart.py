import numpy as np
from numpy import genfromtxt
data = genfromtxt('cmplnt_fr_days_count_combined.csv', delimiter=',', dtype=np.int64)
i = 1
print(data.shape)
mean_cnt = np.mean(data, axis=0)
print(mean_cnt)



text_file = open("calenderChartInput.txt", "w")

val = np.sort( data[:,4] )

import matplotlib.pyplot as plt
plt.plot(val)
plt.ylabel('some numbers')
plt.show()

val_file = open("count.txt","w")

for row in data:
    idx, month, day, year, count = row
    if year > 2005 and year <= 2017:
    	# text_file.write('[ new Date( %d, %d, %d), %f ],' % (year, month - 1, day, (-1)*count + mean_cnt[4]) )
    	text_file.write('[ new Date( %d, %d, %d), %f ],\n' % (year, month - 1, day, count) )
    	

for i in val:
	val_file.write('%d,' % i)


# text_file.write("Purchase Amount: %s" % TotalAmount)
text_file.close()
val_file.close()