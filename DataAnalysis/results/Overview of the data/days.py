import numpy as np
from numpy import genfromtxt
data = genfromtxt('cmplnt_fr_days_count.csv', delimiter=',', dtype=np.int64)
i = 1
print(data.shape)
mean_cnt = np.mean(data, axis=0)
print(mean_cnt)

text_file = open("Output.txt", "w")

for row in data:
    idx, month, day, year, count = row
    if year > 2005 and year <= 2017:
    	text_file.write('[ new Date( %d, %d, %d), %f ],' % (year, month - 1, day, (-1)*count + mean_cnt[4]) )


# text_file.write("Purchase Amount: %s" % TotalAmount)
text_file.close()