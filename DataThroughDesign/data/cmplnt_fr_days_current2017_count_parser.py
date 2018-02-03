import numpy as np
from numpy import genfromtxt
data = genfromtxt('cmplnt_fr_days_current2017_count.csv', delimiter=',')
i = 1
print(data.shape)

text_file = open("Output.txt", "w")


row, col = data.shape
for i in range(0, row):
	idx, count, month, day, year = data[i]
	text_file.write('%d,%02d,%02d,%d,%d\n' % (idx + 4018, month, day, year, count) )

# mean_cnt = np.mean(data, axis=0)
# print(mean_cnt)


# for row in data:
#     idx, month, day, year, count = row
#     if year > 2005 and year <= 2017:
#     	text_file.write('[ new Date( %d, %d, %d), %f ],' % (year, month - 1, day, (-1)*count + mean_cnt[4]) )


# # text_file.write("Purchase Amount: %s" % TotalAmount)
text_file.close()