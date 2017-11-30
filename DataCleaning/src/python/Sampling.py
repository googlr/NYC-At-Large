#-*- coding=utf-8 -*-

import pandas as pd
from random import randint
def random_select(data):
    # data is orignial data, n is samples rate.
    df_len = len(data.index)
    number = randint(0, df_len - 1)
    output = pd.DataFrame(columns=list(data))
    output = output.append(data.iloc[[number]])
    return output
action_frame = pd.read_csv('NYPD_Complaint_Data_Historic.csv', sep=',',iterator=True)

print "start"
i = 0
loop = True
while (loop):
    try:
         i = i + 1
         print i
         chunk = action_frame.get_chunk(100)
         # 1/1000 rate
         random_select(chunk).to_csv("crime_sample.csv", mode='a', header=False, index=False)
    except StopIteration:
             loop = False
             print "Iteration is stopped."
print "end"



# chunk = action_frame.get_chunk(1000)
# print(random_select(chunk, 1000))