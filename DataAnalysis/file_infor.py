import pandas as pd

# user_f=pd.read_csv('NYPD_Complaint_Data_Historic.csv', sep=',')
# 5580035
print "start reading"

action4_f = pd.read_csv('NYPD_Complaint_Data_Historic.csv', sep=',',iterator=True)
print "ok"

frame_len = 0
loop = True
while (loop):
    try:
         frame_chunk = action4_f.get_chunk(100000)
         frame_len = frame_len + len(frame_chunk)
    except StopIteration:
             loop = False
             print "Iteration is stopped."

print frame_len