import asyncio

from ctc.cycle_time_data_clean import CycleTimeDataClean



# asyncio.run(CycleTimeDataClean.data_clean_all(
#     base_folder='/home/mirk/Desktop/misc/cycle_time_calc/te1/'
# ))


l = []
with open('/home/mirk/Desktop/misc/cycle_time_calc/old2/wk06_OBJECT_clean_data.txt') as f:
    f = f.read()

for n in f.split('\n'):
    if n != '':
        l.append(n)

d = []
with open('/home/mirk/Desktop/misc/cycle_time_calc/old1/wk06_OBJECT_clean_data.txt') as f:
    f = f.read()

for n in f.split('\n'):
    if n != '':
        if n not in l:
            d.append(n)

print(d)