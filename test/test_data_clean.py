import asyncio

from ctc.cycle_time_data_clean import CycleTimeDataClean



asyncio.run(CycleTimeDataClean.data_clean_all(
    base_folder='/home/mirk/Desktop/misc/cycle_time_calc/old1/'
))