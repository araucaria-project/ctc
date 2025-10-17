from ctc.cycle_time_train import CycleTimeTrain
from ctc.cycle_time_data_clean import CycleTimeDataClean
import asyncio
import logging

logging.basicConfig(level='INFO')
asyncio.run(CycleTimeDataClean.data_clean_all(base_folder='/data/misc/cycle_time_calc', skip_if_was_today=False))
asyncio.run(CycleTimeTrain.train_all_telesc_all_commands(
    base_folder='/data/misc/cycle_time_calc', skip_if_was_today=False
))





