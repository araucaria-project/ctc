from ctc.cycle_time_calc import CycleTimeCalc
import asyncio
import logging

logging.basicConfig(level='INFO')

tele = 'jk15'
c = CycleTimeCalc(telescope=tele, base_folder='/data/misc/cycle_time_calc')
c.set_start_rmode(rmode=3)
c.set_current_filter(filter_='u')
prog = 'ZERO seq=21/u/0'
time = c.calc_time(command_dict=prog)
logging.info(f'{tele} {prog} -> time: {time}')

c.reset_time()
c.set_current_filter(filter_='u')
prog = 'DARK seq=21/u/0'
time = c.calc_time(command_dict=prog)
logging.info(f'{tele} {prog} -> time: {time}')

tele = 'zb08'
c = CycleTimeCalc(telescope=tele, base_folder='/data/misc/cycle_time_calc')
c.set_start_rmode(rmode=2)
c.set_current_filter(filter_='u')
prog = 'ZERO seq=11/u/0'
time = c.calc_time(command_dict=prog)
logging.info(f'{tele} {prog} -> time: {time}')

c.reset_time()
c.set_current_filter(filter_='u')
prog = 'DARK seq=11/u/0'
time = c.calc_time(command_dict=prog)
logging.info(f'{tele} {prog} -> time: {time}')

tele = 'wk06'
c = CycleTimeCalc(telescope=tele, base_folder='/data/misc/cycle_time_calc')
c.set_start_rmode(rmode=2)
c.set_current_filter(filter_='u')
prog = 'ZERO seq=11/u/0'
time = c.calc_time(command_dict=prog)
logging.info(f'{tele} {prog} -> time: {time}')

c.reset_time()
c.set_current_filter(filter_='u')
prog = 'DARK seq=11/u/0'
time = c.calc_time(command_dict=prog)
logging.info(f'{tele} {prog} -> time: {time}')