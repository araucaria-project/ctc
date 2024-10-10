import time
from typing import Dict, Any, List, Tuple
from collections import OrderedDict
import numpy as np
from sklearn.linear_model import LinearRegression
from ctc.abstract_cycle_time import AbstractCycleTime
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
import logging
from ctc.iter_async import AsyncListIter, AsyncDictItemsIter, AsyncEnumerateIter


logger = logging.getLogger(__name__.rsplit('.')[-1])


class CycleTimeTrain(AbstractCycleTime):
    """
    This class represent cycle time data train
    """

    _MIN_DATA_RECORDS_TO_TRAIN = 10

    @staticmethod
    async def _build_array(data: List[Dict[str, Any]], xory: str) -> np.array or None:

        if xory == 'x':
            w = CycleTimeTrain._X_COLUMNS
        elif xory == 'y':
            w = CycleTimeTrain._Y_COLUMN
        else:
            w = []
        l1 = []
        async for n in AsyncListIter(data):
            l2 = []
            async for p, q in AsyncDictItemsIter(n):
                if p in w:
                    l2.append(q)
            l1.append(l2)

        return np.asarray(l1)

    @staticmethod
    async def calc_dome_mount_average_dist(parsed_data: List[Dict[str, Any]]) -> Tuple[float, float]:
        li_d = []
        li_m = []
        async for record in AsyncListIter(parsed_data):
            try:
                li_d.append(record['dome_distance'])
                li_m.append(record['mount_distance'])
            except (LookupError, TypeError):
                pass
        dome_avr_dist = np.mean(np.array([li_d]))
        mount_avr_dist = np.mean(np.array([li_m]))
        return dome_avr_dist, mount_avr_dist

    @staticmethod
    async def train_data_command(telescope: str, command: str, base_folder: str) -> None:
        logger.debug(f'Train telelescope: {telescope} command: {command}')
        min_record_to_train = CycleTimeTrain._MIN_DATA_RECORDS_TO_TRAIN
        data = CycleTimeTrain.read_file(
            base_folder, CycleTimeTrain.clean_data_file_name(telescope=telescope, command=command)
        )
        parsed_data = CycleTimeTrain._parse_data(data=data)
        dome_avr_dist, mount_avr_dist = await CycleTimeTrain.calc_dome_mount_average_dist(parsed_data=parsed_data)
        data_x = await CycleTimeTrain._build_array(data=parsed_data, xory='x')
        data_y = await CycleTimeTrain._build_array(data=parsed_data, xory='y')
        if (data_x is not None) and (data_y is not None) and (len(data_x) >= min_record_to_train):
            param = await CycleTimeTrain._train(data_x=data_x, data_y=data_y)
            param['dome_average_dist'] = dome_avr_dist
            param['mount_average_dist'] = mount_avr_dist
            await CycleTimeTrain._save_train_param_to_file(
                param=param, telescope=telescope, command=command, base_folder=base_folder
            )

    @staticmethod
    async def _save_train_param_to_file(param: Dict[str, Any], telescope: str, command: str, base_folder: str) -> None:
        c = {}
        async for n, m in AsyncEnumerateIter(CycleTimeTrain._X_COLUMNS):
            c[m] = param['coef'][n]
        dat = OrderedDict({
            'train_utc_time_stamp': str(CycleTimeTrain._time_stamp().isoformat()),
            'coef': c,
            'intercept': param['intercept'],
            'r2': param['r2'],
            'dome_average_dist': param['dome_average_dist'],
            'mount_average_dist': param['mount_average_dist']
        })

        CycleTimeTrain._add_to_file(
            data=CycleTimeTrain._encode_data(data=dat),
            folder=base_folder,
            file_name=CycleTimeTrain.train_param_file_name(telescope=telescope, command=command)
        )
        CycleTimeTrain._add_to_file(
            data=CycleTimeTrain._encode_data(data=dat),
            folder=base_folder,
            file_name=CycleTimeTrain.train_param_last_file_name(telescope=telescope, command=command),
            mode='w'
        )
        logger.debug(f'Save train parameters for telescope:{telescope} command:{command}')

    @staticmethod
    async def _train(data_x: np.array, data_y: np.array) -> Dict[str, Any]:
        model = LinearRegression()
        x_train, x_test, y_train, y_test = train_test_split(data_x, data_y, random_state=123, test_size=0.3)
        model.fit(x_train, y_train)
        y_pred = model.predict(x_test)
        return {'r2': r2_score(y_test, y_pred), 'coef': model.coef_[0], 'intercept': model.intercept_[0]}

    @staticmethod
    async def train_all_telesc_all_commands(base_folder: str) -> None:
        t_0 = time.time()
        logger.info(f'Train all telescopes and all commands type')
        async for t in AsyncListIter(CycleTimeTrain.get_list_telesc(file_type='clean_data', base_folder=base_folder)):
            logger.info(f'Data train for telescope: {t}')
            async for c in AsyncListIter(CycleTimeTrain.get_list_commands(
                    telescope=t, file_type='clean_data', base_folder=base_folder
            )):
                await CycleTimeTrain.train_data_command(telescope=t, command=c, base_folder=base_folder)
        logger.info(f'Training done in {time.time() - t_0:.1f}')
