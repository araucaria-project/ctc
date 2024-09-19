from typing import Dict, Any, List
from collections import OrderedDict
import numpy as np
from sklearn.linear_model import LinearRegression
from ob.planrunner.cycle_time_calc.abstract_cycle_time import AbstractCycleTime
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
import logging


logger = logging.getLogger(__name__.rsplit('.')[-1])


class CycleTimeTrain(AbstractCycleTime):
    """
    This class represent cycle time data train
    """

    @staticmethod
    def build_array(data: List[Dict[str, Any]], xory: str) -> np.array or None:

        if xory == 'x':
            w = CycleTimeTrain.X_COLUMNS
        elif xory == 'y':
            w = CycleTimeTrain.Y_COLUMN
        else:
            w = []
        l1 = []
        for n in data:
            l2 = []
            for p, q in n.items():
                if p in w:
                    l2.append(q)
            l1.append(l2)

        return np.asarray(l1)

    @staticmethod
    def train_data_command(telescope: str, command: str, base_folder: str) -> None:
        logger.debug(f'Train telelescope: {telescope} command: {command}')
        min_record_to_train = 10
        data = CycleTimeTrain.read_file(base_folder,
                                        CycleTimeTrain.clean_data_file_name(telescope=telescope, command=command))
        parsed_data = CycleTimeTrain.parse_data(data=data)
        data_x = CycleTimeTrain.build_array(data=parsed_data, xory='x')
        data_y = CycleTimeTrain.build_array(data=parsed_data, xory='y')
        if (data_x is not None) and (data_y is not None) and (len(data_x) >= min_record_to_train):
            param = CycleTimeTrain.train(data_x=data_x, data_y=data_y)
            CycleTimeTrain.save_train_param_to_file(param=param, telescope=telescope,
                                                    command=command, base_folder=base_folder)

    @staticmethod
    def save_train_param_to_file(param: Dict[str, Any], telescope: str, command: str, base_folder: str) -> None:
        c = {}
        for n, m in enumerate(CycleTimeTrain.X_COLUMNS, start=0):
            c[m] = param['coef'][n]
        dat = OrderedDict({
            'train_utc_time_stamp': str(CycleTimeTrain.time_stamp().isoformat()),
            'coef': c,
            'intercept': param['intercept'],
            'r2': param['r2']
        })

        CycleTimeTrain.add_to_file(data=CycleTimeTrain.encode_data(data=dat),
                                   folder=base_folder,
                                   file_name=CycleTimeTrain.train_param_file_name(telescope=telescope,
                                                                                  command=command))
        CycleTimeTrain.add_to_file(data=CycleTimeTrain.encode_data(data=dat),
                                   folder=base_folder,
                                   file_name=CycleTimeTrain.train_param_last_file_name(telescope=telescope,
                                                                                       command=command),
                                   mode='w')
        logger.debug(f'Save train parameters for telescope:{telescope} command:{command}')

    @staticmethod
    def train(data_x: np.array, data_y: np.array) -> Dict[str, Any]:
        model = LinearRegression()
        x_train, x_test, y_train, y_test = train_test_split(data_x, data_y, random_state=123, test_size=0.3)
        model.fit(x_train, y_train)
        y_pred = model.predict(x_test)
        param = {}
        param['r2'] = r2_score(y_test, y_pred)
        param['coef'] = model.coef_[0]
        param['intercept'] = model.intercept_[0]
        return param

    @staticmethod
    def train_all_telesc_all_commands(base_folder: str) -> None:
        logger.info(f'Train all telescopes and all commands type')
        for t in CycleTimeTrain.get_list_telesc(file_type='clean_data', base_folder=base_folder):
            for c in CycleTimeTrain.get_list_commands(telescope=t, file_type='clean_data', base_folder=base_folder):
                CycleTimeTrain.train_data_command(telescope=t, command=c, base_folder=base_folder)
