import logging
import re
from abc import ABC
import os
from typing import Dict, Any, List
import datetime
import math
import numpy as np
import json


logger = logging.getLogger(__name__.rsplit('.')[-1])


class AbstractCycleTime(ABC):
    """
    This class represent abstract time data
    """

    _FILE_NAMES = {
        'raw_data': 'raw_data.txt', 'clean_data': 'clean_data.txt',
        'night_verif': 'night_verif.txt', 'train_param': 'train_param.txt',
        'train_param_last': 'train_param_last.txt'
    }

    _DEFAULT_RM_MODES_MHZ = {
        "zb08": [5, 3, 1, 0.05],
        "jk15": [4, 2, 1, 1, 0.1, 0.1],
        "wk06": [5, 3, 1, 0.05],
        "dev": [1, 1, 1, 1, 1, 1, 1],
        "sim": [1, 1, 1, 1, 1, 1, 1],
        "default": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]}

    _DATA_CLEAN_ORDER = [
        'night_id', 'command_name', 'start_utc_date_stamp', 'cycle_time', 'dome_distance',
        'mount_distance', 'exposure_time_sum', 'filter_changes', 'rmmode_expno', 'dither_expno'
    ]
    _X_COLUMNS = _DATA_CLEAN_ORDER[4:]
    _Y_COLUMN = _DATA_CLEAN_ORDER[3]
    _POSSIBLE_DITHER = ['OBJECT', 'SNAP']
    _NO_TRAIN_COMMANDS = ['WAIT']

    @staticmethod
    def get_list_telesc(file_type: str, base_folder: str) -> List[str]:
        tel_lst = []
        try:
            l_dir = os.listdir(base_folder)

            for n in l_dir:
                if AbstractCycleTime._FILE_NAMES[file_type] in n and re.fullmatch(pattern=r'\w\w..?_.*.txt', string=n):
                    tel = n.split('_')[0]
                    if tel not in tel_lst:
                        tel_lst.append(tel)
        except (FileExistsError, FileNotFoundError):
            pass
        return tel_lst

    @staticmethod
    def get_list_commands(telescope: str, file_type: str, base_folder: str) -> List[str]:
        com_lst = []
        try:
            l_dir = os.listdir(base_folder)
            for n in l_dir:
                if (AbstractCycleTime._FILE_NAMES[file_type] in n) and (telescope in n):
                    com_lst.append(n.split('_')[1])
        except (FileExistsError, FileNotFoundError):
            pass
        return com_lst

    @staticmethod
    def _encode_data(data: Dict or List) -> str:
        return json.dumps(data)

    @staticmethod
    def _decode_data(data: str) -> Dict or List:
        return json.loads(data)

    @staticmethod
    def raw_file_name(telescope: str) -> str:
        return f'{telescope}_{AbstractCycleTime._FILE_NAMES["raw_data"]}'

    @staticmethod
    def night_verif_file_name(telescope: str)  -> str:
        return f'{telescope}_{AbstractCycleTime._FILE_NAMES["night_verif"]}'

    @staticmethod
    def clean_data_file_name(telescope: str , command: str) -> str:
        return f'{telescope}_{command}_{AbstractCycleTime._FILE_NAMES["clean_data"]}'

    @staticmethod
    def train_param_file_name(telescope: str, command: str) -> str:
        return f'{telescope}_{command}_{AbstractCycleTime._FILE_NAMES["train_param"]}'

    @staticmethod
    def train_param_last_file_name(telescope: str, command: str) -> str:
        return f'{telescope}_{command}_{AbstractCycleTime._FILE_NAMES["train_param_last"]}'

    @staticmethod
    def _parse_data(data: str) -> List:
        ret = []
        s = data.split('\n')
        for n in s:
            if len(n) > 1:
                ret.append(AbstractCycleTime._decode_data(n))
        return ret

    @staticmethod
    def _mk_dirs(base_folder: str) -> None:
        try:
            if not os.path.exists(base_folder):
                os.makedirs(base_folder, exist_ok=True)
        except PermissionError:
            logger.error(f'Can not create folder {base_folder} - no permission, '
                         f'please create folder manually and grant permissions')

    @staticmethod
    def _add_to_file(data: str, folder: str, file_name: str, mode: str = "a") -> None:
        try:
            f = open(os.path.join(folder, file_name), mode, encoding='utf-8')
            f.write(f'{data}')
            f.write('\n')
            f.close()
        except (FileExistsError, FileNotFoundError):
            logger.error(f'File {file_name} can not be reached')
            return None

    @staticmethod
    def read_file(folder: str, file_name: str) -> str or None:
        try:
            f = open(os.path.join(folder, file_name), "r", encoding='utf-8')
            dat = f.read()
            f.close()
            logger.debug(f'File read')
            return dat
        except (FileExistsError, FileNotFoundError):
            logger.error(f'File {file_name} can not be reached')
            return None

    @staticmethod
    def _time_stamp() -> datetime:
        return datetime.datetime.utcnow()

    @staticmethod
    def _rm_mode(record: Dict[str, Any]) -> int:
        try:
            return int(record['readoutmode'])
        except KeyError:
            return 1

    @staticmethod
    def _rm_mode_inverse_mhz(rm_mode: int, telescope: str, rm_modes: Dict[List[float]] = None) -> float or int or None:
        if not rm_modes:
            rm_modes = AbstractCycleTime._DEFAULT_RM_MODES_MHZ
        if rm_modes is not None:
            if telescope in rm_modes.keys():
                if len(rm_modes[telescope]) - 1 >= rm_mode:
                    try:
                        rm_mode_val = rm_modes[telescope][rm_mode]
                        return 1/rm_mode_val
                    except IndexError:
                        logger.warning('Readoutmodes not match readoutmode - rm mode default returned')
                        rm_mode_val = rm_modes['default'][rm_mode]
                        return 1/rm_mode_val
                    except ZeroDivisionError:
                        logger.warning('Readoutmode value not right - rm mode default returned')
                        rm_mode_val = rm_modes['default'][rm_mode]
                        return 1/rm_mode_val
                else:
                    logger.warning('Readoutmode value out of index - rm mode default returned')
                    rm_mode_val = rm_modes['default'][rm_mode]
                    return 1 / rm_mode_val
            else:
                logger.warning(f'No readoutmodes for {telescope} - rm mode default returned')
                rm_mode_val = rm_modes['default'][rm_mode]
                return 1/rm_mode_val
        else:
            logger.warning('Can not read readoutmode inverse MHz - 1 returned')
            return 1

    @staticmethod
    def _dither(record: Dict[str, Any]) -> int:
        if record['dither'] == 'True':
            return 1
        else:
            return 0

    @staticmethod
    def _az_distance(start: float, end: float) -> float:
        """
        Method giving back shortest distance in azimuth
        :param start: start position of azimuth
        :param end: end position of azimuth
        :return: shortest distance in degrees
        """
        if abs(start - end) > 180.0:
            if start > end:
                ret = abs(start - (end + 360.0))
            else:
                ret = abs(end - (start + 360.0))
        else:
            ret = abs(start - end)
        logger.debug(f'Az_start:{start}, az_end:{end}, az_distance:{ret}')
        return ret

    @staticmethod
    def _vector(alt: float, az: float) -> np.array:
        """
        Method builds vector from alt az
        :param alt: alt degrees
        :param az: az degrees
        :return: numpy vector
        """
        az = math.radians(az)
        alt = math.radians(alt)
        x = math.cos(az) * math.cos(alt)
        z = math.sin(az) * math.cos(alt)
        y = math.sin(alt)
        ret = np.array([x, y, z])
        logger.debug(f'Alt:{alt}, az:{az}, vector:{ret}')
        return ret

    @staticmethod
    def _alt_az_distance(start_alt: float, end_alt: float, start_az: float, end_az: float) -> float:
        """
        Method calculate angle between altaz positions
        :param start_alt: start alt degrees
        :param end_alt: end alt degrees
        :param start_az: start az degrees
        :param end_az: end az degrees
        :return: distance in degrees
        """
        v1 = AbstractCycleTime._vector(alt=start_alt, az=start_az)
        v2 = AbstractCycleTime._vector(alt=end_alt, az=end_az)
        if np.array_equal(v1, v2):
            ret = 0.0
        else:
            ret = math.degrees(math.acos(np.dot(v1, v2)))
        logger.debug(f'Alt_start:{start_alt}, alt_end:{end_alt}, az_start:{start_az},'
                     f' az_end:{end_az}, altaz_distance:{ret}')
        return ret

    @staticmethod
    def _exposure_time_sum(record: Dict[str, Any]) -> float:
        ret = 0
        if 'kwargs' in record.keys():
            if 'seq' in record['kwargs']:
                seq_res = AbstractCycleTime.solve_multiple_seq(seq_string=record['kwargs']['seq'])
                s = seq_res.split(',')
                for n in s:
                    z = n.split('/')
                    if z[2] == 'a':
                        t = 5
                    else:
                        t = z[2]
                    try:
                        ret += int(z[0]) * float(t)
                    except (ValueError, IndexError):
                        ret += 1
        logger.debug(f'Record:{record}, exposure_time_sum:{ret}')
        return ret

    @staticmethod
    def _seq_number(record: Dict[str, Any]) -> int:
        ret = 0
        if 'kwargs' in record.keys():
            if 'seq' in record['kwargs']:
                seq_res = AbstractCycleTime.solve_multiple_seq(seq_string=record['kwargs']['seq'])
                s = seq_res.split(',')
                for n in s:
                    ret += 1
        logger.debug(f'Record:{record}, seq_number:{ret}')
        return ret

    @staticmethod
    def _multiple_seq(seq_string: str):
        # TODO boilerplate - rethink
        spl_x = seq_string.split('x')
        repeat = int(spl_x[0])
        se = spl_x[1][1:-1]
        seq_res = ''
        for n in range(0, repeat):
            seq_res += f'{se},'
        return seq_res[:-1]

    @staticmethod
    def solve_multiple_seq(seq_string: str) -> str:
        if re.fullmatch(pattern=r'\d*x\(.*\)', string=seq_string):
            return AbstractCycleTime._multiple_seq(seq_string)
        else:
            return seq_string

    @staticmethod
    def _exposure_number(record: Dict[str, Any]) -> int:
        ret = 0
        if 'kwargs' in record.keys():
            if 'seq' in record['kwargs']:
                seq_res = AbstractCycleTime.solve_multiple_seq(seq_string=record['kwargs']['seq'])
                s = seq_res.split(',')
                for n in s:
                    try:
                        ret += int(n.split('/')[0])
                    except (ValueError, IndexError):
                        ret += 1
        logger.debug(f'Record:{record}, seq_number:{ret}')
        return ret

    @staticmethod
    def _last_filter(record: Dict[str, Any]) -> str:
        ret = 0
        if 'kwargs' in record.keys():
            if 'seq' in record['kwargs']:
                s = record['kwargs']['seq'].split(',')
                len_s = len(s)
                for w, n in enumerate(s, start=1):
                    if w == len(s):
                        z = n.split('/')
                        ret = z[1]
        logger.debug(f'Record:{record}, last filter is:{ret}')
        return ret

    @staticmethod
    def _filter_changes(record: Dict[str, Any]) -> int:
        ret = 0
        f = record['filter_pos']
        if 'kwargs' in record.keys():
            if 'seq' in record['kwargs']:
                seq_res = AbstractCycleTime.solve_multiple_seq(seq_string=record['kwargs']['seq'])
                s = seq_res.split(',')
                for n in s:
                    z = n.split('/')
                    if f != z[1]:
                        ret += 1
                    f = z[1]
        logger.debug(f'Record:{record}, filter_changes:{ret}')
        return ret

    @staticmethod
    def _dither_on(command_dict: Dict[str, Any]) -> int:
        """
        Do dithering or not.
        :return: bool
        """
        if 'kwargs' in command_dict.keys():
            kw = command_dict['kwargs']
        else:
            kw = {}
        if command_dict['command_name'] == 'SKYFLAT':
            if 'dither' not in kw.keys():
                return 1
            elif 'dither' in kw.keys() and kw['dither'] != 'off':
                return 1
            elif 'dither' in kw.keys() and kw['dither'] == 'off':
                return 0
            else:
                return 0

        elif command_dict['command_name'] in AbstractCycleTime._POSSIBLE_DITHER:
            if 'dither' not in kw.keys():
                return 0
            elif 'dither' in kw.keys() and kw['dither'] != 'off':
                return 1
            elif 'dither' in kw.keys() and kw['dither'] == 'off':
                return 0
            else:
                return 0
        else:
            return 1
