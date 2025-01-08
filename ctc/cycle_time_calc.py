from typing import Dict, Any, List, Union
import logging
from ctc.abstract_cycle_time import AbstractCycleTime
import datetime
from pyaraucaria.coordinates import ra_dec_2_az_alt, ra_to_decimal, dec_to_decimal
from pyaraucaria.ephemeris import calculate_sun_rise_set
from pyaraucaria.obs_plan.obs_plan_parser import ObsPlanParser
import time


logger = logging.getLogger(__name__.rsplit('.')[-1])


class CycleTimeCalc(AbstractCycleTime):
    """
    This class represent cycle time calculator (CTC).
    CTC base on machine learning module.
    To prepare proper parameters, CTC need steps:
    1. CycleTimeDataBuild is implemented inside plan runner, mostly in Command and NightPlan classes, to get all need
     data. After collection, it saves data to files (every telescope in different file).
    2. CycleTimeDataClean is run to select, clean, and distribute data from raw data to
     separate files. It also has mechanism to do not take data from same nights, to do not double operations.
    3. CycleTimeTrain is machine learning module built on linear regression. Parameters used to training are selected
     with care and supported by analyses. To aloud to train data, it has to be at least 10 records.
     If there is more data, the parameters will be more accurate.
    4. CycleTimeCalc is main class, used to calculate cycle time. Calculator use trained parameters prepared from
     previous observations. If there is no parameters to use, calculator backs 0.0 time length. For WAIT command
     calculator backs time strait, without any other calc. Calculator sets start_time at time when instance is call,
     but it can be changed by set_start_time.To make calculation more accurate, please also use
     set_observatory_location (default is oca), set_epoch (default is 2000), set_start_rmode (default is 0),
     set_telescope_start_az_alt (default is 0 70) and set_dome_start_az (default is 0). If operations are below alt
      limit (and get 0.0 time because of skipping) please set set_skipping to True.
     You can also change alt limit by alt_limit method (default is set to 15.0).
     Example:
         c = CycleTimeCalc('dev')
         c.set_telescope_start_az_alt(az=120.0, alt=60.0)
         c.set_skipping(skipping=True)
         com_1 = {'command_name': 'OBJECT', 'kwargs': {'seq': '12/Red/1,3/Green/3,2/Blue/5', 'az': 12.0, 'alt':70.0}}
         c.calc_time(command_dict=com_1) --> you get time for com_1
         com_2 = {'command_name': 'WAIT', 'kwargs': {'wait': 40}}
         c.calc_time(command_dict=com_2) --> you get time for com_2
         c.time_lenght_min() --> you get time lenght for all operations (full cycle) in minutes
         c.finnish_time_utc() --> you get utc date_time when program will finish
    """

    USE_OBJECT_PARAMS_IN = ['DARK', 'ZERO', 'SNAP']

    def __init__(self, telescope: str, base_folder: str, tpg: bool = False) -> None:
        self.available_param: Dict[str, Dict[str, Any]] = {}
        self._rmode: int = 0
        self._current_filter: str = ''
        self._alt_mount: float = 70.0
        self._az_mount: float = 0.0
        self._az_dome: float = 0.0
        self.telescope = telescope
        self.base_folder = base_folder
        self._time_length: float = 0.0
        self._epoch: str = '2000'
        self._skipping: bool = False
        self._alt_limit: float = 15.0
        self._observatory_location: Dict[str, Any] = {
            'latitude': -24.598056, 'longitude': -70.196389, 'elevation': 2817
        }
        self._start_time: datetime.datetime = datetime.datetime.utcnow()
        self._time_length_list: List[float] = []
        self._mk_dirs(self.base_folder)
        self._get_params()
        self._set_rm_modes: Dict[str, List[float]] or None = None
        self.tpg = tpg
        super().__init__()

    def set_observatory_location(self, location: Dict[str, Any]) -> None:
        """
        Set observatory location, default is OCM location.
        :param location: dict of location, like: {'latitude': -24.598056, 'longitude': -70.196389, 'elevation': 2817}
        """
        self._observatory_location = location

    def set_epoch(self, epoch: str) -> None:
        """
        Set epoch for ra dec coordinates, default: '2000'
        :param epoch: epoch in str format, example: '2000'
        """
        self._epoch = epoch

    def set_alt_limit(self, alt_limit: float) -> None:
        """
        Set lower alt limit, with is used to skip calculation for operations below this limit.
        :param alt_limit: Lower alt limit in deg (float).
        :return: None
        """
        self._alt_limit = alt_limit

    def set_skipping(self, skipping: bool) -> None:
        """
        Set skip calculation for operations below alt limit.
        :param skipping: True / False (bool).
        :return: None
        """
        self._skipping = skipping

    @property
    def _avialable_param_telesc(self) -> Dict[str, Any] or None:
        if self.telescope in self.available_param.keys():
            return self.available_param[self.telescope]
        else:
            return None

    @property
    def _avialable_param_telesc_commands(self) -> List[str]:
        li = []
        if self._avialable_param_telesc:
            for ke, val in self._avialable_param_telesc.items():
                li.append(ke)
        for n in CycleTimeCalc._NO_TRAIN_COMMANDS:
            if n in li:
                li.remove(n)
        return li

    def _get_params(self) -> None:
        for t in CycleTimeCalc.get_list_telesc(file_type='train_param_last', base_folder=self.base_folder):
            self.available_param[t] = {}
            for co in CycleTimeCalc.get_list_commands(
                    telescope=t, file_type='train_param_last', base_folder=self.base_folder):
                data = self.get_last_params(telescope=t, command=co)
                if data:
                    self.available_param[t][co] = data

    def get_last_params(self, telescope: str, command: str) -> Dict[str, Any]:
        """
        Method returns last trained parameters for selected telescope and command.
        :param telescope: Telescope id (str).
        :param command: Command name (str)
        :return: Dictionary of trained parameters.
        """
        data = CycleTimeCalc.read_file(
            self.base_folder, CycleTimeCalc.train_param_last_file_name(telescope=telescope, command=command)
        )
        try:
            ret = CycleTimeCalc._parse_data(data=data)[-1]
        except (LookupError, ValueError):
            ret = None
        return ret

    def set_start_rmode(self, rmode: int) -> None:
        """
        Method set readout mode index.
        :param rmode: Put index of readout mode in camera readout modes list (index starts from 0).
        :return: None
        """
        self._rmode = rmode

    def set_rm_modes(self, rm_modes: Dict[str, List[float]] = None) -> None:
        """
        Method set readout modes data.
        :param rm_modes: Rm modes in dict contains telescope id and list of readout modes in MHz in order of index
            setup in camera: {
            "dev": [5, 2, 1, 0.1, 0.1, 0.2, 0.2],
            "sim": [5, 3, 2, 1, 0.1, 0.1, 0.05]
            }
            For example for telescope "dev", value 1MHz persist on index 2 in camera readout modes list.
        :return: None
        """
        self._set_rm_modes = rm_modes

    @property
    def _rm_mode_inv_mhz(self) -> float:
        return CycleTimeCalc._rm_mode_inverse_mhz(
            rm_mode=self._rmode, telescope=self.telescope, rm_modes=self._set_rm_modes
        )

    def set_start_time(self, utc_time_stamp: datetime.datetime) -> None:
        """
        Set start time.
        :param utc_time_stamp: Put utc timestamp (datetime).
        :return: None
        """
        self._start_time = utc_time_stamp

    def set_telescope_start_az_alt(self, az: float, alt: float) -> None:
        """
        Set mount az alt start position in deg (for more precise prediction).
        :param az: Mount azimuth in deg (float).
        :param alt: Mount altitude in deg (float).
        :return: None
        """
        self._alt_mount = alt
        self._az_mount = az

    def set_dome_start_az(self, az: float) -> None:
        """
        Set dome start position in deg (for more precise prediction).
        :param az: Dome azimuth in deg (float).
        :return: None
        """
        self._az_dome = az

    def _mount_altaz_target(self, command_dict: Dict[str, Any]) -> Dict[str, float] or None:
        """
        Method calculate alt az from command dictionary
        :param command_dict: command dict
        :return: calculated alt az
        """
        altaz = None
        if 'args' in command_dict.keys():
            if len(command_dict['args']) == 2:
                ra = command_dict['args'][0]
                dec = command_dict['args'][1]

            elif len(command_dict['args']) == 3:
                ra = command_dict['args'][1]
                dec = command_dict['args'][2]
            else:
                ra = None
                dec = None
        else:
            ra = None
            dec = None
        if (ra is not None) and (dec is not None):
            az, alt = ra_dec_2_az_alt(ra=ra_to_decimal(ra),
                                      dec=dec_to_decimal(dec),
                                      longitude=self._observatory_location['longitude'],
                                      latitude=self._observatory_location['latitude'],
                                      elevation=self._observatory_location['elevation'],
                                      epoch=self._epoch,
                                      time=self._start_time.timestamp() + self._time_length)
            altaz = {'az': az, 'alt': alt}
        if 'kwargs' in command_dict.keys() and (ra is None) and (dec is None):
            if ('az' and 'alt') in command_dict['kwargs'].keys():
                az = float(command_dict['kwargs']['az'])
                alt = float(command_dict['kwargs']['alt'])
                altaz = {'az': az, 'alt': alt}
        return altaz

    def forced_readout_mode(self, command_dict: Dict[str, Any]) -> None:
        if 'kwargs' in command_dict.keys():
            if 'read_mod' in command_dict['kwargs']:
                try:
                    new_rm = command_dict['kwargs']['read_mod']
                except (LookupError, TypeError, ValueError):
                    return
                if new_rm in self._set_rm_modes[self.telescope]:
                    self._rmode = new_rm
                else:
                    logger.error(
                        f"No red mod {new_rm} in {self._set_rm_modes[self.telescope]}"
                    )

    def _calc_time_no_wait_commands(self, command_dict: Dict[str, Any]) -> float or None:
        azalt = self._mount_altaz_target(command_dict=command_dict)
        if azalt is not None and not self.tpg:
            if (azalt['alt'] >= self._alt_limit) or (azalt['alt'] < self._alt_limit and self._skipping is False):
                mount_alt_az_dist = CycleTimeCalc._alt_az_distance(
                    start_alt=self._alt_mount, end_alt=azalt['alt'], start_az=self._az_mount, end_az=azalt['az']
                )
                dome_az_dist = CycleTimeCalc._az_distance(start=self._az_dome, end=azalt['az'])
                self._alt_mount = azalt['alt']
                self._az_mount = azalt['az']
                self._az_dome = azalt['az']
            else:
                logger.debug(f'Alt is under lower limit, no time calculation')
                return 0.0
        elif self.tpg:
            try:
                dome_az_dist = self.available_param[self.telescope][command_dict['command_name']]['dome_average_dist']
                mount_alt_az_dist = \
                    self.available_param[self.telescope][command_dict['command_name']]['mount_average_dist']
            except (LookupError, TypeError):
                dome_az_dist = 0.0
                mount_alt_az_dist = 0.0
        else:
            mount_alt_az_dist = 0.0
            dome_az_dist = 0.0
        command_dict_param = {}
        exp_no = CycleTimeCalc._exposure_number(command_dict)
        dither = CycleTimeCalc._dither_on(command_dict=command_dict)
        command_dict['filter_pos'] = self._current_filter
        command_dict_param['dome_distance'] = dome_az_dist
        command_dict_param['mount_distance'] = mount_alt_az_dist
        command_dict_param['exposure_time_sum'] = CycleTimeCalc._exposure_time_sum(command_dict)
        command_dict_param['filter_changes'] = CycleTimeCalc._filter_changes(record=command_dict)
        self.forced_readout_mode(command_dict=command_dict)
        command_dict_param['rmmode_expno'] = self._rm_mode_inv_mhz * exp_no
        command_dict_param['dither_expno'] = dither * exp_no
        self._current_filter = CycleTimeCalc._last_filter(command_dict)
        time_cal = self._calc_time(command_name=command_dict['command_name'], command_dict_param=command_dict_param)
        if time_cal:
            if time_cal >= 0:
                comm_time = time_cal
            else:
                comm_time = 0.0
        else:
            comm_time = 0.0
        self._time_length += comm_time
        return comm_time

    def _calc_time_wait_command(self, command_dict: Dict[str, Any]) -> float or None:
        if command_dict['command_name'] == 'WAIT':
            if 'kwargs' in command_dict.keys():

                if 'sec' in command_dict['kwargs'].keys():
                    t = float(command_dict['kwargs']['sec'])
                    self._time_length += t
                    return t

                elif 'ut' in command_dict['kwargs'].keys():
                    dat_0 = self._start_time.timestamp() + self._time_length
                    val = (command_dict['kwargs']['ut']).split(':')
                    if len(val) == 2:
                        ut_str = f'{val[0]}:{val[1]}:00'
                    else:
                        ut_str = command_dict['kwargs']['ut']
                    target_t = self._that_day_time(date_time_stamp=dat_0, time_str=ut_str)
                    if target_t >= (self._start_time.timestamp() + self._time_length):
                        t = float(target_t - (self._start_time.timestamp() + self._time_length))
                        self._time_length += t
                        return t
                    else:
                        dat_1 = self._start_time.timestamp() + self._time_length +\
                                datetime.timedelta(days=1).total_seconds()
                        target_t = self._that_day_time(date_time_stamp=dat_1, time_str=command_dict['kwargs']['ut'])
                        t = float(target_t - (self._start_time.timestamp() + self._time_length))
                        self._time_length += t
                        return t

                elif 'sunrise' in command_dict['kwargs'].keys():
                    now = self._start_time + datetime.timedelta(seconds=self._time_length)
                    try:
                        sun = calculate_sun_rise_set(
                            date=now,
                            horiz_height=float(command_dict['kwargs']['sunrise']),
                            sunrise=True,
                            latitude=self._observatory_location['latitude'],
                            longitude=self._observatory_location['longitude'],
                            elevation=self._observatory_location['elevation']
                        )
                    except AttributeError:
                        return 0
                    t = (sun - now).seconds
                    if t / 3600 > 18:
                        t = 0
                    else:
                        self._time_length += t
                    return t

                elif 'sunset' in command_dict['kwargs'].keys():
                    now = self._start_time + datetime.timedelta(seconds=self._time_length)
                    try:
                        sun = calculate_sun_rise_set(
                            date=now,
                            horiz_height=float(command_dict['kwargs']['sunset']),
                            sunrise=False,
                            latitude=self._observatory_location['latitude'],
                            longitude=self._observatory_location['longitude'],
                            elevation=self._observatory_location['elevation']
                        )
                    except AttributeError:
                        return 0
                    t = (sun - now).seconds
                    t = (sun - now).seconds
                    if t / 3600 > 12:
                        t = 0
                    else:
                        self._time_length += t
                    return t

                else:
                    logger.debug(f'Cannot calculate time, no kwarg')
                    return 0.0
            else:
                logger.debug(f'Cannot calculate time, no kwargs')
                return 0.0
        else:
            logger.debug(f'Cannot calculate time, command not recognized')
            return 0.0

    @staticmethod
    def _that_day_time(date_time_stamp: float, time_str: str) -> float:
        """
        Metgod gives time of time_str for day given by date_time_stamp (no matter what time in this date_time_stamp)
        :param date_time_stamp: day red from this timestamp
        :param time_str: time in str format
        :return: timestamp of date(date_time_stamp) and time(time_str)
        """
        dat_ts_0 = datetime.datetime.fromtimestamp(date_time_stamp)
        day = dat_ts_0.strftime('%Y-%m-%d')
        mydate = f"{day} {time_str}"
        return time.mktime(datetime.datetime.strptime(mydate, "%Y-%m-%d %H:%M:%S").timetuple())

    def calc_time(self, command_dict: Union[Dict[str, Any], str]) -> float or None:
        """
        Method calculate time for one command.
        :param command_dict: command_dict parsed by pyaraucaria obs_plan_parser or command string.
        :return: operation time in seconds (float).
        """
        tim = 0.0
        if isinstance(command_dict, str):
            conv = ObsPlanParser.convert_from_string(command_dict)
            try:
                command_dict = conv['subcommands'][0]
            except (KeyError, IndexError):
                logger.error(f'Please check plan format')
        elif isinstance(command_dict, Dict):
            command_dict = dict(command_dict)
        else:
            logger.error(f'command_dict TypeError')
            raise TypeError

        try:
            if command_dict['command_name'] in self._avialable_param_telesc_commands:
                tim = self._calc_time_no_wait_commands(command_dict=command_dict)
            elif command_dict['command_name'] in CycleTimeCalc._NO_TRAIN_COMMANDS:
                tim = self._calc_time_wait_command(command_dict=command_dict)
            else:
                logger.debug(f'Cannot calculate time, lack of params')
            self._time_length_list.append(tim)
        except KeyError:
            logger.error(f'Please check plan format')
        return tim

    @property
    def time_lenght_sec(self) -> float:
        """
        Sum of accumulated time predictions.
        :return: Sum of accumulated time predictions in seconds (float).
        """
        return self._time_length

    @property
    def time_lenght_min(self) -> float:
        """
        Sum of accumulated time predictions.
        :return: Sum of accumulated time predictions in minutes (float).
        """
        return round(self._time_length/60, 1)

    @property
    def finnish_time_utc(self) -> str:
        """
        Operation sequence finnish time UTC.
        :return: Str operation sequence finnish time UTC.
        """
        return str(self._start_time + datetime.timedelta(seconds=self._time_length))

    @property
    def time_list(self) -> List[float]:
        """
        List of accumulated time predictions.
        :return: List of accumulated time predictions.
        """
        return self._time_length_list

    def _calc_time(self, command_name: str, command_dict_param: Dict[str, Any]) -> float or None:
        no_error = True
        if command_name in self.USE_OBJECT_PARAMS_IN:
            command_name = 'OBJECT'
        param = self.available_param[self.telescope][command_name]
        for n, m in command_dict_param.items():
            if n not in param['coef'].keys():
                no_error = False
        if no_error:
            ret = 0
            for n, m in command_dict_param.items():
                ret += (m * param['coef'][n])
            ret += param['intercept']
            return ret
        else:
            return None

    def reset_time(self) -> None:
        """
        Reset time accumulated during time calculation.
        :return: None
        """
        self._start_time = datetime.datetime.utcnow()
        self._time_length_list = []
        self._time_length = 0
