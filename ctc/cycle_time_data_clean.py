import time
from typing import Dict, Any, List
from ctc.abstract_cycle_time import AbstractCycleTime
import logging
from collections import OrderedDict
from ctc.iter_async import AsyncEnumerateIter, AsyncDictItemsIter, AsyncListIter


logger = logging.getLogger(__name__.rsplit('.')[-1])


class CycleTimeDataClean(AbstractCycleTime):
    """
    This class represent cycle time data cleaner
    """

    @staticmethod
    async def _find_nights_id(data: List[Any]) -> Dict[str, Dict[str, Any]]:
        random_id = ''
        full_nights_id = {}
        line_started = None
        async for n, m in AsyncEnumerateIter(data):
            if m['command_name'] == 'NIGHTPLAN' and 'done' not in m.keys() and \
                    'skipped' not in m.keys() and random_id != m['random_id']:
                if line_started is not None:
                    line_ended = n - 1
                    full_nights_id[random_id] = {
                        'started': line_started,
                        'ended': line_ended,
                        'utc_time_stamp': m["utc_time_stamp"]
                    }
                    line_started = None
                else:
                    random_id = m['random_id']
                    line_started = n
            if m['command_name'] == 'NIGHTPLAN' and ('done' in m.keys() or 'skipped' in m.keys()) \
                    and random_id == m['random_id']:
                line_ended = n
                full_nights_id[random_id] = {
                    'started': line_started,
                    'ended':line_ended,
                    'utc_time_stamp': m["utc_time_stamp"]
                }
                line_started = None
        logger.debug(f'Nights id found: {full_nights_id}')
        return full_nights_id

    @staticmethod
    async def _build_commands_data(
            data: List[Dict[str, Any]], telescope: str,
            full_nights_id: Dict[str, Dict[str, Any]], rm_modes: Dict[str, List[float]] = None) -> Dict[str, Any]:
        commands_data = {}
        async for n_id, m_dict in AsyncDictItemsIter(full_nights_id):
            async for p_no, q_data in AsyncEnumerateIter(data):
                if not isinstance(m_dict['started'], int) or not isinstance(m_dict['ended'], int):
                    continue
                if m_dict['started'] < p_no < m_dict['ended']:
                    if q_data['command_name'] not in commands_data.keys():
                        commands_data[q_data['command_name']] = []
                    if 'done' not in q_data.keys():
                        if q_data['command_name'] not in AbstractCycleTime._NO_TRAIN_COMMANDS:
                            if data[p_no + 1]['skipped'] == 'False':
                                dat = await CycleTimeDataClean._build_command_data(
                                    data=data, telescope=telescope, night_id=n_id, line_start=p_no, rm_modes=rm_modes
                                )
                                if dat is not None:
                                    commands_data[q_data['command_name']].append(dat)
        return commands_data

    @staticmethod
    async def _verify_nights(full_nights_id: Dict[str, Dict[str, Any]], telescope: str, base_folder: str):
        f = await CycleTimeDataClean.a_read_file(
            folder=base_folder, file_name=CycleTimeDataClean.night_verif_file_name(telescope=telescope)
        )
        if f is not None:
            l = f.split('\n')
            async for n in AsyncListIter(l):
                if len(n) > 1:
                    d = CycleTimeDataClean._decode_data(n)
                    if d['random_id'] in full_nights_id.keys():
                        if d['utc_time_stamp'] == full_nights_id[d['random_id']]['utc_time_stamp']:
                            full_nights_id.pop(d['random_id'])
        else:
            pass
        return full_nights_id

    @staticmethod
    async def _save_commands_data_to_files(
            commands_data: Dict[str, Any], full_nights_id: Dict[str, Dict[str, Any]],
            telescope: str, base_folder: str) -> None:
        logger.debug(f'Save commands data for telescope: {telescope}')
        async for n in AsyncListIter(list(commands_data.keys())):
            async for m in AsyncListIter(commands_data[n]):
                await CycleTimeDataClean._a_add_to_file(
                    data=CycleTimeDataClean._encode_data(m),
                    folder=base_folder,
                    file_name=CycleTimeDataClean.clean_data_file_name(telescope=telescope, command=n))
        async for p, q in AsyncDictItemsIter(full_nights_id):
            dat = OrderedDict({
                'random_id': p,
                'utc_time_stamp': q['utc_time_stamp'],
            })
            await CycleTimeDataClean._a_add_to_file(
                data=CycleTimeDataClean._encode_data(data=dat),
                folder=base_folder,
                file_name=CycleTimeDataClean.night_verif_file_name(telescope=telescope))

    @staticmethod
    async def _build_command_data(
            data: List[Dict[str, Any]], telescope: str, night_id: str,
            line_start: int, rm_modes: Dict[str, List[float]] = None) -> Dict[str, Any]:

        first = data[line_start]
        second = data[line_start + 1]
        third = data[line_start + 2]

        if third['command_name'] == 'NIGHTPLAN' and 'done' not in third.keys() and 'skipped' not in third.keys():
            dat = None
        else:
            if 'dome_az_end' in third.keys():
                index_dome = 'dome_az_end'
            else:
                index_dome = 'dome_az_begin'

            if 'mount_alt_end' in third.keys():
                index_mount_alt = 'mount_alt_end'
            else:
                index_mount_alt = 'mount_alt_begin'

            if 'mount_az_end' in third.keys():
                index_mount_az = 'mount_az_end'
            else:
                index_mount_az = 'mount_az_begin'
            try:
                dat = OrderedDict({
                    CycleTimeDataClean._DATA_CLEAN_ORDER[0]:
                        night_id,
                    CycleTimeDataClean._DATA_CLEAN_ORDER[1]:
                        first['command_name'],
                    CycleTimeDataClean._DATA_CLEAN_ORDER[2]:
                        first['utc_date_stamp'],
                    CycleTimeDataClean._DATA_CLEAN_ORDER[3]:
                        float(third['utc_time_stamp']) - float(first['utc_time_stamp']),
                    CycleTimeDataClean._DATA_CLEAN_ORDER[4]:
                        CycleTimeDataClean._az_distance(
                            start=float(first['dome_az_begin']),end=float(third[index_dome])
                        ),
                    CycleTimeDataClean._DATA_CLEAN_ORDER[5]:
                        CycleTimeDataClean._alt_az_distance(
                            start_alt=float(first['mount_alt_begin']),
                            end_alt=float(third[index_mount_alt]),
                            start_az=float(first['mount_az_begin']),
                            end_az=float(third[index_mount_az])
                        ),
                    CycleTimeDataClean._DATA_CLEAN_ORDER[6]:
                        CycleTimeDataClean._exposure_time_sum(first),
                    CycleTimeDataClean._DATA_CLEAN_ORDER[7]:
                        CycleTimeDataClean._filter_changes(record=first),
                    CycleTimeDataClean._DATA_CLEAN_ORDER[8]:
                        CycleTimeDataClean._rm_mode_inverse_mhz(
                            rm_mode=CycleTimeDataClean._rm_mode(second),
                            telescope=telescope,
                            rm_modes=rm_modes
                        ) * CycleTimeDataClean._exposure_number(first),
                    CycleTimeDataClean._DATA_CLEAN_ORDER[9]:
                        CycleTimeDataClean._dither(first) * CycleTimeDataClean._exposure_number(first),

                })
            except (KeyError, IndexError, ValueError, TypeError):
                dat = None
        return dat

    @staticmethod
    async def data_clean(telescope: str, base_folder: str, rm_modes: Dict[str, List[float]] = None) -> None:
        """
        Method reading and cleaning only data from full nights (i.e. complete program) and only new data
        """
        str_dat = await CycleTimeDataClean.a_read_file(
            folder=base_folder,
            file_name=CycleTimeDataClean.raw_file_name(telescope=telescope)
        )
        parsed_data = await CycleTimeDataClean._a_parse_data(str_dat)
        full_nights_id = await CycleTimeDataClean._find_nights_id(parsed_data)
        full_nights_id = await CycleTimeDataClean._verify_nights(
            full_nights_id=full_nights_id, telescope=telescope, base_folder=base_folder
        )
        commands_data = await CycleTimeDataClean._build_commands_data(
            data=parsed_data, telescope=telescope, full_nights_id=full_nights_id, rm_modes=rm_modes
        )
        await CycleTimeDataClean._save_commands_data_to_files(
            commands_data=commands_data,
            full_nights_id=full_nights_id,
            telescope=telescope,
            base_folder=base_folder
        )

    @staticmethod
    async def data_clean_all(
            base_folder: str, rm_modes: Dict[str, List[float]] = None, skip_if_was_today: bool = True) -> None:
        """
        Method clean data for all telescopes and all commands.
        :param base_folder: Path to data folder.
        :param rm_modes: Rm modes in dict contains telescope id and list of readout modes in MHz in order of index
            setup in camera: {
            "dev": [5, 2, 1, 0.1, 0.1, 0.2, 0.2],
            "sim": [5, 3, 2, 1, 0.1, 0.1, 0.05]
            }
            For example for telescope "dev", value 1MHz persist on index 2 in camera readout modes list.
        :param skip_if_was_today: skip operation if was done today
        :return: None
        """
        t_0 = time.time()
        if skip_if_was_today and await CycleTimeDataClean.if_last_clean_train_was_today(base_folder=base_folder):
            logger.info(f'Cleaning was done today, skipping.')
            return
        logger.debug(f'Data clean for all telescopes and all commands type')
        tel = CycleTimeDataClean.get_list_telesc(file_type='raw_data', base_folder=base_folder)
        async for t in AsyncListIter(tel):
            logger.info(f'Data clean for telescope: {t}')
            await CycleTimeDataClean.data_clean(telescope=t, base_folder=base_folder, rm_modes=rm_modes)
        logger.info(f'Data clean done in {time.time() - t_0:.1f}')
