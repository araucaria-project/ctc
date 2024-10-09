from typing import Dict, Any
from ctc.ctdata import CTData
import logging
from ctc.abstract_cycle_time import AbstractCycleTime


logger = logging.getLogger(__name__.rsplit('.')[-1])


class CycleTimeDataBuild(AbstractCycleTime):
    """
    This class represent cycle time data builder (collector)
    """

    def __init__(self, telescope: str, base_folder: str) -> None:
        self.telescope = telescope
        self.base_folder = base_folder
        self._data: CTData = CTData()
        super().__init__()
        self._mk_dirs(base_folder=base_folder)

    async def insert_data(self, name: str, value: Any) -> None:
        logger.debug(f'Data inserted: {name} {value}')
        await self._data.insert_single_data(name=name, value=value)

    async def insert_data_dict(self, dict: Dict[str, Any]) -> None:
        logger.debug(f'Data inserted from dict: {dict}')
        await self._data.insert_data_from_dict(dict=dict)

    async def save_to_log(self) -> None:
        logger.debug(f'Data {self.telescope} saved to log')
        await self._data.insert_single_data('utc_date_stamp', str(self._time_stamp().isoformat()))
        await self._data.insert_single_data('utc_time_stamp', str(self._time_stamp().timestamp()))
        self._add_to_file(
            data=self._encode_data(await self._data.get_data()),
            folder=self.base_folder,
            file_name=self.raw_file_name(telescope=self.telescope)
        )
