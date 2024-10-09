import asyncio
from typing import Dict, Any
import logging


logger = logging.getLogger(__name__.rsplit('.')[-1])


class CTData:
    """
    This class represent cycle time data
    """

    def __init__(self) -> None:
        self._str_data: str = ''
        self._data: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
        super().__init__()

    async def insert_single_data(self, name: str, value: Any) -> None:
        async with self._lock:
            self._data[name] = str(value)

    async def insert_data_from_dict(self, dict: Dict[str, Any]) -> None:
        async with self._lock:
            self._data = self._data | dict

    async def get_data(self) -> Dict[str, Any]:
        async with self._lock:
            return self._data

    async def get_str_data(self) -> str:
        async with self._lock:
            return self._str_data