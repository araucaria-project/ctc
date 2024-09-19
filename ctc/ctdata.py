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
        super().__init__()

    def insert_single_data(self, name: str, value: Any) -> None:
        self._data[name] = str(value)

    def insert_data_from_dict(self, dict: Dict[str, Any]) -> None:
        self._data = self._data | dict

    def insert_str_data(self, value: str) -> None:
        self._data_str = value

    @property
    def data(self) -> Dict[str, Any]:
        return self._data

    @property
    def str_data(self) -> str:
        return self._str_data