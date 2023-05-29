import sys
import pathlib as pl
sys.path.append(str(pl.Path(__file__).parent.parent.parent))

from datetime import datetime, timedelta
import logging
import numpy as np
import pandas as pd
import re
from functools import partial
from typing import Any
from .data_parsing import DataWrapper, Iterable_Baseclass
from dataclasses import dataclass, field
from mvg_tracker.data_validation.validator import Validator, TypeHandler, Function_Mapper
from mvg_tracker.data_validation.validation_func import is_in, extract_digits_from_string
from .enum_classes import Network, Product


typeHandler = TypeHandler(dateformat="%Y/%m/%d, %H:%M:%S")

@dataclass()
class Departure(DataWrapper):
    departureTime: datetime = Validator(typeHandler)
    delay: int = Validator(typeHandler, allow_none=True, omit_logging=True)
    time_of_record: datetime = datetime.now()
    sev: bool = Validator(typeHandler)
    destination: str = Validator(typeHandler)
    product: Product = Validator(typeHandler)
    label: str = Validator(typeHandler)
    cancelled: bool = Validator(typeHandler)
    lineBackgroundColor: str = Validator(typeHandler)
    live: bool = Validator(typeHandler)
    departureId: str = Validator(typeHandler)
    platform: int = Validator(typeHandler,
                              cleaning_func=Function_Mapper(extract_digits_from_string, "value"))
    stopPositionNumber: int = Validator(typeHandler)
    infoMessages: list[str] = Validator(typeHandler)
    displayInfoMessage: str = field(default=None)
    station_id: int = field(init=False)
    destination_id: int = field(init=False)
    time_of_dep: datetime = field(init=False)
    line_id: int = field(init=False)
    invaild: bool = field(init=False, default=False)
    departure_id: str = field(init=False)
    
    def __post_init__(self):
        super().__post_init__()
        self.time_of_dep = self.departureTime
        self.departure_id = self.departureId
        try:
            self.line_id = int(
                self.label[1:] if not self.label[0].isnumeric() else self.label
            )
        except ValueError:
            self.invaild = True
            # self.logger.warn(f"could parse label {self.label} to line id")
    
    def get_time_to_dep(self) -> timedelta:
        cur_time = datetime.now()
        return self.departureTime - cur_time

    def set_station_id(self, station_id: str) -> None:
        self.station_id = int(station_id.replace(":", "")
                                        .replace("de", ""))

    def set_destinationId_by_name(self, all_stations_names: np.ndarray[Any, np.dtype[np.str_]],
                                  all_stations_ids: np.ndarray[Any, np.dtype[np.int32]]):
        destination_ind: int = list(map(
            lambda x: x in self.destination, all_stations_names))\
            .index(True)
        try:
            self.destination_id = all_stations_ids[destination_ind]
        except ValueError:
            self.destination_id = 0
            self.logger.warn(
                f"Encountered non matching destination {self.destination}")
    
    def get_df_repr(self, *args: str) -> pd.DataFrame:
        dict_repr: dict = {}
        for field_name in args:
            dict_repr[field_name] = [getattr(self, field_name)]
        return pd.DataFrame(dict_repr)



@dataclass
class ServingLine(DataWrapper):
    destination: str = Validator(typeHandler)
    sev: bool = Validator(typeHandler)
    network: Network = Validator(typeHandler)
    product: Product = Validator(typeHandler)
    lineNumber: str = Validator(typeHandler)
    divaId: str = Validator(typeHandler)


@dataclass(slots=True)
class StationResponse(DataWrapper):
    servingLines: list[ServingLine]
    departures: list[Departure]
