import pathlib as pl
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
from mvg_tracker.request_parsing.enum_classes import Network, Product


typeHandler = TypeHandler(dateformat="%Y/%m/%d, %H:%M:%S")


def defaultValidatorFactory() -> Validator:
    return Validator(typeHandler)


@dataclass()
class Departure(DataWrapper):
    plannedDepartureTime: datetime = defaultValidatorFactory()
    realtimeDepartureTime: datetime = defaultValidatorFactory()
    time_of_record: datetime = datetime.now()
    sev: bool = defaultValidatorFactory()
    destination: str = defaultValidatorFactory()
    transportType: Product = defaultValidatorFactory()
    label: str = defaultValidatorFactory()
    trainType: str = defaultValidatorFactory()
    network: Network = defaultValidatorFactory()
    cancelled: bool = defaultValidatorFactory()
    realtime: bool = defaultValidatorFactory()
    delayInMinutes: int = Validator(typeHandler, allow_none=True, omit_logging=True)
    platform: int = Validator(typeHandler,
                              allow_none=True,
                              omit_logging=True,
                              cleaning_func=Function_Mapper(extract_digits_from_string, "value"))
    messages: list[str] = defaultValidatorFactory()
    bannerHash: str = defaultValidatorFactory()
    occupancy: str = defaultValidatorFactory()
    stopPointGlobalId: str = defaultValidatorFactory()
    stopPositionNumber: int = Validator(typeHandler,
                                        omit_logging=True,
                                        allow_none=True)
    displayInfoMessage: str = field(default=None)
    delay: int = field(init=False)
    station_id: int = field(init=False)
    destination_id: int = field(init=False)
    time_of_dep: datetime = field(init=False)
    line_id: int = field(init=False)
    invaild: bool = field(init=False, default=False)
    departure_id: str = field(init=False)

    def __post_init__(self):
        super().__post_init__()
        self.time_of_dep = self.plannedDepartureTime
        self.delay = self.delayInMinutes
        try:
            self.line_id = int(
                self.label[1:] if not self.label[0].isnumeric() else self.label
            )
        except ValueError:
            self.invaild = True
            # self.logger.warn(f"could parse label {self.label} to line id")

    def get_time_to_dep(self) -> timedelta:
        cur_time = datetime.now()
        return self.plannedDepartureTime - cur_time

    def set_station_id(self, station_id: str) -> None:
        self.station_id = int(station_id.replace(":", "")
                                        .replace("de", ""))
    
    def set_departure_id(self):
        self.departure_id = f"{self.station_id}_{int(self.plannedDepartureTime.timestamp())}_{self.label}_{self.transportType.value}"

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
    destination: str = defaultValidatorFactory()
    sev: bool = defaultValidatorFactory()
    network: Network = defaultValidatorFactory()
    product: Product = defaultValidatorFactory()
    lineNumber: str = defaultValidatorFactory()
    divaId: str = defaultValidatorFactory()


@dataclass(slots=True)
class StationResponse(DataWrapper):
    departures: list[Departure]
