import logging
import pathlib as pl
from mvg_tracker.logging_util.init_loggers import init_console_logger, init_file_logger
from datetime import datetime
from copy import deepcopy
from functools import partial
from collections.abc import Callable
from typing import Any
from enum import Enum
from math import log2

LOG_DIRECTORY = None


def _cast_to_bool_from_int(inp: int) -> bool:
    if inp in [0, 1]:
        return bool(inp)
    raise TypeError(f"int value {inp} can not be casted to bool")


def _cast_to_int_from_float(inp: float) -> int:
    if int(inp) == inp:
        return int(inp)
    raise TypeError(
        f"float value {inp} could not be casted to int without loss of presicion")


def _cast_to_datetime_from_str(inp: str, dateformat: str) -> datetime:
    return datetime.strptime(inp, dateformat)


def _cast_to_datetime_from_int(inp: int) -> datetime:
    if log2(inp) > 34:
        inp = inp // 1000
    return datetime.fromtimestamp(inp)


class TypeHandler():
    DATEFORMAT: str = None
    TYPE_MAPPING: dict[tuple[type], Callable[[Any], Any]] = {
        (int, bool): _cast_to_bool_from_int,
        (int, float): lambda x: float(x),
        (str, pl.Path): lambda x: pl.Path(x),
        (str, int): lambda x: int(x) if x else None,
        (str, float): lambda x: float(x) if x else None,
        (float, int): _cast_to_int_from_float,
        (int, datetime): _cast_to_datetime_from_int,
    }

    def __init__(self, dateformat) -> None:
        datetime_parsing_fct = partial(
            _cast_to_datetime_from_str, dateformat=dateformat)
        self.TYPE_MAPPING[(str, datetime)] = datetime_parsing_fct


class Function_Mapper():

    def __init__(self,
                 func: callable,
                 value_kw: str,
                 *args,
                 **kwargs
                 ) -> None:
        self.value_kw = value_kw
        self.func = func
        self.args = args
        self.kwargs = kwargs
    
    def invoke(self, value):
        self.kwargs[self.value_kw] = value
        self.func(*self.args, **self.kwargs)


class Validator:
    """class for validation of arguments Dataclass Args:\n
       Args:
        func (function): callback function which will either run successfully \
            or raise a Value Error
        value_list (list): reference list for checking for inclusion of the \
            value
        value_range (list): list of form [minValue, maxValue] for checking \
            if value is in between, if None is set for maxValue or minValue, \
            performs one sided check
        default (any): default value which is dependent on the type annotation\
             the class is instantiated with
        allow_none (bool): accept None as set Value, if False and default is None\
            raises TypeError when no argument is passed
        **kwargs: additional arguments for the callback function for special\
             cases
       Raises:
        ValueError: if the Validation fails
        TypeError: if the type in value_list or value_range don't match

       Returns:
        either the default value or the value passed by the owner class \
        defaults itself to none   
    """

    def __init__(self,
                 type_handler: TypeHandler,
                 validator_func: Function_Mapper = None,
                 cleaning_func: Function_Mapper = None,
                 default=None,
                 allow_none: bool = False,
                 omit_logging: bool = False):

        self.cleaning_func = cleaning_func
        self.validator_func = validator_func
        self.type_handler = type_handler
        self.allow_none = allow_none
        self.default = default
        self.omit_logging = omit_logging
        self.init_logger()

    def init_logger(self):

        self.logger = logging.getLogger()
        self.logger = init_console_logger(self.logger)
        if LOG_DIRECTORY is not None:
            self.logger = init_file_logger(self.logger, LOG_DIRECTORY)

    def __repr__(self) -> str:
        return str(self.default)

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        if not instance:
            return self
        # return instance.__dict__[self.name]
        return instance.__dict__.get(self.name, self.default)

    def __delete__(self, instance):
        del instance.__dict__[self.name]

    def __set__(self, instance, value):

        annotated_type = instance.__annotations__[self.name]

        if value is self:
            if not self.omit_logging:
                self.logger.warn(f"field '{self.name}' in Parentfield " +
                                f"'{instance.__class__.__name__}' was not passed defaulting to {self.default}")
            if self.default is None and not self.allow_none:
                raise TypeError(
                    f"{instance.__class__.__name__}() missing 1 required " +
                    f"positional argument: '{self.name}'")
            # handle case where a field is equal to another field,
            # e.g. output_dir = input_dir
            if isinstance(self.default, str)\
                    and hasattr(instance, self.default):
                value = getattr(instance, self.default)

            else:
                value = self.default
        # apply function to clean the possible values
        if self.cleaning_func is not None:
            value = self.cleaning_func.invoke(value)
        # cast to annotated values if applicable
        value_type = type(value)
        if isinstance(value, Validator):
            value = value.__repr__()

        if issubclass(annotated_type, Enum):
            try:
                value = annotated_type(value)
            except ValueError:
                self.logger.info(f"found non matching value for enum {annotated_type} of value {value}")
                value = None
            finally:
                instance.__dict__[self.name] = value
            return

        if hasattr(annotated_type, "__origin__"):
            sub_annotated_type = annotated_type.__args__[0]
            annotated_type = getattr(annotated_type, "__origin__")
            # return if list is empty
            if not value:
                return
            sub_value_type = type(value[0])
        else:
            sub_annotated_type = type(None)

        if not isinstance(value, annotated_type)\
                and value is not None\
                and not isinstance(value, dict):
            # pass test if int is compared to float
            try:
                if isinstance(sub_annotated_type, type(None)):
                    fct_to_call = self.type_handler.TYPE_MAPPING[
                        (sub_value_type, sub_annotated_type)]
                    value = map(fct_to_call, value)
                else:
                    fct_to_call = self.type_handler.TYPE_MAPPING[
                        (value_type, annotated_type)]
                    value = fct_to_call(value)

            except KeyError:
                raise NotImplementedError(
                    f"value of type {value_type} could not be automatically casted to {annotated_type}")

        try:
            if self.validator_func is not None and value is not None:
                temp_bounds = deepcopy(self.validator_func.kwargs)
                # for getting proxy reference e.g. outputpath = "inputpath" with "inputpath" being a reference to the
                # field inputpath of the class itself same with the reference nested inside a list 
                for key, val in temp_bounds.items():
                    if isinstance(val, list):
                        for i, v in enumerate(val):
                            if not isinstance(v, str):
                                continue
                            elif hasattr(instance, v):
                                val[i] = getattr(instance, v)
                    else:
                        if hasattr(instance, str(val)):
                            temp_bounds[key] = getattr(instance, val)
                self.validator_func.kwargs = temp_bounds
                msg = self.validator_func.invoke(value)
                if msg is not None:
                    self.logger.error(msg)
        except ValueError as e:
            raise ValueError(
                f"ValidationTest failed for field '{self.name}': {e}")
        instance.__dict__[self.name] = value
