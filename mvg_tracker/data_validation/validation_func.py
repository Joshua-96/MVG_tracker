import pathlib as pl
import re

LOG_DIRECTORY: pl.Path = None

IS_FILE_MSG = "path to directory was expected but filepath was given: "
IS_DIR_MSG = "filepath was expected, but path to directory was given: "
FILE_MISSING_MSG = "file not found at location: "
DIR_MISSING_MSG = "directory not found at location: "


def extract_digits_from_string(value: str):
    candidates = re.findall(pattern=r'\d+', string=value)
    if candidates:
        return candidates[0]


def begins_with(value: str, start_val: str):
    if value.startswith(start_val):
        return
    else:
        raise ValueError(f"Value <{value}> does not begin with '{start_val}'")


def is_positiv(value: list[int | float]) -> bool:
    if isinstance(value, list):
        for v in value:
            is_positiv(v)
        return
    if value >= 0:
        return
    else:
        raise ValueError(f"Value <{value}> must be positive")


def is_between(value: int | float,
               value_range: list[int | float]) -> bool:
    if isinstance(value, list):
        for v in value:
            is_between(v, value_range)
        return
    min_val, max_val = value_range
    if max_val is not None:
        if not (isinstance(max_val, int) or isinstance(max_val, float)):
            raise TypeError(
                f"max_value must be either Int or Float not {type(value)}!"
            )
        if value > max_val:
            raise ValueError(
                f"Value <{value}> is too large must be equal or smaller than {max_val}")

    if min_val is not None:
        if isinstance(min_val, list):
            min_val = min(min_val)
        if not (isinstance(min_val, int) or isinstance(min_val, float)):
            raise TypeError(
                f"min_value must be either Int or Float not {type(value)}!"
            )
        if value < min_val:
            raise ValueError(
                f"Value <{value}> is too small must be equal or grater than {min_val}")
    return


def has_length(value: list,
               value_range: list[int | float]) -> bool:
    """function to check, if list of values is in the expected length interval
        :param value: list - list of values
        :param value_range: list - list of upper and lower bound of expected length
        :raises TypeError if value list does not contain int or float elements
        :raises ValueError if length is out of bounds
        :returns None"""
    minsize, maxsize = value_range
    if maxsize is not None:
        if not isinstance(maxsize, int):
            raise ValueError(
                f"Value <'{value}'> is too long must be less than {maxsize} elements long")
        if isinstance(value, int):
            value = str(value)
        if len(value) > maxsize:
            raise TypeError(
                f"max_value must be either Int or Float not {type(value)}!")

    if minsize is not None:
        if not isinstance(minsize, int):
            raise TypeError(
                f"min_value must be either Int or Float not {type(value)}!")
        if len(value) < minsize:
            raise ValueError(
                f"Value '{value}' is too short must at least {minsize} elements long")
    return


def is_in(value: str, value_list: list):
    if value in value_list:
        return
    else:
        raise ValueError(
            f"Value <{value}> is not a included in {value_list}")


def is_optional_dir(value: pl.Path) -> str:
    if value.is_dir() and value.exists():
        return
    elif value.is_file():
        is_file_msg = f"{IS_FILE_MSG} {value}"
        raise NotADirectoryError(is_file_msg)
    missing_dir_msg = f"{DIR_MISSING_MSG} {value}"
    value.mkdir(parents=True, exist_ok=True)
    return missing_dir_msg + "creating directory"


def is_dir(value: pl.Path) -> str:
    """Checks if directory exists or is a file, raises and error or returns a
       warning. Will create the directory recursively if raise_error is set to false
       :param value: pl.Path - Path to check
       :param raise_error: bool - whether to raise an error
       :raises NotADirectoryError
       :raises FileNotFoundError
       :returns: str - a message with a appropriate warning"""
    if value.is_dir() and value.exists():
        return
    elif value.is_file():
        is_file_msg = f"{IS_FILE_MSG} {value}"
        raise NotADirectoryError(is_file_msg)

    missing_dir_msg = f"{DIR_MISSING_MSG} {value}"
    raise FileNotFoundError(missing_dir_msg)


def is_optional_file(value: pl.Path) -> str:
    if value.is_file():
        return
    elif value.is_dir():
        is_dir_msg = f"{IS_DIR_MSG} {value}"
        return is_dir_msg
    file_missing_msg = f"{FILE_MISSING_MSG} {value}"
    return file_missing_msg


def is_file(value: pl.Path):
    """Checks if file exists or is a directory, raises and error or returns a
       warning.
       :param value: pl.Path - Path to check
       :param raise_error: bool - whether to raise an error
       :raises IsADirectoryError
       :raises FileNotFoundError
       :returns: str - a message with a appropriate warning"""
    if value.is_file():
        return
    elif value.is_dir():
        is_dir_msg = f"{IS_DIR_MSG} {value}"
        raise IsADirectoryError(is_dir_msg)
    file_missing_msg = f"{FILE_MISSING_MSG} {value}"
    raise FileNotFoundError(file_missing_msg)


def is_divisible(value: int, divisor: int):
    if value % divisor == 0:
        return
    else:
        raise ValueError(f"value {value} must be divisible by {divisor}")
