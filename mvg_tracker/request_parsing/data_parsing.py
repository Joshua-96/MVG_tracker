import logging
import pathlib as pl
from logging_util.init_loggers import init_console_logger, init_file_logger
from abc import ABC, abstractmethod
from typing import TypeAlias


class DataParingError(Exception):
    def __init__(self, message: str = None) -> None:
        super().__init__(message)


class DataWrapper(ABC):
    """Abstract Base Class serving as Component Class that implements the Composite 
       pattern
       :param META_PARAMS: list - carry arguments from composite classes to leaf classes but are
        not directly part of a leaf class e.g. a relative path is given in a nested field
        the full path can only the constructed if the base path is propagated down from it's parent nodes """

    META_PARAMS = ["base_path", "log_level"]

    def _propagate_to_subclass(self,
                               attr,
                               attr_type: type,
                               actual_type: type,
                               *args,
                               **kwargs) -> None:
        """Method of the Abstract BaseClass DataWrapper:
        When a subclass of DataWrapper is nested inside another one.
        The method will instantiate the nested class with the kwargs provided

        :param attr: str - name of the subclass defined by the parent composite
            dataclass attribute name
        :param attr_type: type - Dataclass of the Leaf from which the attribute is
            instantiated from, determined by the type annotation in the parent
            composite Dataclass
        :param actual_type: type - Checks if the Datatype is valid to pass,
            either dict to pass as **kwargs or list to pass as *args
        **kwargs Keyword arguments to instantiate the leaf class
        :returns: None
        """
        def _call_subclass_with_additional_args():
            """method to pass the META_PARAMS from composite to leaf class"""
            if issubclass(attr_type, Iterable_Baseclass):
                setattr(self, attr, attr_type(**kwargs))
            else:
                setattr(self, attr, attr_type(base_path=self.base_path,
                                              **kwargs))
            return

        if actual_type == "dict":
            _call_subclass_with_additional_args()
        elif actual_type == "list":
            setattr(self, attr, attr_type(*args))

    def __post_init__(self, **kwargs):
        """general post init method that performs type checking and propagates the 
           error upwards
           :**kwargs Keyword arguments to pass for initiating a leaf class instance

           :raises DataParsingError, being re-raised if the Validator class raises an Error
           """
        # set the META_PARAMS module-wide if they are set in the class i.E.
        # the top Level Classes

        for attr, attr_type in self.__annotations__.items():
            sub_attr_type = type(None)
            if hasattr(attr_type, "__origin__"):
                sub_attr_type = attr_type.__args__[0]
                attr_type = getattr(attr_type, "__origin__")

            if not (issubclass(attr_type, DataWrapper) or
                    (issubclass(attr_type, list) and issubclass(sub_attr_type, DataWrapper))):
                continue

            if not hasattr(self, attr):
                continue
            kwargs = getattr(self, attr)
            actual_type = type(kwargs).__name__
            # cast to float automatically if int is given
            if attr_type == float and type(kwargs) == int:
                setattr(self, attr, float(kwargs))
                continue

            if not isinstance(sub_attr_type, type(None)):
                setattr(self,
                        attr,
                        [sub_attr_type(**element) for element in kwargs]
                        )

            if actual_type in ["dict", "list"]\
                and issubclass(attr_type, DataWrapper)\
                    and kwargs is not None:
                # convert dict to given Dataclass if specified
                if actual_type == "list":
                    args = kwargs
                    kwargs = {}
                else:
                    args = []
                try:
                    self._propagate_to_subclass(attr,
                                                attr_type,
                                                actual_type,
                                                *args,
                                                **kwargs)

                except TypeError as e:
                    raise DataParingError(
                        f"'{attr_type.__name__}' {e} from keyword arguments: {kwargs}")
                except ValueError as e:
                    raise DataParingError(
                        f"'{attr_type.__name__}' " +
                        f"got invalid value from field '{attr}': {e}")
                except (FileNotFoundError, IsADirectoryError, NotADirectoryError) as e:
                    raise DataParingError(
                        f"{attr_type}" +
                        f" got invalid path in field '{attr}': {e}")

                except DataParingError as e:
                    raise DataParingError(
                        f"'{attr_type.__name__}' in Class {e}")
                continue

        self.init_logger()

    def init_logger(self):
        """initialize the class logger with the name of the class,
           if a base_path is given, the logs will be written there as well"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger = init_console_logger(self.logger)
        if hasattr(self, "base_path") and self.base_path is not None:
            self.logger = init_file_logger(self.logger, self.base_path)

    def check_passed_args(self, kwargs: dict):
        """check the arguments upon instantiation of a child class,
           gives a warning, if no value is provided"""
        for key in self.__dict__.keys():
            if key == "logger":
                continue
            if key not in kwargs.keys():
                self.logger.warn(
                    f"Fallback for key {key} as it is not specified in params," +
                    f"refer to default params.json for use, using default value {getattr(self, key)}")

    def as_dict(self) -> dict:
        """Converts the a composite class into a flattened dict where key values 
           for Leaf classes are prefixed with it's name e.g. 
            Params{ModelParams{name, structure}, DataParams{image_shape,mask_shape}} -> 
            {Model_Params_name:..., Model_Params_structure:..., DataParams_image_shape:...}"""
        output_dict = {}
        for key, value in self.__dict__.items():
            if type(value) in [int, float, str, bool] or value is None:
                output_dict[key] = value
            elif isinstance(value, pl.Path):
                output_dict[key] = str(value)
            elif type(value) == dict:
                sub_dict = {f"{key}_{value}": sub_value
                            for value, sub_value in value.items()}
                output_dict.update(sub_dict)
            elif issubclass(type(value), DataWrapper):
                append_dict = {f"{value.__class__.__name__}_{k}": v
                               for k, v in value.__dict__.items()}
                append_dict.pop(f"{value.__class__.__name__}_logger")
                output_dict.update(append_dict)
            elif isinstance(value, logging.Logger):
                pass
            else:
                self.logger.warn(
                    f"Unsupported datastructure, for dict unpacking: {value}")

        return output_dict

    def as_nested_dict(self) -> dict:
        """get nested dict representation of a composite class"""
        output_dict = {}
        for key, value in self.__dict__.items():
            if key in self.META_PARAMS + ["distribution"]:
                continue
            if type(value) in [int, float, str, list] or value is None:
                output_dict[key] = value
            elif type(value) == dict:
                output_dict.update({key: value})
            elif issubclass(type(value), DataWrapper):
                output_dict.update({key: value.as_nested_dict()})
            elif isinstance(value, pl.Path):
                output_dict[key] = str(value)
        return output_dict


class Iterable_Baseclass(DataWrapper):
    """Baseclass that implements subscription magic methods"""

    def __init__(self) -> None:
        super().__init__()

    def __iter__(self):
        return iter(self.as_dict().values())

    def __getitem__(self, item):
        return list(self)[item]
