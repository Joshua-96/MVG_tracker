from importlib_metadata import entry_points
from setuptools import setup, find_packages
import re
import pathlib as pl


def get_version():
    filename = "mvg_tracker/__init__.py"
    with open(filename) as f:
        match = re.search(
            r"""^__version__ = ['"]([^'"]*)['"]""", f.read(), re.M
        )
    if not match:
        raise RuntimeError("{} doesn't contain __version__".format(filename))
    version = match.groups()[0]
    return version


def get_install_requires():
    path = pl.Path(__file__).parent
    path = path.joinpath("requirements.txt")
    with open(path) as f:
        content = f.read()
        content_list = content.splitlines()
        return content_list


def main():

    setup(
        name="mvg_tracker",
        packages=find_packages(),
        author="Joshua Ziegler",
        version=get_version(),
        description="script for getting departures and delays of\
        all public transportation within munich",
        install_requires=get_install_requires(),
        url="https://github.com/Joshua-96/MVG_tracker",
        package_data={"mvg_tracker": ["LUTs/*.csv", "config/*.json"]},
        entry_points={
            "console_scripts": ["mvg_tracker=__main__:main"]
        }
    )


if __name__ == "__main__":
    main()
