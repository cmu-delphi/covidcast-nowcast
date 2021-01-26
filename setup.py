from setuptools import setup
from setuptools import find_packages

required = [
    "aiohttp",
    "numpy",
    "pandas",
    "pydocstyle",
    "pytest",
    "pytest-cov",
    "pylint",
    # "delphi-utils",
    "delphi-epidata",
    "statsmodels"
]

setup(
    name="delphi_covidcast_nowcast",
    version="0.1.0",
    description="COVID19 Infection Nowcasts",
    author="Maria Jahja",
    author_email="",
    url="https://github.com/cmu-delphi/covidcast-indicators",
    install_requires=required,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
)
