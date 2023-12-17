from setuptools import find_packages, setup

setup(
    name="Energy",
    packages=find_packages(exclude=["energy_tests"]),
    install_requires=[
        "dagster==1.4.*",
        "dagster-cloud",
        "dagster-duckdb",
        "geopandas",
        "kaleido",
        "pandas",
        "plotly",
        "shapely",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
