from setuptools import find_packages, setup

setup(
    name="airweave_risk",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "dagster>=1.5.0",
        "watchdog>=3.0.0",
        "prometheus_client>=0.16.0",
    ],
)
