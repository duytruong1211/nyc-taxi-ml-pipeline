from setuptools import find_packages, setup

setup(
    name="dagster_ingestion",
    packages=find_packages(exclude=["dagster_ingestion_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
