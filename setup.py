from setuptools import find_packages, setup

setup(
    name="gp2gp-reports-generator",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=["python-dateutil>=2.8", "PyArrow>=5.0", "polars>=0.9"],
)
