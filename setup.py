from setuptools import setup, find_packages


setup(
    name="pipeline_penguin",
    version="1.0",
    author="DP6",
    description="Pyton library used for validating data pipelines",
    url="https://github.com/DP6/pipeline-penguin",
    packages=find_packages(exclude=("test")),
    package_dir={"pipeline_penguin": "pipeline_penguin"},
    python_requires=">=3.6",
    install_requires=["pandas", "pandas-gbq"],
    include_package_data=True,
)
