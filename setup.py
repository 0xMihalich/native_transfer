import shutil
from setuptools import setup, find_packages


shutil.rmtree("build", ignore_errors=True,)
shutil.rmtree("native_transfer.egg-info", ignore_errors=True,)

with open(file="README.md", mode="r", encoding="utf-8",) as f:
    long_description = f.read()

setup(name="native_transfer",
      version="0.0.4",
      packages=find_packages(),
      author="0xMihalich",
      author_email="bayanmobile87@gmail.com",
      description="Class for working with Clickhouse Native Format",
      url="https://github.com/0xMihalich/native_transfer",
      long_description=long_description,
      long_description_content_type="text/markdown",
      zip_safe=False,)
