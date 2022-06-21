import os
import re
import setuptools
from pkg_resources import parse_requirements

BASE_DIR = os.path.dirname(__file__)

with open(os.path.join(BASE_DIR, 'hstreamdb', '__init__.py')) as fh:
    version = re.search(r"__version__ = \"(.+)\"", fh.read()).group(1)

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    install_requires = [str(r) for r in parse_requirements(fh)]

setuptools.setup(
    name="hstreamdb",
    version=version,
    author="lambda",
    author_email="lambda@emqx.io",
    description="Python client for HStreamDB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hstreamdb/hstreamdb-py",
    project_urls={
        "Bug Tracker": "https://github.com/hstreamdb/hstreamdb-py/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(exclude=["tests"]),
    install_requires=install_requires,
    python_requires=">=3.7",
)
