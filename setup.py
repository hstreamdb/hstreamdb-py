import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    install_requires = fh.read().splitlines()

setuptools.setup(
    name="hstreamdb",
    version="0.0.1",
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
