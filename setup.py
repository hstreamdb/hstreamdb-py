import setuptools

setuptools.setup(
    name="hstreamdb",
    version="0.0.1",
    author="lambda",
    author_email="lambda@emqx.io",
    description="Python client for HStreamDB",
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
    install_requires=[
        "grpcio",
        "protobuf",
        "hstreamdb-api>=0.0.2",
    ],
    python_requires=">=3.7",
)
