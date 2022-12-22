from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="substreams",
    version="0.0.4",
    packages=[".substreams"],
    author="Ryan Sudhakaran",
    author_email="ryan.sudhakaran@messari.io",
    description="Light Python wrapper for consuming Substreams",
    url="https://github.com/messari/substreams-python",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
)
