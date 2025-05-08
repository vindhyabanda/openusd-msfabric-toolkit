from setuptools import setup, find_packages

setup(
    name="openusd-msfabric-toolkit",
    version="0.1.0",
    description="A Python package to import, enrich, and contextualize OpenUSD data using Spark on Microsoft Fabric Notebooks",
    author="Vindhya Banda",
    author_email="vindhyabanda@microsoft.com",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.3",
        "fuzzywuzzy",
        "python-Levenshtein",
        "openusd",  # Make sure this is available or handle installation elsewhere
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
