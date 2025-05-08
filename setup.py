from setuptools import setup, find_packages

setup(
    name="openusd_msfabric_toolkit",  # Use underscores to match import name
    version="0.1.0",
    description="A Python package to import, enrich, and contextualize OpenUSD data using Spark on Microsoft Fabric Notebooks",
    author="Vindhya Banda",
    author_email="vindhyabanda@microsoft.com",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.3",
        "fuzzywuzzy",
        "python-Levenshtein", 
        "usd-core"
    ],
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)