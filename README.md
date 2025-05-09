# OpenUSD Toolkit

This package provides utilities for working with [OpenUSD](https://openusd.org/) files in Microsoft Fabric Notebooks, including metadata extraction, fuzzy matching with external asset tables, and enrichment of USD files.

## Features

- Extract and flatten metadata from USD files using Spark
- Match USD assets to external sources using fuzzy matching
- Enrich USD files with matched identifiers
- Output Delta tables for downstream usage

## Installation

1. Clone this repository.
2. Run `python -m build --wheel`
3. Upload the `.whl` file to your environment.
4. Run `pip install /lakehouse/default/Files/openusd_msfabric_toolkit-0.1.0-py3-none-any.whl --quiet` to use the package in your environment.
