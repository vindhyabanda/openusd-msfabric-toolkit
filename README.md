# OpenUSD Toolkit

This package provides utilities for working with [OpenUSD](https://openusd.org/) files in Microsoft Fabric Notebooks, including metadata extraction, fuzzy matching with external asset tables, and enrichment of USD files.

## Features

- Extract and flatten metadata from USD files using Spark
- Match USD assets to external sources using fuzzy matching
- Enrich USD files with matched identifiers
- Output Delta tables for downstream usage

## Installation

```bash
pip install git+https://github.com/yourusername/openusd-toolkit.git