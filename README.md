# fit2parquets

Convert Garmin `.fit` files to `.parquet` files using Garmin's FIT SDK.

## Installation

```bash
poetry add fit2parquets
```

Available on PyPI [here](https://pypi.org/project/fit2parquets/).

## Usage

```python

from fit2parquets import fit2parquets
fit2parquets("Afternoon_Run.fit")
```

If you want to save the parsed `.parquet` files in a different location (e.g. `some_other_folder`), simply run:

```python
fit2parquets(
    "Afternoon_Run.fit",
    write_to_folder_in_which_fit_file_lives=False,
    alternate_folder_path="some_other_folder",
)
```

This is also as available as a script from the command line directly
```bash
fit2parquets Afternoon_Run.fit --output_format=csv
```


You can read the resulting data via e.g.:

```python
import polars as pl
df = pl.read_parquet("Afternoon_Run/record_mesgs.parquet")
```

## Credits

Garmin's FIT SDK is licensed under the [Flexible and Interoperable Data Transfer (FIT) Protocol](https://developer.garmin.com/fit/download/).

The Python implementation of that SDK is found here: https://github.com/garmin/fit-python-sdk.

This package was created with [`cookiecutter`](https://github.com/audreyr/cookiecutter) and [`thomascamminady/cookiecutter-pypackage`](https://github.com/thomascamminady/cookiecutter-pypackage), a fork of [`audreyr/cookiecutter-pypackage`](https://github.com/audreyr/cookiecutter-pypackage).
