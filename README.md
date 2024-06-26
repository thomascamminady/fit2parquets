# fit2parquets

Convert `.fit` files to `.parquet` files using Garmin's FIT SDK.

## Installation

```bash
poetry add fit2parquets
```

## Usage

```python

from fit2parquets import fit2parquets
fit2parquets("Afternoon_Run.fit")

# optional
import polars as pl
df = pl.read_parquet("Afternoon_Run/record_mesgs.parquet")
```

## Credits

Garmin's FIT SDK is licensed under the [Flexible and Interoperable Data Transfer (FIT) Protocol](https://developer.garmin.com/fit/download/).

This package was created with [`cookiecutter`](https://github.com/audreyr/cookiecutter) and [`thomascamminady/cookiecutter-pypackage`](https://github.com/thomascamminady/cookiecutter-pypackage), a fork of [`audreyr/cookiecutter-pypackage`](https://github.com/audreyr/cookiecutter-pypackage).
