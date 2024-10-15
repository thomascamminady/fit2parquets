import os
import shutil

import polars as pl
import pytest

from fit2parquets.parser import Parser

FIT_FILES = [
    "tests/data/Rund_um_den_Allendorfer_Bahnhof.fit",
    "tests/data/Afternoon_Run.fit",
]
ALTERNATE_PATH = "tests/data2"
FORMATS = ["parquet", "csv"]


@pytest.mark.parametrize("ending", FORMATS)
@pytest.mark.parametrize("fit_file", FIT_FILES)
def test_parse_fit_file_in_original_location(fit_file, ending):
    Parser.fit2parquets(fit_file, output_format=ending)

    if ending == "parquet":
        df = pl.read_parquet(
            fit_file.replace(".fit", "") + f"/record_mesgs.{ending}"
        )
    else:
        df = pl.read_csv(
            fit_file.replace(".fit", "") + f"/record_mesgs.{ending}"
        )

    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0


@pytest.mark.parametrize("fit_file", FIT_FILES)
@pytest.mark.parametrize("ending", FORMATS)
def test_parse_fit_file_in_modified_location(fit_file, ending):
    Parser.fit2parquets(
        fit_file,
        write_to_folder_in_which_fit_file_lives=False,
        alternate_folder_path=f"{ALTERNATE_PATH}/{os.path.basename(fit_file).replace('.fit','')}",
        output_format=ending,
    )

    if ending == "parquet":
        df = pl.read_parquet(
            fit_file.replace(".fit", "") + f"/record_mesgs.{ending}"
        )
    else:
        df = pl.read_csv(
            fit_file.replace(".fit", "") + f"/record_mesgs.{ending}"
        )
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0


# Clean up all the parquet files created during testing.
def teardown_module():
    for fit_file in FIT_FILES:
        folder = fit_file.replace(".fit", "")
        if os.path.exists(folder):
            shutil.rmtree(folder)
    if os.path.exists(ALTERNATE_PATH):
        shutil.rmtree(ALTERNATE_PATH)
