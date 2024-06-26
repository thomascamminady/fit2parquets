import os
import shutil

import polars as pl
import pytest

from fit2parquets import fit2parquets

FIT_FILES = [
    "tests/data/Rund_um_den_Allendorfer_Bahnhof.fit",
    "tests/data/Afternoon_Run.fit",
]
ALTERNATE_PATH = "tests/data2"


@pytest.mark.parametrize("fit_file", FIT_FILES)
def test_parse_fit_file_in_original_location(fit_file):
    fit2parquets(fit_file)

    df = pl.read_parquet(fit_file.replace(".fit", "") + "/record_mesgs.parquet")
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0


@pytest.mark.parametrize("fit_file", FIT_FILES)
def test_parse_fit_file_in_modified_location(fit_file):
    fit2parquets(
        fit_file,
        write_to_folder_in_which_fit_file_lives=False,
        alternate_folder_path=f"{ALTERNATE_PATH}/{os.path.basename(fit_file).replace('.fit','')}",
    )

    df = pl.read_parquet(fit_file.replace(".fit", "") + "/record_mesgs.parquet")
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
