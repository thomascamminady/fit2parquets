"""
.. include:: ../README.md
"""

import os
from typing import Any, TypeVar

import polars as pl
from garmin_fit_sdk import Decoder, Stream

from fit2parquets.utils.logger import logger

T = TypeVar("T")


def fit2parquets(
    fit_file: str,
    write_to_folder_in_which_fit_file_lives: bool = True,
    alternate_folder_path: str = "",
) -> None:
    """
    Converts a .fit file to a set of parquet files.

    Parameters
    ----------
    fit_file : str
        The path to the .fit file.
    write_to_folder_in_which_fit_file_lives : bool, optional
        If True, writes the parquet files to the same folder as the .fit file, by default True.
    alternate_folder_path : str, optional
        The path to the folder where the parquet files will be written, by default "".

    Raises
    ------
    ValueError
        If the input file is not a .fit file.

    Examples
    --------
    ```python
    import polars as pl

    from fit2parquets import fit2parquets

    fit2parquets("Afternoon_Run.fit")
    df = pl.read_parquet("Afternoon_Run/record_mesgs.parquet")
    ```

    """
    df_dict = _fit2dfdict(fit_file)

    folder = _resolve_path(
        fit_file, write_to_folder_in_which_fit_file_lives, alternate_folder_path
    )

    # Write each dataframe to a parquet file.
    for key, value in df_dict.items():
        value.write_parquet(os.path.join(folder, f"""{key}.parquet"""))


def _read_fit_file(
    fit_file: str,
) -> tuple[dict[Any, Any], list[Any]]:
    stream = Stream.from_file(fit_file)
    decoder = Decoder(stream)

    messages, errors = decoder.read(
        apply_scale_and_offset=True,
        convert_datetimes_to_dates=True,
        convert_types_to_strings=True,
        enable_crc_check=True,
        expand_sub_fields=True,
        expand_components=True,
        merge_heart_rates=True,
        mesg_listener=None,
    )
    return messages, errors


def _dictkeys2str(
    list_of_dicts: list[dict[str | int, T]],
) -> list[dict[str, T]]:
    return [{str(k): v for k, v in d.items()} for d in list_of_dicts]


def _fit2dfdict(fit_file: str) -> dict[str, pl.DataFrame]:
    messages, errors = _read_fit_file(fit_file)
    df_dict: dict[str, pl.DataFrame] = {}
    for key, value in messages.items():
        try:
            df_dict[key] = pl.DataFrame(_dictkeys2str(value))
        except Exception as e:
            logger.info(key, e)
    return df_dict


def _resolve_path(
    fit_file: str,
    write_to_folder_in_which_fit_file_lives: bool = True,
    alternate_folder_path: str = "",
) -> str:
    if not fit_file.endswith(".fit"):
        raise ValueError("Input file must be a .fit file.")

    if write_to_folder_in_which_fit_file_lives:
        # Writes to the exact same location that the
        # .fit file is located.
        folder = fit_file.replace(".fit", "")
    else:
        # Writes to the folder specified by the user.
        folder = alternate_folder_path
    # If folder does not exist, create it.
    if not os.path.exists(folder):
        os.makedirs(folder)
    return folder
