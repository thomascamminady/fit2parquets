import os
from typing import Any, Literal, TypeVar

import garmin_fit_sdk
import polars as pl
from polars.exceptions import ComputeError

from fit2parquets.utils.logger import logger


class Parser:
    T = TypeVar("T")

    @staticmethod
    def fit2parquets(
        fit_file: str,
        write_to_folder_in_which_fit_file_lives: bool = True,
        alternate_folder_path: str = "",
        output_format: Literal["parquet", "csv"] = "parquet",
        hive: bool = False,
    ) -> None:
        """
        Converts a .fit file to a set of parquet or CSV files.

        Parameters
        ----------
        fit_file : str
            The path to the .fit file.
        write_to_folder_in_which_fit_file_lives : bool, optional
            If True, writes the output files to the same folder as the .fit file, by default True.
        alternate_folder_path : str, optional
            The path to the folder where the output files will be written, by default "".
        output_format : Literal["parquet", "csv"], optional
            The format of the output files, by default "parquet".
        hive:  bool, optional
            Whether or not to write the output in a hive compatible format, by default False.

        Raises
        ------
        ValueError
            If the input file is not a .fit file or if the output format is invalid.

        Examples
        --------
        ```python
        import polars as pl

        from fit2parquets import fit2parquets

        fit2parquets("Afternoon_Run.fit")
        df = pl.read_parquet("Afternoon_Run/record_mesgs.parquet")
        ```
        """
        df_dict = Parser._fit2dfdict(fit_file=fit_file)

        folder = Parser._resolve_path(
            fit_file=fit_file,
            write_to_folder_in_which_fit_file_lives=write_to_folder_in_which_fit_file_lives,
            alternate_folder_path=alternate_folder_path,
            hive=hive,
        )

        # Write each dataframe to a parquet or CSV file.
        for key, value in df_dict.items():
            try:
                if hive:
                    subfolder = os.path.join(folder, f"message={key}")
                    if not os.path.exists(subfolder):
                        os.makedirs(subfolder)
                    file = f"""message={key}/data.{output_format}"""

                else:
                    file = f"""{key}.{output_format}"""

                destination = os.path.join(folder, file)

                if output_format == "parquet":
                    value.write_parquet(destination)
                elif output_format == "csv":
                    value.write_csv(destination)
                else:
                    raise ValueError(
                        "output_format must be either 'parquet' or 'csv'."
                    )
            except TypeError:
                pass
            except ComputeError:
                pass

    @staticmethod
    def _read_fit_file(
        fit_file: str,
    ) -> tuple[dict[Any, Any], list[Any]]:
        """
        Reads a .fit file and returns its messages and errors.

        Parameters
        ----------
        fit_file : str
            The path to the .fit file.

        Returns
        -------
        tuple[dict[Any, Any], list[Any]]
            A tuple containing the messages and errors from the .fit file.
        """
        stream = garmin_fit_sdk.Stream.from_file(fit_file)
        decoder = garmin_fit_sdk.Decoder(stream)

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

    @staticmethod
    def _fit2dfdict(fit_file: str) -> dict[str, pl.DataFrame]:
        """
        Converts the messages from a .fit file to a dictionary of DataFrames.

        Parameters
        ----------
        fit_file : str
            The path to the .fit file.

        Returns
        -------
        dict[str, pl.DataFrame]
            A dictionary where the keys are message types and the values are DataFrames.
        """
        messages, errors = Parser._read_fit_file(fit_file)
        df_dict: dict[str, pl.DataFrame] = {}
        for key, value in messages.items():
            try:
                df_dict[key] = pl.DataFrame(Parser._dictkeys2str(value))
            except Exception as e:
                logger.info(e)
        return df_dict

    @staticmethod
    def _dictkeys2str(
        list_of_dicts: list[dict[str | int, T]],
    ) -> list[dict[str, T]]:
        """
        Converts the keys of dictionaries in a list to strings.

        Parameters
        ----------
        list_of_dicts : list[dict[str | int, T]]
            A list of dictionaries with keys that are either strings or integers.

        Returns
        -------
        list[dict[str, T]]
            A list of dictionaries with keys converted to strings.
        """
        return [{str(k): v for k, v in d.items()} for d in list_of_dicts]

    @staticmethod
    def _resolve_path(
        fit_file: str,
        write_to_folder_in_which_fit_file_lives: bool,
        alternate_folder_path: str,
        hive: bool,
    ) -> str:
        """
        Resolves the output folder path based on the input parameters.

        Parameters
        ----------
        fit_file : str
            The path to the .fit file.
        write_to_folder_in_which_fit_file_lives : bool
            If True, writes the output files to the same folder as the .fit file.
        alternate_folder_path : str
            The path to the folder where the output files will be written.
        hive:  bool
            Whether or not to write the output in a hive compatible format.

        Returns
        -------
        str
            The resolved folder path.

        Raises
        ------
        ValueError
            If the input file is not a .fit file.
        """
        if not fit_file.endswith(".fit"):
            raise ValueError("Input file must be a .fit file.")

        if write_to_folder_in_which_fit_file_lives:
            # Writes to the exact same location that the
            # .fit file is located.
            folder = fit_file.replace(".fit", "")
        else:
            # Writes to the folder specified by the user.
            folder = alternate_folder_path

        if hive:
            # Prepend "file=" to the basename of the folder.
            basename = os.path.basename(folder)
            everything_but_basename = os.path.dirname(folder)
            folder = os.path.join(everything_but_basename, f"file={basename}")

        # If folder does not exist, create it.
        if not os.path.exists(folder):
            os.makedirs(folder)
        return folder
