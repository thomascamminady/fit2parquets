import glob
import os
from dataclasses import dataclass
from functools import partial

import fire
import numpy as np
import polars as pl
import xyzservices.providers as xyz
from bokeh.layouts import gridplot
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure, output_file, save


@dataclass
class ParsedFitDataFrameCollection:
    record_mesgs: pl.DataFrame


class Plotter:
    @staticmethod
    def vis(root: str) -> None:
        """Visualize the parsed .fit data.

        Parameters
        ----------
        root : str
            Root directory containing the parsed .fit data in parquet or CSV format.
        """
        dfc = Plotter._get_dataframe_collection(root)

        figure_kwargs = {
            "x_axis_label": "Time",
            "x_axis_type": "datetime",
            "width": 1000,
            "height": 300,
            "x_range": figure().x_range,
            "tools": "box_zoom,pan,reset,save,hover,lasso_select,box_select",
        }
        map_kwargs = {
            "x_axis_label": "Longitude",
            "y_axis_label": "Latitude",
            "width": 1000,
            "height": 500,
            "x_range": figure().x_range,
            "y_range": figure().y_range,
            "x_axis_type": "mercator",
            "y_axis_type": "mercator",
            "tools": "box_zoom,pan,reset,save,hover,lasso_select,box_select",
        }
        mark_kwargs = {
            "x": "timestamp",
            "source": ColumnDataSource(dfc.record_mesgs.to_dict()),
            "line_width": 2,
        }

        create_plot = partial(
            Plotter._create_plot,
            figure_kwargs=figure_kwargs,
            mark_kwargs=mark_kwargs,
        )
        create_map = partial(
            Plotter._create_map,
            cds=ColumnDataSource(dfc.record_mesgs.to_dict()),
            figure_kwargs=map_kwargs,
        )

        # Arrange plots in a grid
        grid = gridplot(
            children=[
                [create_map()],
                [create_plot(y="heart_rate", line_color="red")],
                [create_plot(y="cadence", line_color="blue")],
                [create_plot(y="enhanced_speed", line_color="black")],
                [create_plot(y="enhanced_altitude", line_color="green")],
                [create_plot(y="temperature", line_color="purple")],
                [create_plot(y="distance", line_color="orange")],
            ]
        )
        output_file(
            filename=os.path.join(root, "record_mesg.html"), title="Records"
        )
        save(grid)

    @staticmethod
    def _create_plot(
        y: str,
        line_color: str,
        figure_kwargs: dict,
        mark_kwargs: dict,
    ) -> figure:
        p = figure(y_axis_label=y, **figure_kwargs)
        p.line(y=y, legend_label=y, line_color=line_color, **mark_kwargs)
        p.scatter(y=y, legend_label=y, color=line_color, **mark_kwargs)
        return p

    @staticmethod
    def _create_map(cds: ColumnDataSource, figure_kwargs: dict) -> figure:
        # plot position_lat and position_long on a zoomable map
        map_lat_long = figure(**figure_kwargs)
        map_lat_long.line(
            x="position_long",
            y="position_lat",
            source=cds,
        )
        map_lat_long.scatter(
            x="position_long",
            y="position_lat",
            source=cds,
        )
        map_lat_long.add_tile(xyz.OpenStreetMap.Mapnik)  # type: ignore
        return map_lat_long

    @staticmethod
    def _get_dataframe_collection(root: str) -> ParsedFitDataFrameCollection:
        def lon_to_web_mercator(lon):
            k = 6378137
            lon = lon / 2**31 * 180
            return lon * (k * np.pi / 180.0)

        def lat_to_web_mercator(lat):
            k = 6378137
            lat = lat / 2**31 * 180
            return np.log(np.tan((90 + lat) * np.pi / 360.0)) * k

        return ParsedFitDataFrameCollection(
            record_mesgs=(
                Plotter._load_dataframe(root, "record_mesgs").with_columns(
                    pl.col("position_lat").map_elements(
                        lat_to_web_mercator, return_dtype=pl.Float64
                    ),
                    pl.col("position_long").map_elements(
                        lon_to_web_mercator, return_dtype=pl.Float64
                    ),
                )
            )
        )

    @staticmethod
    def _load_dataframe(root: str, dataset: str) -> pl.DataFrame:
        # find file in folder that ends with .csv or .parquet
        files = glob.glob(os.path.join(root, f"{dataset}.*"))
        if len(files) == 0:
            raise ValueError(f"Could not find data: {dataset}")
        elif files[0].endswith(".csv"):
            return pl.read_csv(os.path.join(root, files[0]))
        elif files[0].endswith(".parquet"):
            return pl.read_parquet(os.path.join(root, files[0]))
        else:
            raise ValueError(
                f"Data must be in either CSV or parquet format, found {files[0]}"
            )


def main() -> None:
    fire.Fire(Plotter.vis)


if __name__ == "__main__":
    main()
