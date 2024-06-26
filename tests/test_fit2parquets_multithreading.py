# # Test that the fit2parquets function works with multithreading

# from multiprocessing import Pool

# import polars as pl

# from fit2parquets import fit2parquets

# FIT_FILES = [
#     "tests/data/Afternoon_Run.fit",
#     "tests/data/Rund_um_den_Allendorfer_Bahnhof.fit",
# ]


# def test_parse_fit_file_in_alternate_location_multithreaded():
#     with Pool(2) as p:
#         p.map(fit2parquets, FIT_FILES)

#     for fit_file in FIT_FILES:
#         df = pl.read_parquet(
#             fit_file.replace(".fit", "") + "/record_mesgs.parquet"
#         )
#         assert isinstance(df, pl.DataFrame)
#         assert len(df) > 0


# # Clean up all the parquet files created during testing.
# def teardown_module():
#     for fit_file in FIT_FILES:
#         folder = fit_file.replace(".fit", "")
#         if os.path.exists(folder):
#             shutil.rmtree(folder)
