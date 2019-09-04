from tools.PreProcess import PreProcess
from GeoTrends import GeoTrends
from PermitTrends import PermitTrends
import regex as re
import logging


def TrendingPermits(kwargs):
    # Inferring the columns to use from the provided keywords
    kwargs["usecols"] = [kwargs["owners_col"], kwargs["date_col"]]
    # Instatiating classes
    processor = PreProcess(kwargs)
    df = processor.Process()

    trendFinder = PermitTrends(kwargs)

    owner_dict = trendFinder.create_owner_dict(df)
    growth_dict = trendFinder.create_growth_dict(owner_dict)

    print()
    print("Creating visualization...")
    print()

    id = trendFinder.visualize_permits(df, owner_dict, growth_dict)

    return id



def TrendingGeos(kwargs):
    # Inferring the columns to use from the provided keywords
    if kwargs["lat_col"] and kwargs["lon_col"]:
        kwargs["usecols"] = [kwargs["lat_col"], kwargs["lon_col"], kwargs["date_col"]]
    else:
        kwargs["usecols"] = [kwargs["lat_lon_col"], kwargs["date_col"]]
        kwargs["lat_col"] = "latitude"
        kwargs["lon_col"] = "longitude"

    # Instatiating classes
    old_processor = PreProcess(kwargs)
    trendFinder = GeoTrends(kwargs)

    # Processing
    old_df = old_processor.Process()
    coords = trendFinder.get_coords(old_df)

    print()
    print("Clustering old_df...")
    print()

    old_df = trendFinder.db_scan(old_df, coords)
    trendFinder.get_top_areas(old_df) # Creates self._top_areas

    # Setting max_dt as min_dt now.
    # Removing max_dt so processor processes everything up to present day.
    kwargs["min_dt"] = kwargs["max_dt"]
    kwargs.pop("max_dt", None) # Removing max_dt kwarg
    since_year = re.search(r"\d\d\d\d", kwargs["min_dt"]).group() # Setting "since_year" for plot
    new_processor = PreProcess(kwargs)
    new_df = new_processor.Process()

    coords = trendFinder.get_coords(new_df)
    new_df, coords = trendFinder.calculate_trending(new_df, coords)
    print()
    print("Clustering new_df...")
    print()
    new_df = trendFinder.db_scan(new_df, coords)

    new_centers = trendFinder.get_centers(new_df)
    intersections = trendFinder.find_intersections(new_centers)

    print()
    print("Creating visualization...")
    print()

    id = trendFinder.plot_map(new_centers, intersections, since_year)

    return id
