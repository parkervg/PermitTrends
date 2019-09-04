import os
import dask.dataframe as dd
from datetime import datetime
import pandas as pd
import numpy as np
import glob
import logging
import regex as re
import logging

class PreProcess:
    """
    Filter column values can be both strings and integers.
    If an integer value is required to filter, the value will be a list where the first value is the bottom range, and the second is the top.
    If the second value is empty, only the bottom range is used in filtering.
    e.x. filter_cols = {"Permit Class Mapped": ["Residential"],
                                "Permit Type Desc": ["Building Permit"],
                                "Total Lot SQFT": [1000,]}   <<< All permits with sqft greater than 1,000
    """

    def __init__(self, kwargs):
        self.csv_path = kwargs["csv_path"] # str
        self.usecols = kwargs["usecols"] # list
        self.min_dt = kwargs["min_dt"] # str, minimum date (e.x. '2012-01-01')
        self.date_col = kwargs["date_col"] # str
        self.date_index = kwargs.get("date_index", False) # bool, whether to make the index the date
        self.filter_cols = kwargs.get("filter_cols", None) # dict
        if self.filter_cols: self.filter_cols = dict(self.filter_cols)
        self.max_dt = kwargs.get("max_dt", None) # str, maximum date (e.x. '2017-01-01')
        self.lat_lon_col = kwargs.get("lat_lon_col", None)
        self.remove_values = kwargs.get("remove_values", None) # dict
        if self.remove_values: self.remove_values = dict(self.remove_values)

    def split_lat_lon(self, df, lat_lon_col):
        lats, lons = [], []
        for val in df[lat_lon_col].tolist():
            lat = re.search(r"(?<=.+?)\d{1,3}\.\d+(?=\D)", val).group()
            lon = re.search(r"(?<=, )-?\d{2,3}\.\d+(?=\D)", val).group()
            lats.append(lat)
            lons.append(lon)
        df["latitude"] = lats
        df["longitude"] = lons
        df["latitude"] = df["latitude"].astype(float)
        df["longitude"] = df["longitude"].astype(float)
        return df

    def file_size(self):
        """
        Returns file size in MB.
        """
        try:
            return os.path.getsize(self.csv_path) / 1000000
        except FileNotFoundError:
            bytes = 0
            for file in glob.glob(self.csv_path):
                bytes += os.path.getsize(file)
            return bytes / 1000000

    def load_csv(self):

        if self.file_size() >= 150:
            print()
            print("Processing CSV with Dask...")
            print()
            """
            File is bigger than 150 MB. Dask becomes useful.
            """
            ddf = dd.read_csv(self.csv_path,
                              parse_dates = [self.date_col], infer_datetime_format = True,
                              usecols = self.usecols + list(self.filter_cols.keys()) + list(self.remove_values.keys()))

            return ddf.dropna(subset = self.usecols)

        else:
            print()
            print("Processing CSV with Pandas...")
            print()
            """
            File is smaller than 150MB. Pandas will work fine.
            """
            try:
                df = pd.read_csv(self.csv_path,
                                  parse_dates = [self.date_col], infer_datetime_format = True,
                                  usecols = self.usecols + list(self.filter_cols.keys()) + list(self.remove_values.keys()))

            except FileNotFoundError: # Inputted csv_path is a directory, and each file must be read and merged together
                df = pd.concat([pd.read_csv(f, parse_dates = [self.date_col], infer_datetime_format = True,
                usecols = self.usecols) for f in glob.glob("{}/*.csv".format(self.csv_path))])
            return df.dropna(subset = self.usecols)


    def filter_df(self, df):
        if not isinstance(df, pd.DataFrame): dask = True
        else: dask = False

        self.min_dt = datetime.strptime(self.min_dt, "%Y-%m-%d")

        if self.max_dt:
            self.max_dt = datetime.strptime(self.max_dt, "%Y-%m-%d")
            df = df[(self.min_dt <= df[self.date_col]) & (self.max_dt >= df[self.date_col])]
        else:
            df = df[self.min_dt <= df[self.date_col]]

        if dask: df = df.compute() # Turning dask df into pandas
        if self.filter_cols:
            for column, values in self.filter_cols.items():
                if isinstance(values[0], int): # Int filtering is inputted
                    try: # Works if a top and bottom range is supplied.
                        df = df[(df[column] >= int(values[0])) & (df[column] <= int(values[1]))]
                    except IndexError: # There is no top range.
                        df = df[df[column] >= int(values[0])]
                else: # String filtering is inputted
                    df = df[df[column].isin(values)]
        if self.remove_values:
            for column, values in self.remove_values.items():
                for value in values:
                    df = df[df[column] != value]

        # Splitting latitude and longitude if it appears in the same column
        if self.lat_lon_col:
            df = self.split_lat_lon(df, self.lat_lon_col)

        if self.date_index: return df.set_index(self.date_col)
        else: return df.reset_index()


    def Process(self):
        df = self.load_csv()
        return self.filter_df(df)
        print()
        print("Dataframe processed and cleaned. Beginning analysis...")
        print()
