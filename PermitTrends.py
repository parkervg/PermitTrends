import dask.dataframe as dd
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg') # To be able to run command line
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import statistics
import operator
import math
import regex as re
import csv
import logging
from tools.bucket_storage import send_to_client

class PermitTrends():
    """
    An owner’s historical baseline is calculated by finding their average application rate up until 4 months ago.
    Then, their rate of application in the most recent 4 months is compared to their historical baseline.
    The infographic shows the owners that, in recent months, have drastically increased their rate of permit application.
    """

    def __init__(self, kwargs):

        self.owners_col = kwargs["owners_col"] # String
        self.top_owners_cutoff = kwargs["top_owners_cutoff"] # Int
        self.resample_size = kwargs["resample_size"] # Resample sting. e.x. "4M" = 4 months, "1D" = 1 day
        self.location = kwargs["location"] # str
        self.display_kwargs = kwargs["display_kwargs"] # bool
        self.filter_cols = kwargs["filter_cols"] # dict
        self.min_dt = kwargs["min_dt"]

        """
        Filter column values can be both strings and integers.
        If an integer value is required to filter, the value will be a list where the first value is the bottom range, and the second is the top.
        If the second value is empty, only the bottom range is used in filtering.
        e.x. filter_cols = {"Permit Class Mapped": ["Residential"],
                                    "Permit Type Desc": ["Building Permit"],
                                    "Total Lot SQFT": [1000,]}   <<< All permits with sqft greater than 1,000
        """

        # Linear function to find an appropriate distance to factor in for current trends.
        # Set per month, so 1 month yields a lookback of 4, and 8 yields 1.
        self._lookback = math.floor((-(3/7) * int(re.search(r"\d+", self.resample_size).group())) + (31/7))


    def create_owner_dict(self, df):
        """
        Finding permits issued by owners within a timeframe (resample_size).
        """
        grouper = df.groupby([pd.Grouper(freq = self.resample_size), self.owners_col])
        df["permit_count"] = 1
        grouped_df = grouper.permit_count.count()
        str_dt_bins = list(grouped_df.index.levels[0].astype(str))
        self._dt_bins = grouped_df.index.levels[0]

        owner_dict = {}
        for ix in list(grouped_df.index):
            date_str = re.search(r"\d\d\d\d-\d\d-\d\d", str(ix[0])).group()
            owner = ix[1]
            bins_index = str_dt_bins.index(date_str)
            permit_count = grouped_df[ix]
            try:
                owner_dict[owner][bins_index] = permit_count
            except:
                owner_dict[owner] = [0] * len(str_dt_bins) # Owner has not been seen yet. Creating array of 0s across date bins.
                owner_dict[owner][bins_index] = permit_count
        return owner_dict


    def create_growth_dict(self, owner_dict):
        """
        For each owner, assigns percentage increase in recent months compared to historical baseline.
        """
        output = {}
        for owner, permits in owner_dict.items():
            baseline = self.get_historical_baseline(permits)
            change = self.compare_current(permits, baseline)
            output[owner] = change
        output = sorted(output.items(), key = operator.itemgetter(1), reverse = True)
        print("")
        print("The following owners have been increasing their permit application rate lately:")
        print("____________________________________________________________________________________")
        for owner in output[:10]:
            print("")
            print(owner[0], "  >>>>>> Applying at a rate {}x more than their historical baseline.".format(owner[1]))
        print("____________________________________________________________________________________")
        print("")
        return output


    def visualize_permits(self, df, owner_dict, growth_dict):
        """
        Creates 9 subplots, charting owner's activity across self.resample_size.
        """
        counts = []
        for val in growth_dict[:9]:
            for count in owner_dict[val[0]]:
                counts.append(count)
        max_x = max(counts) + 10 # Most permits in a 4 month span by owner
        fig, ax = plt.subplots(figsize = (20, 15))
        palette = plt.get_cmap('tab10')
        plt.style.use("seaborn-darkgrid")
        for ix, val in enumerate(growth_dict[:9]):
            owner = val[0]
            x = self._dt_bins[:-1]
            y = [val for val in owner_dict[owner]][:-1]
            plt.subplot(3,3,ix+1)
            if ix not in [0, 3, 6]:
                plt.tick_params(labelleft = False)
            if ix not in [6, 7, 8]:
                plt.tick_params(labelbottom = False)
            plt.plot(x, y, linewidth=1.9, alpha=0.9, label = owner, color = palette(ix), marker=".")
            plt.ylim(0, max_x)
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=12))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
            plt.title(owner, loc='center', fontsize=14, fontweight=20, color=palette(ix))
        plt.suptitle("{} Owners with Recent Increases in Permit Applications".format(self.location), fontsize = 27, weight = "bold", color = "black", style = "italic")
        plt.figtext(.43, .93, "Charted across {} Chunks".format(self.resample_size), fontsize = 15, fontweight = 40, color = "#525756")
        fig.text(0.5, 0.08, "Date", ha="center", va="center", fontsize = 20)
        fig.text(0.06, 0.47, "Number of Permits", ha="center", va="center", rotation=90, fontsize = 20)
        if self.display_kwargs:
            kwargs_textstr = ""
            if self.filter_cols:
                kwargs_textstr += "filter_cols: {} \n".format(self.filter_cols)
            kwargs_textstr += "min_dt: {} \n".format(self.min_dt)
            kwargs_textstr += "top_areas_cutoff: {}".format(self.top_owners_cutoff)
            props2 = dict(boxstyle='round', facecolor='black', alpha=0.8)
            plt.text(0.001, .04, kwargs_textstr,
                     fontsize=13,
                     verticalalignment='top',
                     bbox=props2,
                     color ="white",
                     transform=plt.gcf().transFigure)

        plt.savefig("outputs/image.png", dpi = 400, bbox_inches = "tight", pad_inches = .5)
        id = send_to_client("outputs/image.png")
        print("_______________________________________________________")
        print("YOUR BUCKET ID IS {}".format(id))
        print("_______________________________________________________")
        return id

    def get_historical_baseline(self, permits):
        """
        Returns average of historical activity.
        """
        return statistics.mean(permits[:-self._lookback])

    def compare_current(self, permits, baseline):
        """
        Compares recent activity to baseline.
        If no baseline is established, it is assigned -100 so it's ignored in the final results.
        """
        if baseline != 0 :
            return round(((statistics.mean(permits[-self._lookback:]) - baseline) / baseline), 2)
        else: return -100 # Just so that it doesn't appear when sorted by top owners
