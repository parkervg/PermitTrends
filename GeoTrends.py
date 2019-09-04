import dask.dataframe as dd
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg') # To be able to run command line
import matplotlib.pyplot as plt
import matplotlib.font_manager
import statistics
import operator
import math
import regex as re
import logging
import glob
from sklearn.cluster import DBSCAN
from scipy.spatial.distance import pdist, squareform
from tools.BoundingBox import BoundingBox
import requests
import logging
from tools.bucket_storage import send_to_client
import time
api_key = "HI3nvj7AiMjuTTFrXatqUlem2vXceoX0"

"""
# Fixing weird basemap thing
import conda
conda_file_dir = conda.__file__
conda_dir = conda_file_dir.split('lib')[0]
proj_lib = os.path.join(os.path.join(conda_dir, 'share'), 'proj')
os.environ["PROJ_LIB"] = proj_lib
"""
from mpl_toolkits.basemap import Basemap


class GeoTrends():
    """
    Using DBSCAN, areas in a set historical timerange are clustered together, and the top results specified by "top_areas_cutoff" are saved.
    Bounding Boxes are drawn in each of the top results, and modern-date is filtered so that any coordinate within a historically popular bounding box is dismissed.
    The remaining modern data is clustered again with DBSCAN and plotted on a BaseMap, revealing previously overlooked areas of increasing development.
    """
    def __init__(self, kwargs):

        self.lat_col = kwargs["lat_col"] #str
        self.lon_col = kwargs["lon_col"] #str
        self.top_areas_cutoff = kwargs["top_areas_cutoff"] #int
        self.num_results = kwargs["num_results"] #int
        self.max_miles = kwargs["max_miles"] #float, Refers to the maximum distance DBSCAN allows multiple points to be a single cluster
        self.location = kwargs["location"] # str
        self.filter_cols = kwargs["filter_cols"] # dict
        self.min_dt = kwargs["min_dt"] # str
        self.display_kwargs = kwargs["display_kwargs"] # bool

    def top_areas(self):
        return self._top_areas


    def get_coords(self, df):
        return df.as_matrix(columns = [self.lat_col, self.lon_col])


    def db_scan(self, df, coordinates):
        """
        Density-based clustering of coordinates.
        Only points within self.max_miles distance from each other
        have the chance of being in a cluster.
        """
        db = DBSCAN(eps = float(self.max_miles)/3959, # Multiplied by 3959 to convert it to radians.
                    min_samples = 15,
                    algorithm = "ball_tree",
                    metric = "haversine").fit(coordinates)
        df["label"] = db.labels_
        return df

    def get_top_areas(self, df):
        """
        Establishing historically popular areas to be filtered out.
        """
        counts = df["label"].value_counts()
        boxes_array = []
        for label in set(df["label"].tolist()):
            if label != -1: # The noise label
                coords = list(zip(df[self.lat_col][df["label"] == label].tolist(),df[self.lon_col][df["label"] == label].tolist()))
                boxes_array.append([BoundingBox(coords).get_box(), counts[label]])
        self._top_areas = [i[0] for i in sorted(boxes_array, key = lambda x: x[1], reverse = True)][:self.top_areas_cutoff]


    def _in_bounding_box(self, coords, bb):
        """
        Given a (lat, lon) coord, determines if the point is in a specific BoundingBox (bb).
        """
        if (bb[0] <= coords[0] <= bb[1]) and (bb[2] <= coords[1] <= bb[3]):
            return True
        else: return False

    def calculate_trending(self, new_df, coords):
        """
        Filters out those coordinates in the historical baseline of top areas
        to get only recently trending areas.
        """
        good_indexes = []
        for ix, coord in enumerate(coords):
            add = True
            for area in self._top_areas:
                if self._in_bounding_box(coord, area):
                    add = False
            if add:
                good_indexes.append(ix)
        return new_df.iloc[good_indexes], new_df.iloc[good_indexes].as_matrix(columns = [self.lat_col, self.lon_col])


    def get_centers(self, df):
        """
        Averages lat and lon from each dbscan cluster to get cluster centers.
        """
        counts = df["label"].value_counts()
        labels = df["label"].tolist()
        cluster_centers = {}
        for label in set(labels):
            if label != -1: # The noise label
                avg_lat = statistics.mean(df[self.lat_col][df["label"] == label].tolist())
                avg_lon = statistics.mean(df[self.lon_col][df["label"] == label].tolist())
                count = counts[label]
                cluster_centers[(avg_lat, avg_lon)] = count
        return sorted(cluster_centers.items(), key = operator.itemgetter(1), reverse = True)


    def plot_map(self, cluster_centers, intersections, since_year):
        """
        Plots BaseMap of results.
        """
        cluster_centers = cluster_centers[:self.num_results]
        fig, ax = plt.subplots(figsize=(25,25))
        plt.style.use("seaborn-darkgrid")
        map_cushion = .01
        map = Basemap(projection = 'merc',
                      resolution = 'l',
                      lon_0 = min([x[0][1] for x in cluster_centers]),
                      lat_0= min([x[0][0] for x in cluster_centers]),
                      llcrnrlon = min([x[0][1] for x in cluster_centers]) - (map_cushion +.15), # Adding space for text box in upper left
                      llcrnrlat= min([x[0][0] for x in cluster_centers]) - map_cushion,
                      urcrnrlon = max([x[0][1] for x in cluster_centers]) + map_cushion,
                      urcrnrlat = max([x[0][0] for x in cluster_centers]) + map_cushion,
                      epsg = 4269)
        map.arcgisimage(service= "World_Street_Map",
                        xpixels = 2000,
                        verbose = False,
                        dpi = 400,
                        zorder = 0)

        sc = map.scatter([x[0][1] for x in cluster_centers], [x[0][0] for x in cluster_centers],
                        c = [val[1] for val in cluster_centers],
                        latlon = True,
                        alpha = 1,
                        s = 500,
                        cmap="autumn_r",
                        marker = "o")
        for ix, x in enumerate(cluster_centers):
            plt.annotate(ix + 1, (x[0][1] - .001, x[0][0] - .0002), weight = "bold")
        v = np.linspace(0, 5000, 15, endpoint=True)
        plt.suptitle("{}- Recently Popular Areas for Permit Applications".format(self.location), size = 35, y = .80, x = .46, weight = "bold")
        plt.title("Since {}, within a {} mile radius".format(since_year, self.max_miles), size = 18, color = "#525756", y = 1.08)
        cbar = plt.colorbar(sc)
        cbar.set_label("Number of Permits", rotation = 270, labelpad = 35, size = 15, weight = "bold")

        # Text box of street intersections
        textstr = ""
        for ix, intersection in enumerate(intersections[:self.num_results]):
            textstr += "\n {}) {} \n".format(ix+1, intersection)
        props = dict(boxstyle='round', facecolor='black', alpha=0.8)
        plt.text(0.01, 1, textstr,
                 fontsize=14,
                 weight ="bold",
                 verticalalignment='top',
                 bbox=props,
                 color ="white",
                 transform=ax.transAxes)

        if self.display_kwargs:
            # Text box for kwargs
            kwargs_textstr = ""
            if self.filter_cols:
                kwargs_textstr += "filter_cols: {} \n".format(self.filter_cols)
            kwargs_textstr += "min_dt: {} \n".format(self.min_dt)
            kwargs_textstr += "top_areas_cutoff: {}".format(self.top_areas_cutoff)
            props2 = dict(boxstyle='round', facecolor='black', alpha=0.8)
            plt.text(0.128, .29, kwargs_textstr,
                     fontsize=13,
                     verticalalignment='top',
                     bbox=props2,
                     color ="white",
                     transform=plt.gcf().transFigure)

        plt.savefig("outputs/image.png", dpi = 400, bbox_inches = "tight", pad_inches = .5)
        #time.sleep(5) # For basemap to fully render.
        id = send_to_client("outputs/image.png")
        print("")
        print("YOUR BUCKET ID IS {}".format(id))
        print("")
        return id


    def get_intersections(self, coords):
        """
        Uses MapQuest's API in get_intersections() to find nearest street and cross-street.
        """
        coords = str(coords).replace("(","").replace(")","")
        response = requests.get("http://www.mapquestapi.com/geocoding/v1/reverse?key={}&location={}&includeRoadMetadata=true&includeNearestIntersection=true".format(api_key, coords))
        street1 = re.sub("\d+", "", response.json()["results"][0]["locations"][0]["street"]).strip()
        street2 = response.json()["results"][0]["locations"][0]["nearestIntersection"]["streetDisplayName"]
        return "{} and {}".format(street1,street2)


    def find_intersections(self, cluster_centers):
        """
        Returns list of intersections corresponding to each cluster in cluster_centers.
        """
        intersections = []
        for center in cluster_centers[:self.num_results]:
            intersections.append(self.get_intersections(center[0]))
        print("")
        print("The following areas have experienced a recent uptick in permit applications:")
        print("____________________________________________________________________________________")
        for ix, i in enumerate(intersections):
            print("{}) {} >>>>>>>>> {} permits since 2017".format(ix+1, i, cluster_centers[ix][1]))
        print("____________________________________________________________________________________")
        print("")
        return intersections
