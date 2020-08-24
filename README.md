# Permit Trends

Analyzes historical permits to find recently trending values.

Open directory with Docker installed and run `bash build`

In future builds, if no changes have been made, run `docker-compose --log-level ERROR up`


## kwargs (bold are required):

### ALL:
- **csv_path (str)**
- **location (str)**
  - Location tag to be used in visualization title
- **min_dt (str)**
  - Minimum date cutoff
- **date_col (str)**
- filter_cols (dict)
  - Columns to filter by
  - e.x. {"Permit Class Mapped": ["Residential"], "Permit Type Desc": ["Building Permit"]}
  - For numerical data (e.g. sqft), {"Total Lot SQFT": [1000,]} would return all permits with sqft greater than 1000 sqft
  - {"Total Lot SQFT": [1000,15000]} returns between 1000 and 15000
- remove_values (dict)
  - Values to remove in a specific column
  - Same syntax as filter_cols. {"Owner": ["NULL"]} removes all "NULL" values in the "Owner" column.
- num_results
- max_dt (str)
  - Maximum date cutoff
- display_kwargs
  - boolean, decides whether to include kwargs textbox in plot

### GeoTrends:
- **lat_col (str)**
- **lon_col (str)**
- top_areas_cutoff (int)
- max_miles (float)

### PermitTrends:
- **owners_col (str)**
- top_owners_cutoff (int)
- resample_size (str)
  - Bins for charting values, e.x. "1M"
![alt text](https://github.com/parkervg/PermitTrends/blob/master/data/resample_params.png)


# Example Outputs:

### GeoTrends:
## Austin
![alt text](https://github.com/parkervg/PermitTrends/blob/master/outputs/AustinGeoTrends.png)
## LA
![alt text](https://github.com/parkervg/PermitTrends/blob/master/outputs/LAGeoTrends.png)
## Chicago
![alt text](https://github.com/parkervg/PermitTrends/blob/master/outputs/ChicagoGeoTrends.png)

### PermitTrends
## Austin
![alt text](https://github.com/parkervg/PermitTrends/blob/master/outputs/AustinPermitTrends.png)
## LA
![alt text](https://github.com/parkervg/PermitTrends/blob/master/outputs/LAPermitTrends.png)
## Chicago
![alt text](https://github.com/parkervg/PermitTrends/blob/master/outputs/ChicagoPermitTrends.png)
