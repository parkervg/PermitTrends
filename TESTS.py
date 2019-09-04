import requests
import json
import time
import logging
import sys
from tools.bucket_storage import receive_from_client
from error_handling import StandardError
##########################################################
############### GeoTrends Testing ########################
##########################################################
"""
Austin Test
"""
try:
    data = requests.post("http://localhost:80/api/geotrends",
                        data={
                              "lat_col": "Latitude",
                              "lon_col": "Longitude",
                              "csv_path": "data/Austin_Issued_Construction_Permits.csv",
                              "location": "Austin",
                              "min_dt": "2012-01-01",
                              "max_dt": "2017-01-01",
                              "date_col": "Applied Date",
                              "filter_cols": json.dumps({"Permit Class Mapped": ["Residential"],
                                                          "Permit Type Desc": ["Building Permit"]}),
                              "output_path": "outputs/AustinGeoTrends.png",
                              "display_kwargs": json.dumps(True)})
    sys.stdout.write("Task request made.  Getting status updates... \n")
    finished = False
    if not data.ok:
        print(json.loads(data.content))
        finished = True
    while finished == False:
        result = requests.post("http://localhost:80/api/status",
                              data={"job_id":json.loads(data.content.decode("utf-8"))["SUCCESS"]}) # Getting task_id_
        result = json.loads(result.content)
        if result["STATUS"] == "SUCCESS":
            finished = True
            sys.stdout.write("Visualization saved.")
        elif result["STATUS"] == "FAILED":
            raise StandardError(500, "FAILED", "Failure to run")
            break
        else:
            time.sleep(2)
except:
    raise
try: receive_from_client(result["storage_id"], "outputs/AustinGeoTrends.png")
except: pass



"""
LA Test

Latitude/Longitudes are combined into one column. Dealt with in PreProcess().
"""
try:
    data = requests.post("http://localhost:80/api/geotrends",
                        data={
                              "lat_lon_col": "Latitude/Longitude",
                              "csv_path": "data/LA_Permits.csv",
                              "location": "Los Angeles",
                              "min_dt": "2012-01-01",
                              "max_dt": "2017-01-01",
                              "date_col": "Issue Date",
                              "output_path": "outputs/LAGeoTrends.png"})
    sys.stdout.write("Task request made.  Getting status updates... \n")
    finished = False
    if not data.ok:
        print(json.loads(data.content))
        finished = True
    while finished == False:
        result = requests.post("http://localhost:80/api/status",
                              data={"job_id":json.loads(data.content.decode("utf-8"))["SUCCESS"]}) # Getting task_id_
        result = json.loads(result.content)
        if result["STATUS"] == "SUCCESS":
            finished = True
            sys.stdout.write("Visualization saved.")
        elif result["STATUS"] == "FAILED":
            raise StandardError(500, "FAILED", "Failure to run")
            break
        else:
            time.sleep(2)
except:
    raise
try: receive_from_client(result["storage_id"], "outputs/LAGeoTrends.png")
except: pass

"""
Chicago Test
"""
try:
    data = requests.post("http://localhost:80/api/geotrends",
                        data={
                              "lat_col": "LATITUDE",
                              "lon_col": "LONGITUDE",
                              "csv_path": "data/Chicago_Building_Permits.csv",
                              "location": "Chicago",
                              "min_dt": "2012-01-01",
                              "max_dt": "2017-01-01",
                              "date_col": "APPLICATION_START_DATE",
                              "max_miles": 3})
    sys.stdout.write("Task request made.  Getting status updates...\n")
    finished = False
    if not data.ok:
        print(json.loads(data.content))
        finished = True
    while finished == False:
        result = requests.post("http://localhost:80/api/status",
                              data={"job_id":json.loads(data.content.decode("utf-8"))["SUCCESS"]}) # Getting task_id_
        result = json.loads(result.content)
        if result["STATUS"] == "SUCCESS":
            finished = True
            sys.stdout.write("Visualization saved.")
        elif result["STATUS"] == "FAILED":
            sys.stdout.write("Failed to run.")
        else:
            time.sleep(2)
except:
    raise
try: receive_from_client(result["storage_id"], "outputs/ChicagoGeoTrends.png")
except: pass



"""
Seattle Test
"""
try:
    data = requests.post("http://localhost:80/api/geotrends",
                        data={
                              "lat_col": "Latitude",
                              "lon_col": "Longitude",
                              "csv_path": "data/Seattle_Building_Permits.csv",
                              "location": "Seattle",
                              "min_dt": "2011-01-01",
                              "max_dt": "2016-01-01",
                              "date_col": "AppliedDate",
                              "date_index": False,
                              "filter_cols": json.dumps({}),
                              "max_miles": 1,
                              "top_areas_cutoff": 500})
    sys.stdout.write("Task request made.  Getting status updates...\n")
    finished = False
    if not data.ok:
        print(json.loads(data.content))
        finished = True
    while finished == False:
        result = requests.post("http://localhost:80/api/status",
                              data={"job_id":json.loads(data.content.decode("utf-8"))["SUCCESS"]}) # Getting task_id_
        result = json.loads(result.content)
        if result["STATUS"] == "SUCCESS":
            finished = True
            sys.stdout.write("Visualization saved.")
        elif result["STATUS"] == "FAILED":
            sys.stdout.write("Failed to run.")
        else:
            time.sleep(2)
except:
    raise
try: receive_from_client(result["storage_id"], "outputs/SeattleGeoTrends.png")
except: pass

##########################################################
############### PermitTrends Testing #####################
##########################################################

"""
Austin Test
"""
try:
    data = requests.post("http://localhost:80/api/permittrends",
                        data={
                              "csv_path": "data/Austin_Issued_Construction_Permits.csv",
                              "owners_col": "Applicant Organization",
                              "location": "Austin",
                              "min_dt": "2012-01-01",
                              "date_col": "Applied Date",
                              "filter_cols": json.dumps({"Permit Class Mapped": ["Residential"],
                                                          "Permit Type Desc": ["Building Permit"],
                                                          "Total Lot SQFT": [1000,],
                                                          "Day Issued": ["FRIDAY"]}),
                              "date_index": True,
                              "resample_size": "4M",
                              "display_kwargs": json.dumps(True)
                              })
    sys.stdout.write("Task request made.  Getting status updates...\n")
    finished = False
    if not data.ok:
        print(json.loads(data.content))
        finished = True
    while finished == False:
        result = requests.post("http://localhost:80/api/status",
                              data={"job_id":json.loads(data.content.decode("utf-8"))["SUCCESS"]}) # Getting task_id_
        result = json.loads(result.content)
        if result["STATUS"] == "SUCCESS":
            finished = True
            sys.stdout.write("Visualization saved.")
        elif result["STATUS"] == "FAILED":
            raise ValueError("The thing failed.")
            sys.stdout.write("Failed to run.")
        else:
            time.sleep(2)
except:
    raise
try: receive_from_client(result["storage_id"], "outputs/AustinPermitTrends_Friday.png")
except: pass

"""
LA Test

Latitude/Longitudes are combined into one column. Dealt with in PreProcess().
"""
try:
    data = requests.post("http://localhost:80/api/permittrends",
                        data={
                              "csv_path": "data/LA_Permits.csv",
                              "owners_col": "Applicant Last Name",
                              "location": "Los Angeles",
                              "min_dt": "2012-01-01",
                              "date_col": "Issue Date",
                              "filter_cols_include": json.dumps({"Permit Sub-Type": ["1 or 2 Family Dwelling"],
                                                         "Census Tract": [2000, ]}),
                              "filter_cols_remove"
                              "resample_size": "1M",
                              "display_kwargs": json.dumps(True)
                              })
    sys.stdout.write("Task request made.  Getting status updates...\n")
    finished = False
    if not data.ok:
        print(json.loads(data.content))
        finished = True
    while finished == False:
        result = requests.post("http://localhost:80/api/status",
                              data={"job_id":json.loads(data.content.decode("utf-8"))["SUCCESS"]}) # Getting task_id_
        result = json.loads(result.content)
        if result["STATUS"] == "SUCCESS":
            finished = True
            sys.stdout.write("Visualization saved.")
        elif result["STATUS"] == "FAILED":
            raise ValueError("The thing failed.")
            sys.stdout.write("Failed to run.")
        else:
            time.sleep(2)
except:
    raise
try: receive_from_client(result["storage_id"], "outputs/LAPermitTrends_smallTracts_1M.png")
except: pass


"""
Seattle Test
"""
try:
    data = requests.post("http://localhost:80/api/permittrends",
                        data={
                              "csv_path": "data/Seattle_Building_Permits.csv",
                              "owners_col": "ContractorCompanyName",
                              "location": "Seattle",
                              "min_dt": "2008-01-01",
                              "date_col": "AppliedDate",
                              "date_index": True,
                              "resample_size": "4M",
                              "display_kwargs": json.dumps(True)})
    sys.stdout.write("Task request made.  Getting status updates...\n")
    finished = False
    if not data.ok:
        print(json.loads(data.content))
        finished = True
    while finished == False:
        result = requests.post("http://localhost:80/api/status",
                              data={"job_id":json.loads(data.content.decode("utf-8"))["SUCCESS"]}) # Getting task_id_
        result = json.loads(result.content)
        if result["STATUS"] == "SUCCESS":
            finished = True
            sys.stdout.write("Visualization saved.")
        elif result["STATUS"] == "FAILED":
            raise ValueError("The thing failed.")
            sys.stdout.write("Failed to run.")
        else:
            time.sleep(2)
except:
    raise
try: receive_from_client(result["storage_id"], "outputs/SeattlePermitTrends.png")
except: pass

"""
Toronto Test (multiple csvs in a folder)

These permit csvs had no owners, so street names were used.
"""
try:
    data = requests.post("http://localhost:80/api/permittrends",
                        data={
                              "csv_path": "data/toronto_permits/*.csv",
                              "date_col": "APPLICATION_DATE",
                              "owners_col": "STREET_NAME",
                              "location": "Toronto",
                              "min_dt": "2012-01-01",
                              "date_index": True,
                              "resample_size": "8M"})
    sys.stdout.write("Task request made.  Getting status updates...\n")
    finished = False
    if not data.ok:
        print(json.loads(data.content))
        finished = True
    while finished == False:
        result = requests.post("http://localhost:80/api/status",
                              data={"job_id":json.loads(data.content.decode("utf-8"))["SUCCESS"]}) # Getting task_id_
        result = json.loads(result.content)
        if result["STATUS"] == "SUCCESS":
            finished = True
            sys.stdout.write("Visualization saved.")
        elif result["STATUS"] == "FAILED":
            raise ValueError("The thing failed.")
            sys.stdout.write("Failed to run.")
        else:
            time.sleep(2)
except:
    raise
try: receive_from_client(result["storage_id"], "outputs/TorontoPermitTrends.png")
except: pass


"""
Chicago Test
"""
try:
    data = requests.post("http://localhost:80/api/permittrends",
                        data={
                              "csv_path": "data/Chicago_Building_Permits.csv",
                              "owners_col": "CONTACT_1_NAME",
                              "location": "Chicago",
                              "min_dt": "2008-01-01",
                              "date_col": "APPLICATION_START_DATE",
                              "date_index": True,
                              "filter_cols": json.dumps({"CONTACT_1_TYPE": ["OWNER"]}),
                              "resample_size": "3M",
                              "display_kwargs": json.dumps(True)})
    sys.stdout.write("Task request made.  Getting status updates...\n")
    finished = False
    if not data.ok:
        print(json.loads(data.content))
        finished = True
    while finished == False:
        result = requests.post("http://localhost:80/api/status",
                              data={"job_id":json.loads(data.content.decode("utf-8"))["SUCCESS"]}) # Getting task_id_
        result = json.loads(result.content)
        if result["STATUS"] == "SUCCESS":
            finished = True
            sys.stdout.write("Visualization saved.")
        elif result["STATUS"] == "FAILED":
            raise ValueError("The thing failed.")
            sys.stdout.write("Failed to run.")
        else:
            time.sleep(2)
except:
    raise
try: receive_from_client(result["storage_id"], "outputs/ChicagoPermitTrends.png")
except: pass

"""
San Jose Test
"""
try:
    data = requests.post("http://localhost:80/api/permittrends",
                        data={
                              "csv_path": "data/SanJose_Permits.csv",
                              "owners_col": "OWNERNAME",
                              "location": "San Jose",
                              "min_dt": "2008-01-01",
                              "date_col": "ISSUEDATE",
                              "remove_values": json.dumps({"OWNERNAME": ["NONE"]}),
                              "date_index": True})
    sys.stdout.write("Task request made.  Getting status updates...\n")
    finished = False
    if not data.ok:
        print(json.loads(data.content))
        finished = True
    while finished == False:
        result = requests.post("http://localhost:80/api/status",
                              data={"job_id":json.loads(data.content.decode("utf-8"))["SUCCESS"]}) # Getting task_id_
        result = json.loads(result.content)
        if result["STATUS"] == "SUCCESS":
            finished = True
            sys.stdout.write("Visualization saved.")
        elif result["STATUS"] == "FAILED":
            raise ValueError("The thing failed.")
            sys.stdout.write("Failed to run.")
        else:
            time.sleep(2)
except:
    raise
try: receive_from_client(result["storage_id"], "outputs/SanJosePermitTrends.png")
except: pass
