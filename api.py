import json
from main import *
from redis import Redis
from rq import Queue, use_connection, Worker
from rq.job import Job
from error_handling import StandardError
from flask import Flask, request, make_response, jsonify
import pandas as pd
import logging
app = Flask(__name__)
redis_conn = Redis(host='redis', port=6379, db=0)
workers = Worker.all(connection = redis_conn)
q = Queue("r_queue", connection = redis_conn, default_timeout=3600)

# Clear queue
#q.empty()

@app.errorhandler(StandardError)
def handle_standard_error(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

@app.route("/api/status", methods=["GET", "POST"])
def status():
    job_id = request.form.get('job_id')
    job = Job.fetch(job_id, redis_conn)
    if job.is_finished:
        storage_id = job.return_value
        result = json.dumps({"STATUS":"SUCCESS", "storage_id": storage_id})
    elif job.is_queued:
        result = json.dumps({"STATUS":"IN_QUEUE"})
    elif job.is_started:
        result = json.dumps({"STATUS":"WAITING"})
    elif job.is_failed:
        result = json.dumps({"STATUS":"FAILED"})
        return make_response(result)
    return make_response(result)


@app.route("/api/geotrends", methods=["GET", "POST"])
def geotrends():
    if request.method == "POST":
        """
        Required params
        """
        csv_path = request.form.get("csv_path")
        min_dt = request.form.get("min_dt")
        date_col = request.form.get("date_col")
        location = request.form.get("location")
        if None in [csv_path,min_dt,date_col,location]:
            print(StandardError(400, "Bad Request", "One of the required parameters is left blank: [\"csv_path\", \"lat_col\", \"lon_col\", \"min_dt\", \"date_col\", \"location\"]"))
            raise StandardError(400, "Bad Request", "One of the required parameters is left blank: [\"csv_path\", \"lat_col\", \"lon_col\", \"min_dt\", \"date_col\", \"location\"]")

        """
        Optional params
        """
        # Latitude and Longitude columns
        lat_col = request.form.get("lat_col", None)
        lon_col = request.form.get("lon_col", None)
        lat_lon_col = request.form.get("lat_lon_col", None)
        if all(x is None for x in [lat_col, lon_col, lat_lon_col]):
            print(StandardError(400, "Bad Request", "No latitude or longitude columns provided."))
            raise StandardError(400, "Bad Request", "No latitude or longitude columns provided.")

        # Filter columns
        try:
            filter_cols = json.loads(request.form.get("filter_cols", '{}'))
        except:
            raise StandardError(400, "Bad Request", "Something's wrong with your 'filter_cols' input.")
        if filter_cols:
            for val in filter_cols.values():
                try: assert isinstance(val, list)
                except AssertionError:
                    print(StandardError(400, "Bad Request", "Make sure the values of your 'filter_cols' input are lists."))
                    raise StandardError(400, "Bad Request", "Make sure the values of your 'filter_cols' input are lists.")
        try:
            remove_values = json.loads(request.form.get("remove_values", '{}'))
        except:
            print(StandardError(400, "Bad Request", "Something's wrong with your 'remove_values' input."))
            raise StandardError(400, "Bad Request", "Something's wrong with your 'remove_values' input.")
        if remove_values:
            for val in remove_values.values():
                try: assert isinstance(val, list)
                except AssertionError:
                    print(StandardError(400, "Bad Request", "Make sure the values of your 'remove_values' input are lists."))
                    raise StandardError(400, "Bad Request", "Make sure the values of your 'remove_values' input are lists.")

        max_dt = request.form.get("max_dt", None)
        date_index = False
        top_areas_cutoff = int(request.form.get("top_areas_cutoff", 300))
        if top_areas_cutoff < 1:
            print(StandardError(400, "Bad Request", "'top_areas_cutoff' must be greater than 0"))
            raise StandardError(400, "Bad Request", "'top_areas_cutoff' must be greater than 0")
        max_miles = float(request.form.get("max_miles", 0.5))
        num_results = int(request.form.get("num_results", 15))
        display_kwargs = json.loads(request.form.get("display_kwargs", "false"))

        kwargs = {"csv_path": csv_path,
                  "lat_col": lat_col,
                  "lon_col": lon_col,
                  "location": location,
                  "min_dt": min_dt,
                  "max_dt": max_dt,
                  "date_col": date_col,
                  "date_index": date_index,
                  "filter_cols": filter_cols,
                  "max_miles": max_miles,
                  "num_results": num_results,
                  "top_areas_cutoff": top_areas_cutoff,
                  "display_kwargs": display_kwargs,
                  "lat_lon_col": lat_lon_col,
                  "remove_values": remove_values}

        task = q.enqueue(TrendingGeos, kwargs)
        return make_response(json.dumps({"SUCCESS":task._id}))


@app.route("/api/permittrends", methods=["GET", "POST"])
def permittrends():
    if request.method == "POST":

        """
        Required params.
        """
        csv_path = request.form.get("csv_path")
        owners_col = request.form.get("owners_col")
        date_col = request.form.get("date_col")
        min_dt = request.form.get("min_dt")
        location = request.form.get("location")
        if None in [csv_path,owners_col,date_col,min_dt,location]:
            print(StandardError(400, "Bad Request", "One of the required parameters is left blank: [\"csv_path\", \"owners_col\", \"date_col\", \"min_dt\", \"location\"]"))
            raise StandardError(400, "Bad Request", "One of the required parameters is left blank: [\"csv_path\", \"owners_col\", \"date_col\", \"min_dt\", \"location\"]")


        """
        Optional params
        """
        max_dt = request.form.get("max_dt")
        try:
            filter_cols = json.loads(request.form.get("filter_cols", '{}'))
        except:
            print(StandardError(400, "Bad Request", "Something's wrong with your 'filter_cols' input."))
            raise StandardError(400, "Bad Request", "Something's wrong with your 'filter_cols' input.")
        if filter_cols:
            for val in filter_cols.values():
                try: assert isinstance(val, list)
                except AssertionError:
                    print(StandardError(400, "Bad Request", "Make sure the values of your 'filter_cols' input are lists."))
                    raise StandardError(400, "Bad Request", "Make sure the values of your 'filter_cols' input are lists.")

        try:
            remove_values = json.loads(request.form.get("remove_values", '{}'))
        except:
            print(StandardError(400, "Bad Request", "Something's wrong with your 'remove_values' input."))
            raise StandardError(400, "Bad Request", "Something's wrong with your 'remove_values' input.")
        if remove_values:
            for val in remove_values.values():
                try: assert isinstance(val, list)
                except AssertionError:
                    print(StandardError(400, "Bad Request", "Make sure the values of your 'remove_values' input are lists."))
                    raise StandardError(400, "Bad Request", "Make sure the values of your 'remove_values' input are lists.")



        date_index = True
        top_owners_cutoff = int(request.form.get("top_owners_cutoff", 100))
        if top_owners_cutoff < 1:
            print(StandardError(400, "Bad Request", "'top_areas_cutoff' must be greater than 0"))
            raise StandardError(400, "Bad Request", "'top_areas_cutoff' must be greater than 0")
        display_kwargs = json.loads(request.form.get("display_kwargs", "false"))
        resample_size = request.form.get("resample_size", "4M")
        # Checking to make sure sample_size param is valid.
        if resample_size:
            test_index = [["2017-01-01", "adam"], ["2018-01-01", "patrick"]]
            df = pd.DataFrame([["2017-01-01", "adam"], ["2018-01-01", "patrick"]], columns =["dates", "people"])
            df["dates"] = pd.to_datetime(df["dates"])
            df.index = df["dates"]
            try:
                df = df.resample(resample_size)
            except ValueError:
                print(StandardError(400, "Bad Request", "Invalid 'resample_size' param."))
                raise StandardError(400, "Bad Request", "Invalid 'resample_size' param.")

        kwargs = {"csv_path": csv_path,
                  "owners_col": owners_col,
                  "date_col": date_col,
                  "location": location,
                  "min_dt": min_dt,
                  "max_dt": max_dt,
                  "date_col": date_col,
                  "date_index": date_index,
                  "filter_cols": filter_cols,
                  "top_owners_cutoff": top_owners_cutoff,
                  "resample_size": resample_size,
                  "display_kwargs": display_kwargs,
                  "remove_values": remove_values}

        task = q.enqueue(TrendingPermits, kwargs)
        return make_response(json.dumps({"SUCCESS":task._id}))

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port="5000")
