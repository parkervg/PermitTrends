import os
import uuid
from google.cloud import storage
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Enter Credentials Here"
temp_client = storage.Client()
temp_bucket = temp_client.get_bucket("Enter Bucket Here")


def receive_from_client(id, output_path):
    blob = temp_bucket.get_blob("output/{}".format(id))
    return blob.download_to_filename(output_path), "Saved to {}".format(output_path)

def send_to_client(png_path):
    id = str(uuid.uuid4())
    blob = temp_bucket.blob("output/{}".format(id))
    blob.upload_from_filename(png_path)
    return id
