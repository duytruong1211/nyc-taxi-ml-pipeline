from dagster import op
import requests
import os

# @op
# def download_tlc_data():
#     url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
#     os.makedirs("raw", exist_ok=True)
#     target_path = "raw/yellow_tripdata_2024-01.parquet"

#     if not os.path.exists(target_path):
#         r = requests.get(url)
#         with open(target_path, "wb") as f:
#             f.write(r.content)

#     return os.path.abspath(target_path)
