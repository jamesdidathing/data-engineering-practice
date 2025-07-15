import requests
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():
    mkch_directory()
    with ThreadPoolExecutor() as executor:
        executor.map(get_data, download_uris)

def mkch_directory():
    if not os.path.exists("downloads"):
        os.makedirs("downloads")
    os.chdir("downloads")

def get_data(uri):
        response = requests.get(uri)
        if response.status_code == 200:
            print("Success reading. Downloading file...")
            response = requests.get(uri)
            filename = uri.split("/")[-1]
            with open(filename, mode="wb") as file:
                file.write(response.content)
            print("Download complete:", filename)
            print("Extracting data...", filename)
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall()
                os.remove(filename)
        else:
            print(f"Error with reading. Error code {response.status_code}")

    

if __name__ == "__main__":
    main()
