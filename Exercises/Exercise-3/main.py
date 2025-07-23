import boto3
import requests
import os
import gzip

URI = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-05/wet.paths.gz"

def main():
    file = get_data(URI)
    new_uri = fetch_new_uri(file)
    new_file = get_data(f"https://data.commoncrawl.org/{new_uri}")
    process_data(new_file)


def get_data(uri):
    response = requests.get(uri, stream=True)
    if response.status_code == 200:
        print("Success reading. Streaming data...")
        return response.raw  # Return the raw stream
    else:
        print(f"Error with reading. Error code {response.status_code}")
        return None

def fetch_new_uri(filename):
    print("Extracting data...", filename)
    with gzip.GzipFile(fileobj=filename, mode='rb') as zip_ref:
        file_content = zip_ref.read().decode('utf-8')
        return file_content.split('\n')[0]
    
def process_data(file_stream):
    print("Processing data from stream...")
    with gzip.GzipFile(fileobj=file_stream, mode='rb') as zip_ref:
        file_content = zip_ref.read().decode('utf-8')
        print(file_content.split()[0]) 

if __name__ == "__main__":
    main()

