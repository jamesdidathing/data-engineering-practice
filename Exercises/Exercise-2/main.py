import requests
import pandas as pd

BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"

def main():
    response, csv_list = get_data(BASE_URL)
    filename = find_download_csv(csv_list, "2024-01-19")
    if filename:
        download_csv(BASE_URL + filename, filename)
    process_data(filename)

def get_data(uri):
    response = requests.get(uri)
    csv_list = response.text.split("</tr>")
    return response, csv_list

def find_download_csv(csv_list, date):
    for line in csv_list:
        if date in line and ".csv" in line:
            filename = line.split('href="')[1].split('"')[0]
            print(f"Found file: {filename}")
            return filename
    print("No matching file found.")
    return None

def download_csv(file_url, filename):
    print(f"Downloading from {file_url}")
    response = requests.get(file_url)
    if response.status_code == 200:
        with open(filename, mode="wb") as file:
            file.write(response.content)
        print(f"Downloaded and saved as {filename}")
    else:
        print(f"Failed to download {file_url} (Status code: {response.status_code})")

def process_data(filename):
    df = pd.read_csv(filename)
    df['HourlyDryBulbTemperature'] = pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce')
    print(df['HourlyDryBulbTemperature'].max())

if __name__ == "__main__":
    main()