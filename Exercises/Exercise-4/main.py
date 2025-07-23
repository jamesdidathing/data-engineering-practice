import boto3
import glob
import json
import pandas as pd

def main():
    json_files = glob.glob("data/**/*.json", recursive=True)
    for file in json_files:
        df = pd.read_json(file)
        csv = df.to_csv(f"{file}.csv")

    pass


if __name__ == "__main__":
    main()
