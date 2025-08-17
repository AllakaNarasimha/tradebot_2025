import csv
import os
from custom_logger import *

class Config:
    config_csv = None
    def __init__(self, config_path):
        self.config_csv = config_path

    def save_accestoken(access_token):
        print(access_token)
        # Define the CSV file path
        csv_file_path = "access_token.csv"
        # Write data to CSV file
        with open(csv_file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(access_token)

    def save_text_to_file(self, file_path, content):
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
        with open(file_path, 'w') as file:
            file.write(content)
        print(f'Text saved to {file_path}')

    def read_text_from_file(self, file_path):
        if not os.path.exists(file_path):
            print(f'File not found: {file_path}')
            return None
        try:
            with open(file_path, 'r') as file:
                content = file.read()
                return content
        except FileNotFoundError:
            print(f'File not found: {file_path}')
            return None

    def create_config():
        # Define the data
        data = [
            {"redirect_uri": "https://127.0.0.1/", "client_id": "PGU0PN7T1X-100", "secret_key": "2SWNXYCDPG",
            "grant_type": "authorization_code", "response_type": "code", "state": "sample"}
        ]

        # Define the CSV file path
        csv_file_path = "config.csv"

        # Write data to CSV file
        with open(csv_file_path, mode='w', newline='') as file:
            fieldnames = ["redirect_uri", "client_id", "secret_key", "grant_type", "response_type", "state"]
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            # Write header
            writer.writeheader()

            # Write data
            writer.writerows(data)

        print(f'Data has been written to {csv_file_path}')

    def get_config(self):
        # Read data from CSV file as a dictionary
        with open(self.config_csv, mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                config_dict = dict(row)
                print("Dictionary:", config_dict)
        return config_dict