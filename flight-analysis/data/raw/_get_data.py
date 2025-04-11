import kagglehub
import subprocess

# Download latest version
path = kagglehub.dataset_download("shubhambathwal/flight-price-prediction")

subprocess.run(f'mv {path}/*.csv $(pwd)', shell=True)

