import kagglehub
import subprocess
import sys
import os
import shutil

#Required file destination
data_path = os.path.dirname(__file__) + '/raw'
print(data_path)
# Download latest version
path = kagglehub.dataset_download(handle="shubhambathwal/flight-price-prediction/versions/2")

shutil.move(path, data_path)
print("Files stored at {path}")