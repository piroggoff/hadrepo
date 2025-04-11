import kagglehub
import subprocess

# Download latest version
path = kagglehub.dataset_download("shubhambathwal/flight-price-prediction")

err = subprocess.run(f'mv {path}/*.csv $(pwd)', shell=True, stderr=subprocess.PIPE, text=True)

if err:
    with open('error.log', 'w') as f:
        f.write(err)
        print('ERROR with move the files\nCheck error.log')



