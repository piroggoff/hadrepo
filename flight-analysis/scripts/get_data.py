import glob
import kagglehub
import os
import shutil

dataset = "dilwong/flightprices"

def remove_directory(path: str):
    if os.path.exists(path) and os.path.isdir(path):
        shutil.rmtree(path)


def download_dataset(path: str) -> str:
    return kagglehub.dataset_download(path)


#Required file destination


data_path = os.path.dirname(__file__) + '/../data/raw'
os.makedirs(data_path, exist_ok=True)

# Download latest version
path = download_dataset(dataset)
print(path)

#не удалось скачать датасет и повторная поппытка
if not any(os.listdir(path)):
    remove_directory(path)
    path = download_dataset(dataset)


# Required paths of files
files= glob.glob(os.path.join(path, '*.csv'))

# moving files in /raw
for file_path in files:
    shutil.move(file_path, data_path)