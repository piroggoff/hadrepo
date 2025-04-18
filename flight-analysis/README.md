## USAGE
1. Clone the repo:
    ```bash
    git clone https://github.com/piroggoff/hadrepo.git
2. Setup virtual environment:
   ```bash
   cd hadrepo/flight_analysis
   python3 -m venv .venv
   source .venv/bin/activate  # Linux/Mac 
   .venv\Scripts\activate  # Windows

3. Install requirement packages:
    ```bash
    pip install -r requirements.txt
    pip install -e .

4. Download datset:
    ```bash
   cd scripts & python get_data & cd ..
   
5. Write prepared data to HBase:
   ````bash
   python -m scripts.data_clean_to_hbase

## FAQ
* All scripts must be start from hadrepo/flight_analysis
* In data/raw you can find demo dataset, that contain first 10000 rows of original csv
* **_andl_** - another data loader, with stream data processing, using for each partition method\
**You must**
   ````bash
   create "flights","cf1","cf2","cf3"
Automatically check and create table is not yet available