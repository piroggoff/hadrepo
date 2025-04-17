## Установка
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
    pip install -r requirements.txt
    pip install -e .
   
5. Write prepared data to HBase:
   ````bash
   python scripts/data_clean_to_hbase.py
   

All scripts must be start from hadrepo/flight_analysis