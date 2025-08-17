# Project-Fauna
An ETL (Extract → Transform → Load) pipeline for the Animals API, built with Python 3.10+ 

## Steps to run the pipeline
1. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

2. (Optional) Load env vars if you have a .env file
source .env

3. Install dependencies
pip3 install -r requirements.txt

4. Editable install so imports work
pip3 install -e .

4. Run the barebones
animals-etl

5. To run tests
pytest -q


## Steps to run the script
1. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

2. (Optional) Load env vars if you have a .env file
source .env

3. Install dependencies
pip3 install -r requirements.txt

4. Run the ETL script
python3 scripts/animals_etl.py