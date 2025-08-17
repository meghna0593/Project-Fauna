# Project-Fauna
An ETL (Extract → Transform → Load) pipeline for the Animals API, built with Python 3.10+ 


## (Recommended) Steps to run the pipeline using make commands:
1. Create virtual environment and install dependencies
`make setup`
2. Execute pipeline with optional arguments (use `make help` to understand how the commands work)
`make run`
3. Run tests
`make test`
4. Remove venv, cache and build artifacts
`make clean`

## Steps to run the pipeline using virtual environment
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

6. Exit virtual environment
deactivate


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