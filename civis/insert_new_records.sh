pip install python-dotenv
pip install git+https://github.com/common-cause/ccef_connections.git

DELAY="--delay 0.3"

python app/scripts/sync.py insert_new_records --campaign michigan $DELAY
python app/scripts/sync.py insert_new_records --campaign nebraska $DELAY
python app/scripts/sync.py insert_new_records --campaign california $DELAY
python app/scripts/sync.py insert_new_records --campaign new_york $DELAY
python app/scripts/sync.py insert_new_records --campaign texas $DELAY
python app/scripts/sync.py insert_new_records --campaign pennsylvania $DELAY
python app/scripts/sync.py insert_new_records --campaign florida $DELAY
python app/scripts/sync.py insert_new_records --campaign north_carolina $DELAY
python app/scripts/sync.py insert_new_records --campaign colorado $DELAY
python app/scripts/sync.py insert_new_records --campaign ohio $DELAY
python app/scripts/sync.py insert_new_records --campaign massachusetts $DELAY
python app/scripts/sync.py insert_new_records --campaign oregon $DELAY
python app/scripts/sync.py insert_new_records --campaign illinois $DELAY
python app/scripts/sync.py insert_new_records --campaign minnesota $DELAY
python app/scripts/sync.py insert_new_records --campaign arizona $DELAY
python app/scripts/sync.py insert_new_records --campaign new_mexico $DELAY
python app/scripts/sync.py insert_new_records --campaign wisconsin $DELAY
python app/scripts/sync.py insert_new_records --campaign maryland $DELAY
python app/scripts/sync.py insert_new_records --campaign indiana $DELAY
python app/scripts/sync.py insert_new_records --campaign georgia $DELAY
python app/scripts/sync.py insert_new_records --campaign rhode_island $DELAY
python app/scripts/sync.py insert_new_records --campaign hawaii $DELAY
