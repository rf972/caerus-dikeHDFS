#!/bin/bash

set -e # exit on error

export PYNDP_PATH="/pydike"
pushd $PYNDP_PATH
python3 setup.py sdist
echo "Finished building pydike"
popd

rm pydike_venv.tar.gz || true
python3 -m venv pydike_venv
source pydike_venv/bin/activate
echo "Activated"
pip install wheel
pip install pyarrow pandas venv-pack fastparquet py4j duckdb pyspark sqlparse
echo "first install done"
pip install "$PYNDP_PATH/dist/pydike-0.1.tar.gz"
echo "install of pydike done"
venv-pack -o pydike_venv.tar.gz
deactivate
rm -rf pydike_venv
