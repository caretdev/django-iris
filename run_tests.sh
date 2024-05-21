#!/bin/bash

# python -m venv .venv
# source .venv/bin/activate
# pip install ../django -r requirements.txt -r requirements-dev.txt -e . 

export PYTHONPATH=.:$(pwd)
# export PYTHONPATH=.:$(pwd):../intersystems-irispython
(cd ../django && python tests/runtests.py --settings=iris_settings --noinput "$@")

