#!/bin/bash

export PYTHONPATH=$(pwd):../intersystems-irispython:.
(cd ../django && python tests/runtests.py --settings=iris_settings --noinput "$@")

