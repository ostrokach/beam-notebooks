#!/bin/bash

PYTHON="/usr/local/google/home/strokach/miniconda3/bin/python3.7"
NBSTRIPOUT="/usr/local/google/home/strokach/miniconda3/bin/nbstripout"

${PYTHON} ${NBSTRIPOUT} | \
    sed 's|^   "display_name": "Python \[conda env:.*|   "display_name": "Python 3",|; s|^   "name": "conda-env-.*|   "name": "python3"|'
