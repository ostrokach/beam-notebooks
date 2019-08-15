#!/bin/bash

PYTHON="$(which python)"
NBSTRIPOUT="$(which nbstripout)"

${PYTHON} ${NBSTRIPOUT} "$@" -t | \
    sed 's|^   "display_name": "Python \[conda env:.*|   "display_name": "Python 3",|; s|^   "name": "conda-env-.*|   "name": "python3"|'
