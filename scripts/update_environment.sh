#!/bin/bash

set -e

conda remove -y -n beam-notebook --all || true

conda create -y -n beam-notebook \
    'openjdk=8' maven 'gradle>=5.0,<6.0' nodejs \
    'python=3.7' pip \
    ipython flask 'six>=1.12' \
    cython numpy pandas 'pyarrow>=0.15,<0.16' scikit-learn fastavro \
    matplotlib pydot bokeh tqdm ipywidgets
