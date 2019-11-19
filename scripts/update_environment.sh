#!/bin/bash

set -e

conda remove -y -n beam-notebook --all || true

conda create -y -n beam-notebook \
    'python=3.7' 'openjdk=8' maven nodejs ipython \
    apache-beam cython numpy pandas pyarrow scikit-learn fastavro \
    matplotlib bokeh flask \
    tqdm ipywidgets
source activate beam-notebook
pip install --no-deps 'six>=1.12'
pip install "git+https://github.com/apache/beam.git@master#egg=apache_beam[gcp]&subdirectory=sdks/python"
conda deactivate
