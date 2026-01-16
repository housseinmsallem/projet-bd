#!/bin/bash
export YOUTUBE_API_KEY="AIzaSyA7i3qblwOmcBvjCIy1friRujNFMuZwKdo"
export PYTHONPATH=$PYTHONPATH:$(pwd)/src
export PYSPARK_PYTHON=$(pwd)/venv/bin/python

# Run the main script
./venv/bin/python src/main.py --api-key "$YOUTUBE_API_KEY"
