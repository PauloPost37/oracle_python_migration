#!/bin/bash
echo 'Installing required dependencies and setting up workspace'

python -m venv migration_env

source migration_env/bin/activate

pip install -r requirements.txt

