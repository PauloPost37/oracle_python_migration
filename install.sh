#!/bin/bash
sudo apt install python3-venv

echo 'Installing dependencies'

python3 -m venv migration_env

# Debugged with Chat GPT 5.2
source migration_env/bin/activate

pip install -r requirements.txt

