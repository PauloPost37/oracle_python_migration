This project was implemented during the making of my Bachelors thesis. The project is a proof of concept and has no guarantee that it will work without potential errors. 

The only supported OS is: Linux

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

The installation is conducted in only a few steps:

1. Either clone the git repo if you have git set up, or use: wget https://github.com/PauloPost37/oracle_python_migration/archive/refs/heads/main.zip

2. unzip main.zip

3. cd oracle_python_migration-main

4. chmod 755 install.sh

5. source install.sh

The installation is now completed and you are inside the virtual environment to conduct the migration. If you exit out of the environment you can simple execute install.sh again.

The migration process can be used in two ways:

1. By entering: python3 copy_schema.py into the terminal, a text prompt will pop up indicating the IP and Port the WEBUI is running on. Simply enter the IP and Port into the adress bar of your browser. Next enter the connection data. This data is later saved in the config file. Afterwards press: Load Schemas & Save Config. You can then choose the schemas you want to migrate. The last step is to press: "Start Migration".

2. Configure the config.py located in the config directory. There you can input the connection details to the Oracle and PostgreSQL database. Next type: python3 copy_schema.py into the terminal followed by the Schema name you want to migrate.
Example: python3 copy_schema.py MONDIAL

Both methods will leave you with several LOG files aswell as a migration_report.txt. 
Along with the LOG files you also have access to the output files which hold the executed SQL for the alter statements and the create statements. 
