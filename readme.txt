
Setup steps (based on MacOS)  


Step 1:  checkout the git repo from github to your local machine using the following command in a terminal window:

    git clone git@github.com:pgfox/visitors_luzern_etl.git

step 2: create a python virtual enviroment to run the code. 

    cd visitors_luzern_etl
    python3 -m venv $PWD/venv

step 3: activate the virtual environment and install all the needed python packages in the virtual env:

    source venv/bin/activate
    pip install -r requirements.txt

Step 4: setup the Database .
ASSUMPTION: runing postgresql database server locally. 

    4.1 create a database in pgadmin called 'visitor_luzern' (using pgadmin client)

    4.2 open the sql_scripts/create_tables.sql in pgadmin client query tool and run the script. 

step 5: invoke the python main line file from the terminal (with virtual env activated):

    python3 ImportData.py

step 6:  using the pgadmin client, inspect the table 'visitor_schema.f_visitor_frequency' to ensure data is placed in table.

