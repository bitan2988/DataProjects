# Baker-Hughes data ETL

This tool helps in preparing Baker-Hughes land_and_offShore data from Excel files, 
handling missing column headers, reshaping data, and uploading it to a database.


Python is used as the ETL tool along with Postgres server hosted on localhost as the DB.

    1. Read Individual Sheet of the Excel
    The excel contains more than 1 sheet. First we read the metadata of the excel and then individually read the sheets.
    The first 5 rows contains the company logo and can be neglected.

    2. Rename the Missing Column Headers
    On the excel the column headers are merged into one, this causes issue while reading using pandas
    as the headers go missing for the later cells.
    The stratergy used is the replace the unnamed columns with the previous valid column name present.

    3. Reshape the data
    The data still contains a lot of columns which represent a state/provience and whether its land or offshore.
    We try to reshape the data by unpivoting it to reduce the number of columns and bring the sheets at a similiar level
    and the number of columns would differ otherwise in case of different number of states or provinces.

    4. Prepare the Database
    We create a DB "baker_hughes" to hold all our data.
    Inside we also create a schema "web_scrapes".
    The number of tables we create are directly proportional to the number os sheets in the excel.
    In our case each table holds for a specific country.

    5. Upload the DataFrame to the Table
    We now upload the reshaped dataframe to their respective tables.


### Prerequisites

- Python >= 3.10
- Required Python packages installed (`pandas`, `openpyxl`, `sqlalchemy`, 'psycopg2`)
- Postgres-server




## Setup Instructions
    - Change the credentials on the config.json file to represent your DB creds.
    - All the excel files should be present in the /data directory.
    

# INSTALLATION
    - set the working directory should be at main.py level.
    - python3 main.py











