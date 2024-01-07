import pandas as pd
import json
import psycopg2
import psycopg2.extras as extras


def clean_column_header(df):
    """
    This function renames and fills and missing column headers present in the excel-sheet.
    If a column-name is missing then it replaces it with the previous found valid column-name.

    Args:
        df (pandas.dataframe): Input dataframe

    Returns:
        list: list of new column names
    """
    name_fix = 'Date'
    
    new_column_names = list()
    
    for col_name in df.columns:
        if 'Unnamed:' not in col_name:
            name_fix = col_name
            
        if name_fix != "Date":
            new_column_names.append(f"{name_fix}_{df[col_name].iloc[0]}")
        else:
            new_column_names.append(name_fix)
    
    return new_column_names


def melt_and_reshape(df):
    """
    The table contains state/provience name as header currently.
    We melt(unpivot) it to reduce the number of columns, this also helps with finding aggregations and filtering.

    Args:
        df (pandas.dataframe): Input dataframe

    Returns:
        pandas.dataframe: melted dataframe
    """
    df = df.drop(0)
    unpivoted_df = pd.melt(df, id_vars=['Date'], var_name='State_land_type', value_name='Quantity')
    
    unpivoted_df["Region"] = unpivoted_df.apply(lambda x: '_'.join(x["State_land_type"].split('_')[:-1]), axis=1)
    unpivoted_df["Property_type"] = unpivoted_df.apply(lambda x: x["State_land_type"].split('_')[-1], axis=1)
    
    return unpivoted_df[["Date", "Region", "Property_type", "Quantity"]]


def create_db_objects():
    """
    This function  
        1. Creates a database named baker_hughes if not present.
        2. Creates a schema named web_scrapes if not present.
        
    The credentials are currently stored in a config file as a JSON.
    Psycopg2 is used to connect to the posgres DB.

    Returns:
        (psycopg2.connection, psycopg2.connection.cursor): A connection instance and a cursor-object.
    """
    config_file = "config.json"

    with open(config_file, 'r') as file:
        config = json.load(file)
        
    file.close()
    
    db_creds = config["posgres_creds"]
    
    connection = psycopg2.connect(
        user=db_creds["USER"],
        password=db_creds["PASSWORD"],
        host=db_creds["HOST"],
        port=db_creds["PORT"],
        database=db_creds["DB_NAME"]
    )
    cursor = connection.cursor()
    connection.autocommit = True
    
    new_schema = "web_scrapes"
    new_database = "baker_hughes"
    
    db_check_query = f"SELECT datname FROM pg_database WHERE datname = '{new_database}';"
    cursor.execute(db_check_query)
    if cursor.fetchone() is None:
        print(cursor.fetchone())
        cursor.execute(f"CREATE DATABASE {new_database};")
        cursor.commit()
        
    connection.close()
    
    connection = psycopg2.connect(
        user=db_creds["USER"],
        password=db_creds["PASSWORD"],
        host=db_creds["HOST"],
        port=db_creds["PORT"],
        database=new_database
    )
    cursor = connection.cursor()
    connection.autocommit = True
    
    schema_check_query = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{new_schema}';"
    cursor.execute(schema_check_query)
    if cursor.fetchone() is None:
        cursor.execute(f"CREATE SCHEMA {new_schema};")
    
    return connection, cursor


def execute_values(conn, df, table, schema): 
    """
    This function is used to upload data from a pandas-dataframe to the connected DB.

    Args:
        conn (psycopg2.connection): connection instance to connect to the DB
        df (pandas.dataframe): Dataframe to be uploaded
        table (str): Table-name to upload the data to.
        schema (str): Schema name
    """
    tuples = [tuple(x) for x in df.to_numpy()] 
  
    cols = ','.join(list(df.columns)) 
  
    # SQL query to execute 
    query = f"INSERT INTO {schema}.{table}({cols}) VALUES %s"
    cursor = conn.cursor() 
    try: 
        extras.execute_values(cursor, query, tuples) 
        conn.commit() 
    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error: %s" % error) 
        conn.rollback() 
        cursor.close() 
        return 1
    cursor.close() 
    conn.close()


def create_posgres_tables(table_name, schema_name, df):
    """
    This function creates a table based on the name of the excel-sheet.

    Args:
        table_name (str): Name of the table to create.
        schema_name (str): Schema name
        df (pandas.dataframe): Dataframe to be stored in the table
    """
    connection, cursor = create_db_objects()
    
    create_table_query = f'''
                            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}
                            (Date DATE, Country TEXT, Region_type TEXT, Region TEXT, Property_type TEXT, Quantity INTEGER)
                        '''
    cursor.execute(create_table_query)
    
    try:
        execute_values(connection, df, table_name, schema_name)
    except Exception as e:
        print(e)
                        


def etl_handler():
    """
    This function 
        1. Read individual sheet of the excel.
        2. Rename the missing column headers.
        3. Reshape the data.
        4. Prepare the DB to create schemas and tables.
        5. Upload the dataframe to the table.
    """
    file_path = "data/BH data.xlsx"

    excel_file = pd.ExcelFile(file_path)
    sheet_names = excel_file.sheet_names

    for sheet_name in sheet_names:
        df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=5)
        new_column_names = clean_column_header(df)
        df.columns = new_column_names
        df = melt_and_reshape(df)
        
        country = sheet_name.split(" L & OS Split by ")[0]
        region_type = '_'.join(sheet_name.split(" L & OS Split by ")[1:])
        
        df["Country"] = country
        df["Region_type"] = region_type
        table_name = f"{country}_{region_type}s_l_os"
        
        df = df[["Date", "Country", "Region_type", "Region", "Property_type", "Quantity"]]
        
        create_posgres_tables(table_name, "web_scrapes", df)