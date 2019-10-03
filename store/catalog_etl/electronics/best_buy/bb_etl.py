import os
import sys
import json
import time
import logging
from datetime import datetime

import numpy as np
import pandas as pd
import pyodbc

# globals
date_string = datetime.now().strftime('%m_%d_%Y')
data_directory = 'data/%s' % date_string

# create logger
logger = configure_logging(path_to_log_directory='logs/')

if not os.path.isdir(data_directory):
    logger.error('type="directory" | message="Invalid Directory"' % data_directory)
    raise Exception('Error: Invalid File Directory')

try:
    DRIVER = os.environ['PDB_DRIVER']
    HOST = os.environ['PDB_HOST']
    DATABASE = os.environ['PDB_DATABASE']
    TABLE_NAME = os.environ['PDB_PRODUCT_TABLE']
    USERNAME = os.environ['PDB_USERNAME']
    PASSWORD = os.environ['PDB_PASSWORD']
except KeyError as e:
    logger.error('type="ENV_VARIABLE" | message="Failed to get %s"' % e)
    raise Exception(e)

connection_string = 'DRIVER={%s};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s' % (DRIVER, HOST, DATABASE, USERNAME, PASSWORD)

# establish connection
conn = pyodbc.connect(connection_string)
    

def configure_logging(path_to_log_directory, log_level='WARNING'):
    """
    Creates a file logger using a date prefix name.
    
    Args:
        path_to_log_directory (str): defines the directory to store logs.
        log_level (str): defines type of messages to log.
    """
    log_filename = datetime.now().strftime('%Y-%m-%d') + '.log'
    logger = logging.getLogger('bb_etl_logger')
    logger.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')

    fh = logging.FileHandler(filename=os.path.join(path_to_log_directory, log_filename))
    fh.setLevel(log_level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(log_level)
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    
    return logger

# main functions
def extract(data_directory, subset=None):
    """
    Main function to extract all relevant content from files, databases, etc.
    
    Returns: List of Products
    """
    products = get_template(data_directory)
    if subset:
        products = products[subset].copy()
    
    for file in os.listdir(data_directory):
        if '.json' in file:
            brand, results = read_json(os.path.join(data_directory, file))
            results_df = pd.DataFrame.from_dict(results)
            results_df['brand'] = brand
            
            if subset:
                results_df = results_df[subset]

            products = pd.concat([products, results_df]).reset_index(drop=True)
        
    return products

def read_json(file_path):
    """
    Retrieves data from JSON file.
    
    Returns: list of json objects
    """
    
    try:
        print('Extracting Data From: %s' % file_path)
        with open(file_path, 'r') as readfile:
            results = json.load(readfile)
            return results.get('brand', None), results.get('results', [])
    except Exception as e:
        logger.error('type="parse" | file="%s"' % file_path)
        return []

def transform(data, dtypes):
    """
    Main function to apply necessary transformations.
    Args:
        data (pd.DataFrame): defines the data that is being transformed. 
        brand_context (pd.DataFrame): defines the appropriate brand mappings for the data.
        
    Returns: pd.DataFrame()
    """
    
    print('Transforming Data.')
    
    # drop duplicates
    data = data.drop_duplicates(subset=['brand', 'sku']).reset_index(drop=True)
    
    # fill missing data
    data = data.dropna(subset=['brand', 'sku', 'salePrice', 'highResImage'], how='any').reset_index(drop=True)
    data['salePrice'] = data['salePrice'].fillna(99999)
    data['shortDescription'] = data['shortDescription'].fillna('')
    data['categoryName'] = data['categoryName'].fillna('Other')
    data['customerRating'] = data['customerRating'].fillna(0)
    data['customerRatingCount'] = data['customerRatingCount'].fillna(0)
    data['customerReviewCount'] = data['customerReviewCount'].fillna(0)

    # Rename Columns
    rename_dict = {
                   'highResImage': 'imageUrl', 
                   'shortDescription': 'description'
                  }
    data = data.rename(columns=rename_dict)
    
    # set dtypes
    for column, dtype in dtypes.items():
        data[column] = data[column].astype(dtype)
        
    return data

def validate(data, dtypes):
    """
    Main function to check to see if data meets standards/requirements for upload (ex. appropriate dtypes).
    
    Args:
        data (pd.DataFrame): defines the data that is being validated.   
        
    Returns: None
    """
    
    print('Validating Data.')
    
    # ensure datatypes match up to what was defined
    for column, dtype in zip(data.columns, data.dtypes.values):
        logger.error('type="validation" | message="%s to %s"' % (column, str(dtype)))
        assert dtypes[column] == dtype
        
    print('Succesfully Validated Data.')
    

# helper functions
def get_template(data_directory):
    """
    Get DataFrame Template
    
    Args:
        data_directory: directory which stores all JSON outputs.
    
    Returns: Empty pd.DataFrame
    """
    file_path = os.path.join(data_directory, os.listdir(data_directory)[0])
    _, results = read_json(file_path)
    
    product_template = pd.DataFrame.from_dict(results)
    product_template['brand'] = None
    product_template = product_template.drop(product_template.index)
    
    return product_template

def load(data, database, table, column_map):
    """
    Main function to prepare database table and upload the data.
    
    Args:
        data (pd.DataFrame): defines the data that is being validated.
        database (str): defines database we are uploading to.
        table (str): defines the table we are uploading to.
        column_map [(df.col, db.Col)]: Used to map pd.DataFrame column to respective column in the table.
        
    Returns: None
    """
    
    print('Importing data into [%s].dbo.[%s]' % (database, table))
    
    # cursor object
    cursor = conn.cursor()
    
    # query parameters
    data_columns = [column[0] for column in column_map]
    table_columns = ['[%s]' % column[1] for column in column_map]
    column_placeholders = ['?' for _ in table_columns]
    
    # sql
    create_table_sql = """
    CREATE TABLE [%s].dbo.[%s] (
        Brand varchar(255),
        PartNumber varchar(255),
        Category varchar(255),
        ImageUrl varchar(255),
        RegularPrice float,
        SalePrice float,
        Description varchar(255),
        CustomerRating float,
        CustomerRatingCount int,
        CustomerReviewCount int,
        SourceUrl varchar(255),
        CreatedOn datetime
    );
    """ % (database, table)
    
    truncate_sql = """
    TRUNCATE TABLE [%s].dbo.[%s];
    """ % (database, table)
    
    
    insert_sql = """
    INSERT INTO [%s].dbo.[%s] (%s)
    VALUES (%s)
    """ % (database, table,
          ', '.join(table_columns),
          ','.join(column_placeholders),
          )

    # truncate
    try:
        print(truncate_sql)
        cursor.execute(truncate_sql)
    except pyodbc.ProgrammingError as e:
        logger.error('type="sql" | message="Unable to find table %s.dbo.%s"' % (database, table))
        raise Exception('Unable to find table %s.dbo.%s' % (database, table))
    
    # insert
    try:
        insert_params = [tuple([row[column] for column in data_columns]) for _, row in data.to_dict(orient='index').items()]
        print(insert_sql)
        cursor.executemany(insert_sql, insert_params)
    except pyodbc.ProgrammingError as e:
        logger.error('type="sql" | message="Unable to import data into %s.dbo.%s"' % (database, table))
        raise Exception('Unable to import data into %s.dbo.%s' % (database, table))
    
    # commit
    print('Committing Changes.')
    conn.commit()

    # close connection
    conn.close()

    
if __name__ == '__main__':
    start_time = time.time()
    
    # get data
    column_subset = ['brand', 'sku', 'categoryName', 'highResImage', 
                    'regularPrice', 'salePrice', 'shortDescription', 
                     'customerRating', 'customerRatingCount', 
                     'customerReviewCount', 'productUrl',
                    ]
    products = extract(data_directory, subset=column_subset)

    # define dtypes
    dtypes = {
                  'brand': np.dtype('object'), 
                  'sku': np.dtype('object'), 
                  'categoryName': np.dtype('object'), 
                  'imageUrl': np.dtype('object'), 
                  'regularPrice': np.dtype('float64'),
                  'salePrice': np.dtype('float64'), 
                  'description': np.dtype('object'), 
                  'customerRating': np.dtype('float64'),
                  'customerRatingCount': np.dtype('int32'), 
                  'customerReviewCount': np.dtype('int32'), 
                  'productUrl': np.dtype('object')
             }

    # transform
    products = transform(products, dtypes)

    # validate
    validate(products, dtypes)
    
    # load data
    column_map = [('brand', 'Brand'), 
                  ('sku', 'PartNumber'), 
                  ('categoryName', 'Category'), 
                  ('imageUrl', 'ImageUrl'), 
                  ('regularPrice', 'RegularPrice'), 
                  ('salePrice', 'SalePrice'), 
                  ('description', 'Description'), 
                  ('customerRating', 'CustomerRating'), 
                  ('customerRatingCount', 'CustomerRatingCount'), 
                  ('customerReviewCount', 'CustomerReviewCount'), 
                  ('productUrl', 'SourceUrl'),
                 ]
        
    load(data=data, database=DATABASE, table=TABLE_NAME, column_map=column_map)
    
    end_time = time.time()
    
    print('Time Elapsed: %s seconds.' % end_time - start_time)

