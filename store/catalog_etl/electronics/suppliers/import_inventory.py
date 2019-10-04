import os
import sys
import logging

import pyodbc

from clean_inventory import clean_inventory_data
from suppliers_sql import *

def configure_logging(path_to_log_directory, log_level='WARNING'):
    """
    Creates a file logger using a date prefix name.
    
    Args:
        path_to_log_directory (str): defines the directory to store logs.
        log_level (str): defines type of messages to log.

    Returns: logging.Logger
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

def get_inventory_data(file_path):
	"""
	Reads inventory data from file directory and then runs a preprocessing function.

	Args:
		file_path (str): path to inventory file

	Returns: pd.DataFrame
	"""

	try:
		assert os.path.isfile(file_path)
	except AssertionError:
		logger.error('"type="directory" | message="File does not exist: %s"' % file_path)
		raise Exception('File does not exist: %s' % file_path)

	data = pd.read_csv(file_path)

	try:
		data = clean_inventory_data(data)
	except Exception as e:
		logger.error('"type="clean" | message="Unable to clean inventory data"')
		raise Exception('Unable to clean inventory data.')

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

def load(data, column_map):
	"""
	Main function to prepare database table and upload the data.
    
    Args:
        data (pd.DataFrame): defines the data that is being validated.
        database (str): defines database we are uploading to.
        table (str): defines the table we are uploading to.
        column_map [(df.col, db.Col)]: Used to map pd.DataFrame column to respective column in the table.
        
    Returns: None
	"""
	connection_string = 'DRIVER={%s};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s' % (DRIVER, HOST, DATABASE, USERNAME, PASSWORD)
	conn = pyodbc.connect(connection_string)
	cursor = conn.cursor()

	print('Importing data into [%s].dbo.[%s]' % (database, table))
    
    # query parameters
    data_columns = [column[0] for column in column_map]
    table_columns = ['[%s]' % column[1] for column in column_map]
    column_placeholders = ['?' for _ in table_columns]
    
    # truncate
    try:
        TRUNCATE_SQL = TRUNCATE_SQL % DATABASE
        print(TRUNCATE_SQL)
        cursor.execute(TRUNCATE_SQL)
    except pyodbc.ProgrammingError as e:
        logger.error('type="sql" | message="Unable to find table %s.dbo.%s"' % (DATABASE, TABLE_NAME))
        raise Exception('Unable to find table %s.dbo.%s' % (DATABASE, TABLE_NAME))
    
    # insert
    try:
        insert_params = [tuple([row[column] for column in data_columns]) for _, row in data.to_dict(orient='index').items()]
        INSERT_SQL = INSERT_SQL % (DATABASE, TABLE_NAME,
                                  ', '.join(table_columns),
                                  ','.join(column_placeholders),
                                  )
        print(INSERT_SQL)
        cursor.executemany(INSERT_SQL, insert_params)
    except pyodbc.ProgrammingError as e:
        logger.error('type="sql" | message="Unable to import data into %s.dbo.%s"' % (database, table))
        raise Exception('Unable to import data into %s.dbo.%s' % (database, table))

logger = configure_logging(path_to_log_directory='logs/')

if not os.path.isdir(data_directory):
    logger.error('type="directory" | message="Invalid Directory"' % data_directory)
    raise Exception('Error: Invalid File Directory')

try:
    DRIVER = os.environ['PDB_DRIVER']
    HOST = os.environ['PDB_HOST']
    DATABASE = os.environ['PDB_DATABASE']
    TABLE_NAME = os.environ['PDB_INVENTORY_TABLE']
    USERNAME = os.environ['PDB_USERNAME']
    PASSWORD = os.environ['PDB_PASSWORD']
    INVENTORY_FILE = os.environ['INVENTORY_FILE']
except KeyError as e:
    logger.error('type="ENV_VARIABLE" | message="Failed to get %s"' % e)
    raise Exception(e)

if __name__ == '__main__':
	data = get_inventory_data()
	dtypes = {
                  'brand': np.dtype('object'), 
                  'sku': np.dtype('object'), 
                  'inventory': np.dtype('int32'), 
                  'discontinued': np.dtype('int32'),
                  'source': np.dtype('object')
             }

	validate(data, dtypes)

	column_map = [('brand', 'Brand'), 
                  ('sku', 'PartNumber'), 
                  ('inventory', 'Inventory'), 
                  ('discontinued', 'Discontinued'),
                  ('source', 'Source'),
                 ]

	load(data, column_map)


