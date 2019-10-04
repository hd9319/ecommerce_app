import os
from ftplib import FTP

def download_inventory_file():
	"""
	Downloads Inventory File from FTP.
	"""
    logger = configure_logging(path_to_log_directory='logs/')
    
    try:
        FTP_HOST = os.environ['FTP_HOST']
        FTP_PORT = os.environ['FTP_PORT']
        FTP_USER = os.environ['FTP_USER']
        FTP_PASSWORD = os.environ['FTP_PASSWORD']
        FTP_INVENTORY_FILE = os.environ['FTP_INVENTORY_FILE']
        AA_INVENTORY_PATH = os.environ['AA_INVENTORY_PATH']
    except KeyError as e:
        logger.error('ERROR: RETRIEVING ENVIRONMENT VARIABLE: %s' % e)
        raise Exception('ERROR: RETRIEVING ENVIRONMENT VARIABLE: %s' % e)
    
    ftp = FTP(source_address=(FTP_HOST, FTP_PORT), user=FTP_USER, passwd=FTP_PASSWORD)
    
    with open(AA_INVENTORY_PATH, 'wb') as outfile:
        ftp.retrbinary('RETR ' + FTP_INVENTORY_FILE, outfile.write)

def configure_logging(path_to_log_directory, log_level='WARNING'):
    """
    Creates a file logger using a date prefix name.
    
    Args:
        path_to_log_directory (str): defines the directory to store logs.
        log_level (str): defines type of messages to log.
    """
    log_filename = datetime.now().strftime('%Y-%m-%d') + '.log'
    logger = logging.getLogger('ftp_logger')
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
    
if __name__ == '__main__':
    #download_inventory_file()
    pass