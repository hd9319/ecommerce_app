import os
import sys

import json
import logging
import math
import re
import time

from copy import deepcopy
from datetime import datetime
from queue import Queue
from threading import Thread

import requests
from bs4 import BeautifulSoup

def configure_logging(path_to_log_directory, log_level='WARNING'):
    """
    Creates a file logger using a date prefix name.
    
    Args:
        path_to_log_directory (str): defines the directory to store logs.
        log_level (str): defines type of messages to log.
    """
    log_filename = datetime.now().strftime('%Y-%m-%d') + '.log'
    logger = logging.getLogger('scrape_logger')
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

def write_products_to_json(file_path, products):
    """
    Writes product data into JSON file.
    
    Args:
        file_path (str): defines the JSON path to store data contents.
        products (list): list of products we want to store.
    """
    with open(file_path, 'w+') as outfile:
        json.dump({'results': products}, outfile)
        
def get_headers(referer):
    """
    Creates a copy of the base_headers with an updated referer.
    
    Args:
        referer (str): used to update header field that defines where the requests are from.
    """
    headers = deepcopy(base_headers)
    headers['referer'] = referer
    return headers

def get_params(page_index, brand):
    """
    Creates a tuple of parameters needed to make an API request.
    
    Args:
        page_index (int): defines which page resource to request for.
        brand (int): defines which brand we are making the API request for.
    """
    
    base_params = (
        ('categoryid', ''),
        ('currentRegion', 'ON'),
        ('include', 'facets, redirects'),
        ('lang', 'en-CA'),
        ('page', '%s' % page_index),
        ('pageSize', '24'),
        ('path', 'brandName:%s' % brand),
        ('query', ''),
        ('exp', ''),
        ('sortBy', 'relevance'),
        ('sortDir', 'desc'),
    )
    return base_params

def get_brands_reference(request_delay=request_delay):
    """
    Gets all brands along with their respective links.
    
    Args:
        request_delay (int): defines the seconds in between requests.
    """
    # get all brands
    brands_reference = {}
    
    # make initial request
    response = requests.get(home_page, headers=base_headers)
    soup = BeautifulSoup(response.content)
    
    if response.status_code not in acceptable_status_codes:
        logger.error('Failed to Connect to %s' % home_page)

    for brandContainer in soup.find_all(class_=re.compile("brandGroup")):  # className contains: brandGroup
        for link in brandContainer.find_all('a'):
            try:
                brand, url = link.text, link.get('href')
                if brand != '' and url:
                    brands_reference[brand] = url
            except ValueError as e:
                logger.warning('Unable to Parse: %s' % link)
                
    return brands_reference

def get_products_via_api(brand='ACER', init_url='https://www.bestbuy.ca/en-ca/brand/acer', request_delay=request_delay):
    """
    Uses the API service to retrieve products and then writes the data to a directory in JSON format.
    
    Args:
        brand (str) : defines which brand we want to retrieve products for.
        init_url (str): url used to initialize session.
        request_delay (int): seconds in between each request.
    """
    
    print('Downloading Products for %s' % brand)

    session = requests.session()

    _ = response = session.get(home_page, headers=base_headers)
    time.sleep(request_delay)

    headers = get_headers(referer=home_page)
    response = session.get(init_url, headers=headers)

    if response.status_code in acceptable_status_codes:
        for page_index in range(1, 100):
            print(page_index)
            time.sleep(request_delay)

            # request parameters
            params = get_params(page_index=page_index, brand=brand)
            headers = get_headers(referer=init_url)

            response = session.get(api_page, headers=headers, params=params)

            if response.status_code in acceptable_status_codes:
                try:
                    # write products to file
                    products = response.json().get('products', [])
                    if len(products) > 0:
                        file_path = os.path.join(data_directory, '%s_%s.json' % (brand.replace(' ', ''), 
                                                                                 str(page_index)))
                        write_products_to_json(file_path, products)

                    # break if page_index has exceeded page count
                    total_pages = response.json().get('totalPages', 1)
                    if page_index >= total_pages:
                        print('Completed: %s' % brand)
                        break

                except json.decoder.JSONDecodeError as e:
                    logger.warning('Failed to get content from %s' % response.url)
                    break
            else:
                logger.warning('Failed to connect to %s' % response.url)
            
            print('Completed: %s - %s/%s' % (brand, page_index, total_pages)) 

    else:
        print(response.status_code)
        logger.warning('Failed to home page.')
        return response

def get_products_on_page(page):
    """
    Gets all products from the page.
    
    Args:
        page (): defines the seconds in between requests.
    """
    
    # get page count
    results_per_page = 24
    try:
        result_count = float(re.sub('[^\d]', '', 
                              soup.find('div', text=re.compile('results')).text))
        page_count = math.ceil(result_count / results_per_page)
    except ValueError as e:
        page_count = 1
    
    # get products on page
    product_results = page.find_all(class_=re.compile('productLine'))
    products = []

    # parse products and page count
    for product in product_results:
        # optional content
        try:
            review_content = product.find(class_=re.compile('review'))
            rating = review_content.find('meta', {'itemprop': 'ratingValue'})['content']
            review = review_content.find('span', {'itemprop': 'ratingCount'}).text
            promo = product.find(class_=re.compile('productSaving')).text
        except AttributeError as e:
            rating = None
            review = None
            promo = None

        url, price, name = (product.find('a', {'itemprop': 'url'})['href'],
                            product.find('meta', {'itemprop': 'price'})['content'],
                            product.find(itemprop='name').text,
                           )
        products.append([url, price, name, rating, review, promo])
    return products

def download_all_products(brand_subset=None, threads=1):
    brand_reference = get_brands_reference()
    
    if brand_subset:
        assert len(set(brand_subset) - set(brand_reference.keys())) == 0
        brand_reference = {brand.upper(): domain + url_extension for brand, url_extension in brand_reference.items() 
                           if brand in brand_subset}
    else:
        brand_reference = {brand.upper(): domain + url_extension for brand, url_extension in brand_reference.items()}
    
    # putting into queue
    for brand, brand_page in brand_reference.items():
        get_products_via_api(brand=brand, init_url=brand_page)        


# date prefix
date_string = datetime.now().strftime('%m_%d_%Y')

# define urls
domain = 'https://www.bestbuy.ca'
home_page = 'https://www.bestbuy.ca/en-ca'
api_page = 'https://www.bestbuy.ca/api/v2/json/search'

# data scrapes directory
data_directory = 'data/%s' % date_string

# seconds between requests
request_delay = 1

# allowable requests
acceptable_status_codes = [200]

# request headers
base_headers = {
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
    'accept': '*/*',
    'referer': 'https://www.google.ca/',
    'authority': 'www.bestbuy.ca',
}

# create folder if it does not exist
if not os.path.isdir(data_directory):
    os.mkdir(data_directory)
    
# create logger
logger = configure_logging(path_to_log_directory='logs/')

if __name__ == '__main__':
    response = download_all_products()






