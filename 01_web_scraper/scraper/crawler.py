"""
Purpose: Scrape Zillow property listings, parse data, and store in MongoDB.
Handles async requests, data parsing, and storage operations.
"""

import concurrent.futures
import hashlib
import json
import os

import ParseJSON
import bson.json_util
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

import time
import logging
from datetime import datetime
import traceback
from dotenv import load_dotenv
from logger_settings import batch_process_logger

load_dotenv()

logging.basicConfig(filename='zillowLog',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)

logger = logging.getLogger('zillowRunnerLog')


def run_scraper(input_url, starting_price=1000, nyc=False, ultra_premium=False, listing_links_ultra=False, test=False):
    """Initialize and run the Zillow scraper"""
    start = time.time()
    batch_process_logger.info(
        f"{datetime.now().strftime('%m/%d/%Y, %H:%M:%S')} - Starting Zillow Scraper  {input_url[23:35]} price: {starting_price}")

    zillow = Zillow()
    zillow.test = test
    zillow.NYC = 'new-york-ny' in input_url
    zillow.ultra_premium = ultra_premium
    zillow.listing_links_ultra_premium = listing_links_ultra
    zillow.starting_price = zillow.current_price = starting_price
    zillow.previous_price = 0
    zillow.parseInputUrl(input_url)
    zillow.updateUrlPrice(zillow.current_price, first_run=True)

    page_range = 3 if test else 9
    rounds = 0
    endIn5Pages = 0

    try:
        while zillow.current_price > starting_price - 1 or endIn5Pages < 4:
            if endIn5Pages > 4:
                break
            rounds += 1
            if rounds > 1:
                time.sleep(20)  # cooldown sleep every 10 pages
            if rounds > 1:
                zillow.updateUrlPrice(max(zillow.price_memory) + (1 if rounds % 4 == 0 else 0))
            pages_container = [zillow.updateUrlPage(page_num) for page_num in range(1, page_range)]
            listings_container = zillow.getListingLinksAsync(pages_container)
            if listings_container is None:
                logger.info('Get Listing Links RETURNED NONE')
                continue

            zillow.asCompletedMultiThreadSubmit(listing_links=listings_container, max_workers=48)
            print(len(zillow.listingDatabase), zillow.current_price)
            if rounds == 1:
                zillow.getSaveName()
            zillow.saveCSV()
            if zillow.current_price > 50000000:
                endIn5Pages += 1
                logger.info("EndIn5Pages Initiated")

        print('Done Scraping', len(zillow.listingDatabase), 'Listings!')
        return zillow.listingDatabase

    except Exception as e:
        print(e)
        print(traceback.format_exc())

    finally:
        try:
            logRun = f"Listing Count: {len(zillow.listingDatabase)}, SaveName: {zillow.save_name}, Date: {datetime.now()}, Avg Scrape Time Per Listing: {start / len(zillow.listingDatabase):.2f}, Url: {input_url}"
            logger.info(logRun)
            zillow.upload_to_Azure_blob()
        except:
            logger.info("ISSUE LOGGING RUN DETAILS")


class Zillow:
    def __init__(self):
        self.API_KEY = os.getenv('SCRAPER_API_KEY')
        self.setup_logging()
        self.init_clients()
        self.init_variables()

    def setup_logging(self):
        self.logger = logging.getLogger('Zillow_Class')
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler('Zillow_Class.log')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def init_clients(self):
        self.links_executor = concurrent.futures.ThreadPoolExecutor(15)
        self.executor = concurrent.futures.ThreadPoolExecutor(48)
        self.mongo_client = self.connect_to_mongodb()

    def init_variables(self):
        self.listing_database = []
        self.price_memory = []
        self.current_price = 0
        self.previous_price = 0
        self.current_url = ''
        self.start_url = ''
        self.end_url = ''
        self.page = 2
        self.NYC = False
        self.ultra_premium = False
        self.test = False
        self.listing_links_ultra_premium = False

    def async_scrape_url_as_completed(self, url):
        """Asynchronously scrape URL and process response."""
        NUM_RETRIES = 4
        for attempt in range(NUM_RETRIES):
            response = self.send_request(url)
            if not response:
                self.logger.info(f'No response received on attempt {attempt + 1}')
                continue

            if response.status_code == 429:
                self.logger.info(f'Rate limit hit on attempt {attempt + 1}, waiting 2 seconds')
                time.sleep(2)
                continue

            if response.status_code != 200:
                self.logger.info(f'Received status code {response.status_code} on attempt {attempt + 1}')
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            if self.is_mobile_search_page(soup):
                return response

            if not self.test:
                json_results = self.parse_data_to_json(soup)
                if json_results:
                    self.upload_to_mongodb(json_results)
                else:
                    self.logger.info("Failed to parse data to JSON")

            parsed_data = self.parse_all_data_sections(soup)
            if parsed_data:
                return response

        self.logger.info(f'Failed to get valid response after {NUM_RETRIES} attempts')
        return None

    def is_mobile_search_page(self, soup):
        """Check if the page is a mobile search page."""
        script = soup.find("script", {"data-zrr-shared-data-key": "mobileSearchPageStore"})
        if not script:
            return False
        try:
            data = json.loads(script.contents[0].strip("!<>-\\"))
            return 'cat1' in data.keys()
        except json.JSONDecodeError:
            return False

    def parse_data_to_json(self, soup):
        """Parse BeautifulSoup object to JSON format."""
        try:
            parsed_json = ParseJSON.updatedParseJSON(soup)
            if not parsed_json:
                return None

            json_container = {
                'overview': ParseJSON.generalData(parsed_json),
                'location': ParseJSON.locationData(parsed_json),
                'propertyFeatures': ParseJSON.propertyFeaturesData(parsed_json),
                'pricing': ParseJSON.pricingData(parsed_json),
                'listingAgent': ParseJSON.listingAgentData(parsed_json),
                'schools': ParseJSON.schoolData(parsed_json),
                'compNearbyHomes': ParseJSON.compNearbyHomes(parsed_json),
                'pictures': ParseJSON.pictureData(parsed_json),
                'census': ParseJSON.getCensusData(parsed_json['address'])
            }

            timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            scrape_date = datetime.now().strftime("%d/%m/%Y")
            scrape_time = datetime.now().strftime("%H:%M:%S")

            hash_string = (str(parsed_json['zpid']) + timestamp).encode('UTF-8')
            hash_md5 = hashlib.sha256(hash_string).hexdigest()[:24]

            return {
                'root': {
                    "hash_md5": hash_md5,
                    "extractTimestamp": timestamp,
                    "scrapeDate": scrape_date,
                    "scrapeTime": scrape_time,
                    "zpid": parsed_json['zpid'],
                    'property': json_container
                }
            }
        except Exception as e:
            self.logger.error(f"Error parsing data to JSON: {str(e)}")
            return None

    def parse_all_data_sections(self, soup):
        """Parse all data sections from BeautifulSoup object."""
        parsed_json = ParseJSON.updatedParseJSON(soup)
        if parsed_json is None:
            return False

        listing_data = {}
        listing_data.update(ParseJSON.generalData(parsed_json))
        listing_data.update(ParseJSON.addressData(parsed_json))
        listing_data.update(ParseJSON.propertyFeaturesData(parsed_json))
        listing_data.update(ParseJSON.listingAgentData(parsed_json))
        listing_data.update(ParseJSON.getCensusData(listing_data))

        if not listing_data:
            return False

        self.listing_database.append(listing_data)
        self.price_memory.append(int(listing_data['price']))
        self.current_price = max(self.current_price, int(listing_data['price']))
        return listing_data

    def save_csv(self, df=None, path='OUTPUT/raw_csv/'):
        if df is None:
            df = self.transform_raw_data()
        os.makedirs(path, exist_ok=True)
        csv_path = f"{path}{self.save_name}.csv"
        df.to_csv(csv_path)

    def create_excel(self, path='OUTPUT/cleaned_excel/'):
        df_og = self.transform_raw_data()
        params = ['price', 'streetAddress', 'city', 'zipcode', 'state', 'daysOnZillow', 'agentName', 'agentEmail',
                  'agentPhoneNumber', 'hdpUrl']
        df = df_og[params].sort_values(['agentName', 'price'], ascending=[False, False])
        df = df[~df.agentName.str.contains('Team')]
        df = df.drop_duplicates('agentName', ignore_index=True).sort_values(['price'], ascending=[False])
        df['First'] = df.agentName.apply(lambda x: str(x).split(" ")[0])
        df['Last'] = df.agentName.apply(lambda x: str(x).split("-")[-1].split(' ')[-1])
        cols = ['streetAddress', 'price', 'city', 'zipcode', 'state', 'First', 'Last', 'agentEmail', 'agentPhoneNumber',
                'daysOnZillow', 'hdpUrl']
        df = df[cols]
        df.columns = ['Street', 'Price', 'City', 'Zipcode', 'State', 'First', 'Last', 'AgentEmail', 'AgentPhoneNumber',
                      'DaysOnZillow', 'hdpUrl']
        df['Price'] = df['Price'].astype(float).astype("Int32").apply(
            lambda x: "${:,.0f}".format(x) if isinstance(x, int) else x)
        df = df[df.AgentEmail.str.contains("@", na=False)]
        df = df.loc[df['DaysOnZillow'] < 365].reset_index(drop=True)

        os.makedirs(path, exist_ok=True)
        df.to_excel(f"{path}{self.save_name}.xlsx", index=False, engine='xlsxwriter')

    def upload_to_azure_blob(self, path='OUTPUT/raw_csv/', container_name="zillow-storage-blob"):
        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        save_name_azure = f"{self.save_name}{len(self.listing_database)}"
        upload_file_path = os.path.join(os.getcwd(), f"{path}{self.save_name}.csv")

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=save_name_azure)
        print(f"\nUploading to Azure Storage as blob:\n\t{save_name_azure}")
        with open(file=upload_file_path, mode="rb") as data:
            blob_client.upload_blob(data)

    def connect_to_mongodb(self):
        connection_string = os.getenv('MONGO_CONNECTION_STRING')
        try:
            client = MongoClient(connection_string)
            print("Connected to MongoDB successfully!")
            return client
        except Exception as e:
            print(f"Could not connect to MongoDB: {e}")
            return None

    def upload_to_mongodb(self, data):
        db_name = os.getenv("MONGO_DB")
        collection_all = os.getenv("MONGO_COLLECTION")
        collection_nyc = os.getenv("MONGO_COLLECTION_NYC")
        collection_not_nyc = os.getenv("MONGO_COLLECTION_NOT_NYC")

        try:
            json_file = bson.json_util.dumps(data)
            loaded_json = bson.json_util.loads(json_file)

            db = self.mongo_client[db_name]
            db[collection_all].insert_one(loaded_json)

            if self.NYC:
                db[collection_nyc].insert_one(loaded_json)
            else:
                db[collection_not_nyc].insert_one(loaded_json)

        except Exception as e:
            print(f"Could not upload to MongoDB: {e}")

    def exit_program(self):
        """Perform cleanup operations and exit the program."""
        print(f'Previous Price: {self.listing_database[-4]["price"]}')
        print(f'Current Price: {self.current_price}')
        print(f'Starting Price: {self.starting_price}')
        print(f'Listing Count: {len(self.listing_database)}')
        self.save_csv()
        self.create_excel()
        end = time.time()
        elapsed = end - self.start
        hours, rem = divmod(elapsed, 3600)
        minutes, seconds = divmod(rem, 60)
        print(f'Elapsed time: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}')
