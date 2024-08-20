
from enum import Enum

class NYC(Enum):
    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))
    
    DATE = "DATE"
    AVG_SALES = "AVG_SALES"
    SALES_COUNT = "SALES_COUNT"
    COMPLAINTS = "COMPLAINTS"
    CITI = "CITI"
    BUSINESSES = "BUSINESSES"
    EVICTIONS = "EVICTIONS"
    HEALTH = "HEALTH"

class Zillow(Enum):
    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))
    
    INVENTORY = "INVENTORY"
    PERCENT_LISTINGS_PRICE_CUT = "PERCENT_LISTINGS_PRICE_CUT"
    SALES_COUNT = "SALES_COUNT"
    VALUE_INDEX = "VALUE_INDEX"
    MEDIAN_SALE_PRICE = "MEDIAN_SALE_PRICE"
    AVG_PRICE = "AVG_PRICE"
    
class Redfin(Enum):
    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))
    AVG_PRICE = "AVG_PRICE"
    HOMES_SOLD = "HOMES_SOLD"
    NEW_LISTINGS = "NEW_LISTINGS"
    INVENTORY = "INVENTORY"
    DAYS_ON_MARKET = "DAYS_ON_MARKET"
    AVG_SALE_TO_LIST = "AVG_SALE_TO_LIST"
    
class ETF(Enum):
    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))
    KBWY = "KBWY"
    IYR = "IYR"
    MORT = "MORT"
    NURE = "NURE"
    REET = "REET"
    RWR = "RWR"
    SCHH = "SCHH"
    VNQ = "VNQ"
