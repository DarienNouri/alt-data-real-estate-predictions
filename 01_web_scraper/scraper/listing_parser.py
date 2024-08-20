"""
Purpose: Helper functions for parsing Zillow property data from HTML and JSON.
Includes methods for extracting location, pricing, property features, and other relevant information.
"""

import json
from datetime import datetime

import censusgeocode as cg

from data_model_entities import *


def parse_json(soup):
    """Parse JSON data from Zillow's HTML structure"""
    if soup is None:
        return None

    try:
        parsed_og = json.loads(soup.find("script", {"id": "hdpApolloPreloadedData"}).contents[0].strip("!<>-\\"))
        json_payload = json.loads(parsed_og['apiCache'])
        if json_payload is not None:
            return json_payload[list(json_payload.keys())[1]]['property']
    except:
        get_tag = soup.find("script", {"id": "__NEXT_DATA__"})
        if get_tag:
            try:
                parsed_new = json.loads(get_tag.contents[0].strip("!<>-\\"))
                json_payload = json.loads(parsed_new['props']['pageProps']['gdpClientCache'])
                return json_payload[list(json_payload.keys())[0]]['property']
            except:
                return None
    return None


def extract_data(parsed_json, data_class):
    """Extract data based on the keys defined in the data class"""
    data = {}
    for key in data_class.keys:
        if key == 'utcScrapeTime':
            data[key] = str(datetime.utcnow())
        elif key in ['nearbyCities', 'nearbyNeighborhoods', 'nearbyZipcodes']:
            data[key] = [i.get('name') for i in parsed_json.get(key, [])]
        else:
            data[key] = parsed_json.get(key)
    return data_class(data=data)


def extract_pricing_data(parsed_json):
    """Extract pricing data from parsed JSON"""
    data = extract_data(parsed_json, Pricing())
    price_history = data.data.get('priceHistory', [])
    for item in price_history:
        for key in ['buyerAgent', 'sellerAgent', 'showCountyLink', 'attributeSource']:
            item.pop(key, None)
    data.data['priceHistory'] = price_history
    return data


def extract_property_features(parsed_json):
    """Extract property features data"""
    return extract_data(parsed_json.get('resoFacts', {}), PropertyFeatures())


def extract_listing_agent_data(parsed_json):
    """Extract listing agent data"""
    attribution_info = parsed_json.get('attributionInfo', {})
    data = extract_data(attribution_info, ListingAgent())
    agent_name = attribution_info.get('agentName', '').split()
    data.data['agentFirstName'] = agent_name[0] if agent_name else None
    data.data['agentLastName'] = agent_name[-1] if len(agent_name) > 1 else None
    data.data['listingOriginUrl'] = parsed_json.get('postingUrl')
    return data


def extract_school_data(parsed_json):
    return [extract_data(school, School()) for school in parsed_json.get('schools', [])]


def extract_picture_data(parsed_json):
    data = extract_data(parsed_json, PictureData())
    data.data['propertyPhotos'] = [pic.get('url') for i in parsed_json.get('photos', [])
                                   for pic in i.get('mixedSources', {}).get('jpeg', []) if pic.get('width') == 1536]
    data.data['staticMap'] = [map.get('url') for map in parsed_json.get('staticMap', {}).get('sources', []) if
                              map.get('width') == 768]
    return data


def get_census_data(listing_data):
    """Get and parse census data for the property"""

    def request_census_data(street, city, state):
        try:
            return cg.address(street, city, state)[0]
        except:
            return None

    def parse_census_data(cdata):
        census_block_data = cdata['geographies']['2020 Census Blocks'][0]
        data = {
            'geoId': census_block_data.get('GEOID'),
            'block': census_block_data.get('BLOCK'),
            'tract': census_block_data.get('TRACT'),
            'blkgrp': census_block_data.get('BLKGRP'),
            'addressComponents': cdata['addressComponents'],
            'addressFull': cdata['matchedAddress']
        }
        return CensusData(data=data)

    cdata = request_census_data(listing_data.location.data['streetAddress'],
                                listing_data.location.data['city'],
                                listing_data.location.data['state'])
    return parse_census_data(cdata) if cdata else None


def extract_comp_nearby_homes(parsed_json):
    keys_to_pop = [
        'livingAreaUnits', 'miniCardPhotos', 'livingAreaUnitsShort', 'listingMetadata',
        'formattedChip', 'attributionInfo', 'providerListingID', 'address'
    ]

    def process_homes(homes):
        for home in homes:
            address = home.pop('address', {})
            home.update(address)
            pic_container = home.pop('miniCardPhotos', [])
            for key in keys_to_pop:
                home.pop(key, None)
            if pic_container:
                home['coverPhoto'] = pic_container[0].get('url')
        return homes

    return {
        'comps': process_homes(parsed_json.get('comps', [])),
        'nearby_homes': process_homes(parsed_json.get('nearbyHomes', []))
    }


def parse_zillow_listing(soup):
    """Parse Zillow listing data from BeautifulSoup object"""
    parsed_json = parse_json(soup)
    if not parsed_json:
        return None

    listing = ZillowListing(
        location=extract_data(parsed_json, Location()),
        pricing=extract_pricing_data(parsed_json),
        property_features=extract_property_features(parsed_json),
        listing_agent=extract_listing_agent_data(parsed_json),
        schools=extract_school_data(parsed_json),
        picture_data=extract_picture_data(parsed_json)
    )

    comp_nearby_data = extract_comp_nearby_homes(parsed_json)
    listing.comps = comp_nearby_data['comps']
    listing.nearby_homes = comp_nearby_data['nearby_homes']

    listing.census_data = get_census_data(listing)

    return listing
