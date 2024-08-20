"""
Dataclass definitions for Zillow property data with dynamic key assignment.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any


@dataclass
class ZillowDataClass:
    keys: List[str] = field(default_factory=list)
    data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        for key in self.keys:
            if key not in self.data:
                self.data[key] = None


@dataclass
class Location(ZillowDataClass):
    keys: List[str] = field(default_factory=lambda: [
        'streetAddress', 'city', 'state', 'stateId', 'zipcode', 'zipPlusFour',
        'cityId', 'longitude', 'latitude', 'timeZone', 'zpid', 'neighborhoodRegion',
        'nearbyCities', 'nearbyNeighborhoods', 'nearbyZipcodes', 'utcScrapeTime'
    ])


@dataclass
class Pricing(ZillowDataClass):
    keys: List[str] = field(default_factory=lambda: [
        'price', 'zestimate', 'zestimateLowPercent', 'zestimateHighPercent',
        'rentZestimate', 'restimateLowPercent', 'restimateHighPercent', 'taxHistory',
        'priceHistory', 'mortgageZHLRates', 'propertyTaxRate', 'sellingSoon', 'foreclosureTypes'
    ])


@dataclass
class PropertyFeatures(ZillowDataClass):
    keys: List[str] = field(default_factory=lambda: [
        'isNewConstruction', 'associationFee', 'hoaFee', 'hoaFeeTotal',
        'buildingName', 'buyerAgencyCompensation', 'buyerAgencyCompensationType',
        'hasAssociation', 'basement', 'bathrooms', 'bathroomsFull', 'bathroomsHalf',
        'bedrooms', 'hasGarage', 'homeType', 'garageParkingCapacity',
        'fireplaces', 'hasFireplace', 'parkingCapacity', 'pricePerSquareFoot',
        'stories', 'structureType', 'hasPrivatePool', 'lotSize', 'hasSpa', 'hasView',
        'hasWaterfront', 'hasCooling', 'hasHeating', 'yearBuilt', 'zoning', 'zoningDescription'
    ])


@dataclass
class ListingAgent(ZillowDataClass):
    keys: List[str] = field(default_factory=lambda: [
        'agentName', 'agentEmail', 'agentLicenseNumber', 'agentPhoneNumber', 'brokerName',
        'brokerPhoneNumber', 'buyerAgentName', 'buyerBrokerageName', 'coAgentName', 'coAgentNumber',
        'lastChecked', 'lastUpdated', 'listingOffices', 'listingAgents', 'mlsName', 'mlsId',
        'listingOriginUrl'
    ])


@dataclass
class School(ZillowDataClass):
    keys: List[str] = field(default_factory=lambda: [
        'name', 'type', 'gradeRange', 'rating', 'distance'
    ])


@dataclass
class PictureData(ZillowDataClass):
    keys: List[str] = field(default_factory=lambda: [
        'hiResLink', 'propertyPhotos', 'staticMap',
        'streetViewMetadataUrlMediaWallLatLong', 'streetViewMetadataUrlMediaWallAddress',
        'streetViewTileImageUrlMediumLatLong', 'streetViewTileImageUrlMediumAddress',
        'streetViewServiceUrl'
    ])


@dataclass
class CensusData(ZillowDataClass):
    keys: List[str] = field(default_factory=lambda: [
        'geoId', 'block', 'tract', 'blkgrp', 'addressComponents', 'addressFull'
    ])


@dataclass
class ZillowListing:
    location: Location = field(default_factory=Location)
    pricing: Pricing = field(default_factory=Pricing)
    property_features: PropertyFeatures = field(default_factory=PropertyFeatures)
    listing_agent: ListingAgent = field(default_factory=ListingAgent)
    schools: List[School] = field(default_factory=list)
    picture_data: PictureData = field(default_factory=PictureData)
    census_data: CensusData = field(default_factory=CensusData)
    comps: List[dict] = field(default_factory=list)
    nearby_homes: List[dict] = field(default_factory=list)
