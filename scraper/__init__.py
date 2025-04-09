"""
Scraper package for the GBL Data Contact Management System.
"""
from app.scraper.engineering.engineering_scraper import EngineeringScraper
from app.scraper.water.water_scraper import WaterScraper
from app.scraper.government.government_scraper import GovernmentScraper
from app.scraper.municipal.municipal_scraper import MunicipalScraper
from app.scraper.utilities.utilities_scraper import UtilitiesScraper
from app.scraper.transportation.transportation_scraper import TransportationScraper
from app.scraper.oil_gas.oil_gas_scraper import OilGasScraper
from app.scraper.agriculture.agriculture_scraper import AgricultureScraper

__all__ = [
    "EngineeringScraper", 
    "WaterScraper", 
    "GovernmentScraper", 
    "MunicipalScraper", 
    "UtilitiesScraper",
    "TransportationScraper",
    "OilGasScraper",
    "AgricultureScraper"
]