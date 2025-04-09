"""
Website validator for better municipal website detection.

This module provides improved website validation to handle cases like Boulder City's 'bcnv.org'
that don't contain the full organization name in the domain.
"""

import re
import time
import requests
from typing import List, Tuple, Optional
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

from app.utils.logger import get_logger

logger = get_logger(__name__)

# Headers to use for requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

# Common official website indicators
OFFICIAL_INDICATORS = [
    "official website",
    "official site",
    "home - city of",
    "home - town of",
    "municipal government",
    "official home page",
    "city government",
    "town government",
    "county government",
    "official web site"
]

# Common contact page paths
CONTACT_PATHS = [
    "/contact-us",
    "/contact",
    "/about/contact",
    "/directory",
    "/staff-directory",
    "/team",
    "/about/staff",
    "/about-us/staff",
    "/about/team",
    "/about-us/team",
    "/staff",
    "/people",
    "/personnel",
    "/departments",
    "/officials",
    "/city-government/departments",
    "/city-hall/departments",
    "/town-hall/departments"
]

# Government/official domain extensions
OFFICIAL_EXTENSIONS = ['.gov', '.org', '.us', '.edu']

# Dictionary of common state abbreviations to match in domain names
STATE_ABBREVIATIONS = {
    'Alabama': 'al', 'Alaska': 'ak', 'Arizona': 'az', 'Arkansas': 'ar', 'California': 'ca',
    'Colorado': 'co', 'Connecticut': 'ct', 'Delaware': 'de', 'Florida': 'fl', 'Georgia': 'ga',
    'Hawaii': 'hi', 'Idaho': 'id', 'Illinois': 'il', 'Indiana': 'in', 'Iowa': 'ia',
    'Kansas': 'ks', 'Kentucky': 'ky', 'Louisiana': 'la', 'Maine': 'me', 'Maryland': 'md',
    'Massachusetts': 'ma', 'Michigan': 'mi', 'Minnesota': 'mn', 'Mississippi': 'ms', 'Missouri': 'mo',
    'Montana': 'mt', 'Nebraska': 'ne', 'Nevada': 'nv', 'New Hampshire': 'nh', 'New Jersey': 'nj',
    'New Mexico': 'nm', 'New York': 'ny', 'North Carolina': 'nc', 'North Dakota': 'nd', 'Ohio': 'oh',
    'Oklahoma': 'ok', 'Oregon': 'or', 'Pennsylvania': 'pa', 'Rhode Island': 'ri', 'South Carolina': 'sc',
    'South Dakota': 'sd', 'Tennessee': 'tn', 'Texas': 'tx', 'Utah': 'ut', 'Vermont': 'vt',
    'Virginia': 'va', 'Washington': 'wa', 'West Virginia': 'wv', 'Wisconsin': 'wi', 'Wyoming': 'wy'
}


def validate_website(url: str, org_name: Optional[str] = None, state: Optional[str] = None) -> Tuple[str, List[str]]:
    """
    Validate a website URL and find contact URLs.
    
    Args:
        url: Website URL to validate
        org_name: Optional organization name for validation
        state: Optional state for validation
        
    Returns:
        Tuple of (validated_url, list_of_contact_urls)
    """
    logger.info(f"Validating website: {url}")
    
    # If URL doesn't start with http/https, add it
    if not url.startswith(('http://', 'https://')):
        url = f"https://{url}"
        logger.info(f"Added https:// prefix: {url}")
    
    # Check if URL is valid
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        
        # If redirected, use the final URL
        if response.url != url:
            logger.info(f"URL redirected: {url} -> {response.url}")
            url = response.url
        
        # Check if response is valid
        if response.status_code != 200:
            logger.warning(f"Invalid response code: {response.status_code}")
            return url, []
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Check title and meta description for official website indicators
        title = soup.title.string if soup.title else ""
        
        if title:
            logger.info(f"Page title: {title}")
            
            # Check if title contains organization name
            if org_name and org_name.lower() in title.lower():
                logger.info(f"Title contains organization name: {org_name}")
            
            # Check for official website indicators
            if any(indicator.lower() in title.lower() for indicator in OFFICIAL_INDICATORS):
                logger.info("Title contains official website indicator")
        
        # Find contact URLs
        contact_urls = find_contact_urls(url, soup)
        logger.info(f"Found {len(contact_urls)} contact URLs")
        
        return url, contact_urls
        
    except Exception as e:
        logger.error(f"Error validating website: {e}")
        return url, []


def find_contact_urls(base_url: str, soup: BeautifulSoup) -> List[str]:
    """
    Find contact page URLs in a website.
    
    Args:
        base_url: Base URL of the website
        soup: BeautifulSoup object of the homepage
        
    Returns:
        List of contact page URLs
    """
    contact_urls = []
    
    # Look for links with contact-related text
    contact_links = soup.find_all('a', string=lambda s: s and any(kw in s.lower() for kw in ['contact', 'directory', 'staff', 'team', 'people']))
    
    for link in contact_links:
        if 'href' in link.attrs:
            href = link['href']
            if href.startswith(('javascript:', '#', 'mailto:', 'tel:')):
                continue
                
            # Resolve relative URL
            full_url = urljoin(base_url, href)
            contact_urls.append(full_url)
    
    # Also look for href attributes containing contact-related keywords
    keyword_links = soup.find_all('a', href=lambda h: h and any(kw in h.lower() for kw in ['contact', 'directory', 'staff', 'team', 'people']))
    
    for link in keyword_links:
        href = link['href']
        if href.startswith(('javascript:', '#', 'mailto:', 'tel:')):
            continue
            
        # Resolve relative URL
        full_url = urljoin(base_url, href)
        if full_url not in contact_urls:
            contact_urls.append(full_url)
    
    # If we didn't find any links, try common paths
    if not contact_urls:
        for path in CONTACT_PATHS:
            contact_url = urljoin(base_url, path)
            try:
                # Check if URL exists
                response = requests.head(contact_url, headers=HEADERS, timeout=5)
                if response.status_code == 200:
                    contact_urls.append(contact_url)
            except:
                # Skip if request fails
                pass
            
            # Small delay
            time.sleep(0.2)
    
    return contact_urls


def is_likely_municipal_domain(domain: str, org_name: Optional[str] = None, state: Optional[str] = None) -> bool:
    """
    Check if a domain is likely a municipal government domain.
    
    Args:
        domain: Domain to check
        org_name: Organization name for comparison
        state: State for comparison
        
    Returns:
        Boolean indicating if domain is likely municipal
    """
    # Extract domain without www. prefix
    if domain.startswith('www.'):
        domain = domain[4:]
    
    # Check for official extensions
    if any(domain.endswith(ext) for ext in OFFICIAL_EXTENSIONS):
        return True
    
    # Check for common municipal domain patterns
    if 'cityof' in domain or 'townof' in domain or 'countyof' in domain:
        return True
    
    # If we have org_name, check for both full name and abbreviation matches
    if org_name:
        # Clean org name and domain for comparison
        clean_org = re.sub(r'[^a-z0-9]', '', org_name.lower())
        clean_domain = re.sub(r'[^a-z0-9]', '', domain.lower())
        
        # Check if domain contains full organization name
        if clean_org in clean_domain:
            return True
        
        # Check for abbreviation match
        org_parts = org_name.lower().split()
        
        # City/town/county of X pattern
        if len(org_parts) >= 3 and org_parts[0] in ['city', 'town', 'county'] and org_parts[1] == 'of':
            # Extract location name (e.g., "boulder" from "city of boulder")
            location = org_parts[2]
            
            # Check first letter abbreviation (e.g., "cob" for "city of boulder")
            abbr = f"{org_parts[0][0]}{org_parts[1][0]}{location[0]}"
            if abbr in clean_domain:
                return True
            
            # Check for just the location name
            if location in clean_domain:
                return True
            
            # Check for first few letters of location
            if len(location) >= 3 and location[:3] in clean_domain:
                return True
        
        # For simple city names (e.g., "Boulder City")
        elif len(org_parts) >= 1:
            # Check for first letters of each word
            if len(org_parts) >= 2:
                abbr = ''.join(part[0] for part in org_parts)
                if abbr in clean_domain:
                    return True
            
            # Check for first word
            if org_parts[0] in clean_domain:
                return True
    
    # Check for state abbreviation if provided
    if state and state in STATE_ABBREVIATIONS:
        state_abbr = STATE_ABBREVIATIONS[state]
        if state_abbr in domain:
            return True
    
    return False