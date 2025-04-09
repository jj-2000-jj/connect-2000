"""
Configuration settings for the GDS Contact Management System.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# API Keys and credentials
MICROSOFT_CLIENT_ID = os.getenv("MICROSOFT_CLIENT_ID")
MICROSOFT_CLIENT_SECRET = os.getenv("MICROSOFT_CLIENT_SECRET")
MICROSOFT_TENANT_ID = os.getenv("MICROSOFT_TENANT_ID")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID")  # Custom Search Engine ID
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY")
LINKEDIN_CLIENT_ID = os.getenv("LINKEDIN_CLIENT_ID")
LINKEDIN_CLIENT_SECRET = os.getenv("LINKEDIN_CLIENT_SECRET")
# Bing Search has been removed

# Database configuration
DATABASE_PATH = BASE_DIR / "data" / "contacts.db"

# Email configuration
EMAIL_DAILY_LIMIT_PER_USER = 1
EMAIL_USERS = {
    "tim@gbl-data.com": {
        "org_types": ["engineering", "government", "transportation", "oil_gas", "agriculture", "commercial_electricians", "well_drillers"]
    },
    "marc@gbl-data.com": {
        "org_types": ["municipal", "water", "utility"]
    },
    "jared@gbl-data.com": {
        "org_types": []
    }
}

# Target states
TARGET_STATES = ["Utah", "Illinois", "Arizona", "Missouri", "New Mexico", "Nevada"]
ILLINOIS_SOUTH_OF_I80 = [
    # List of Illinois counties/cities south of I-80
    "Adams", "Alexander", "Bond", "Brown", "Calhoun", "Cass", "Champaign",
    "Christian", "Clark", "Clay", "Clinton", "Coles", "Crawford", "Cumberland",
    "DeWitt", "Douglas", "Edgar", "Edwards", "Effingham", "Fayette", "Ford",
    "Franklin", "Gallatin", "Greene", "Hamilton", "Hancock", "Hardin", "Jackson",
    "Jasper", "Jefferson", "Jersey", "Johnson", "Lawrence", "Logan", "Macon",
    "Macoupin", "Madison", "Marion", "Mason", "Massac", "McDonough", "McLean",
    "Menard", "Monroe", "Montgomery", "Morgan", "Moultrie", "Perry", "Piatt",
    "Pike", "Pope", "Pulaski", "Randolph", "Richland", "Saline", "Sangamon",
    "Schuyler", "Scott", "Shelby", "St. Clair", "Union", "Vermilion", "Wabash",
    "Warren", "Washington", "Wayne", "White", "Williamson"
]

# Infrastructure and Process Keywords (for identifying relevant organizations)
INFRASTRUCTURE_PROCESS_KEYWORDS = {
    "water": [
        "treatment plant", "distribution system", "water quality monitoring",
        "pumping station", "flow measurement", "chlorination", "filtration",
        "turbidity monitoring", "pH control", "chemical dosing",
        "level sensors", "pressure monitoring", "compliance reporting"
    ],
    "wastewater": [
        "treatment facility", "effluent monitoring", "lift station",
        "aeration control", "sludge processing", "influent screening",
        "discharge monitoring", "odor control", "solids handling",
        "process control"
    ],
    "agriculture": [
        "irrigation system", "center pivot", "drip irrigation", "soil moisture monitoring",
        "water conservation", "precision irrigation", "fertigation", "pump control",
        "water allocation", "multiple water sources", "canal management"
    ],
    "oil_gas": [
        "wellhead monitoring", "pipeline monitoring", "pump station", "compressor station",
        "fluid level", "pressure regulation", "flow metering", "cathodic protection",
        "leak detection"
    ],
    "utility": [
        "substation", "distribution network", "power quality", "outage management",
        "load balancing", "circuit breaker", "transformer monitoring", "fault detection",
        "demand response", "grid management"
    ],
    "municipal": [
        "public works", "utility management", "infrastructure monitoring",
        "municipal service", "city operations", "resource management"
    ],
    "transportation": [
        "traffic management", "signal control", "tunnel system", "ventilation control",
        "tollbooth", "bridge monitoring", "road condition monitoring"
    ],
    "engineering": [
        "facility modernization", "infrastructure projects"
    ],
    "healthcare": [
        "environmental monitoring", "critical systems",
        "air handling", "temperature control", "humidity monitoring",
        "medical gas systems", "equipment monitoring"
    ]
}

# Operational Challenge Keywords (indicating target organizations)
OPERATIONAL_CHALLENGE_KEYWORDS = [
    "distributed operations", "24/7 operations",
    "unmanned facilities", "process optimization",
    "operational efficiency", "compliance reporting", "data logging requirements",
    "alarm management", "emergency response", "continuous monitoring",
    "reduce operator workload", "minimize site visits",
    "real-time visibility", "audit trail", "historical data", "trend analysis",
    "predictive maintenance", "critical infrastructure", "resource optimization"
]

# Regulatory Requirement Keywords (indicating target organizations)
REGULATORY_REQUIREMENT_KEYWORDS = [
    "regulatory compliance", "EPA requirements", "water quality standards",
    "environmental monitoring", "discharge permits", "emissions monitoring",
    "safety regulations", "documentation requirements", "audit requirements",
    "continuous monitoring requirements", "reporting obligations",
    "public health standards", "NPDES permit", "Clean Water Act",
    "Safe Drinking Water Act", "air quality standards"
]

# Industry directory URLs for refined target industries - updated with working URLs
INDUSTRY_DIRECTORIES = {
    "water": [
        "https://www.waterworld.com/water-utility-management/article/14286177/us-water-utilities-directory",
        "https://www.wef.org/resources/water-quality-information/",
        "https://www.epa.gov/ground-water-and-drinking-water/drinking-water-service-information",
        "https://www.amwa.net/membership-directory",
        "https://www.awwa.org/Resources-Tools/Resource-Topics/Source-Water-Protection"
    ],
    "agriculture": [
        "https://www.irrigation.org/IA/Resources/Find-an-Irrigation-Professional/",
        "https://www.nacdnet.org/about-nacd/about-districts/",
        "https://www.fb.org/about/join/state-farm-bureaus",
        "https://www.usda.gov/topics/farming/irrigation-water-management",
        "https://www.nrcs.usda.gov/conservation-basics/water/irrigation"
    ],
    "healthcare": [
        "https://www.aha.org/about/members",
        "https://www.cdc.gov/legionella/health-depts/environ-invest-resources.html",
        "https://www.ashe.org/membership/membership-directory",
        "https://www.ashrae.org/technical-resources/bookstore/ansi-ashrae-standard-188-2018-legionellosis-risk-management-for-building-water-systems",
        "https://www.waterqualityassociation.org/legionella/"
    ],
    "emergency": [
        "https://www.fema.gov/emergency-managers",
        "https://www.iaem.org/resources/online-member-directory",
        "https://www.cisa.gov/critical-infrastructure-sectors",
        "https://www.nema.org/statewide-mutual-aid/what-is-emergency-management",
        "https://www.ready.gov/business/implementation/emergency"
    ]
}

# Improved search queries that focus on finding target organizations without technical terms
IMPROVED_SEARCH_QUERIES = {
    "water": [
        "water treatment plants {state}",
        "water utilities {state}",
        "water district {state}",
        "water authority {state}",
        "municipal water {state}",
        "water department {state}",
        "water quality {state}",
        "water systems {state}"
    ],
    "wastewater": [
        "wastewater treatment facility {state}",
        "sewage treatment {state}",
        "wastewater plant {state}",
        "wastewater operations {state}",
        "municipal wastewater {state}",
        "sewer authority {state}",
        "water reclamation district {state}"
    ],
    "agriculture": [
        "large farms {state}",
        "irrigation district {state}",
        "agricultural operation {state}",
        "farm {state} irrigation",
        "agriculture {state} water",
        "precision agriculture {state}",
        "agricultural water {state}"
    ],
    "oil_gas": [
        "oil production {state}",
        "natural gas {state}",
        "pipeline operations {state}",
        "wellhead {state}",
        "oil field {state}",
        "gas processing {state}"
    ],
    "utility": [
        "electric utility {state}",
        "power company {state}",
        "utility company {state}",
        "power distribution {state}",
        "gas utility {state}",
        "energy {state}"
    ],
    "municipal": [
        "city public works {state}",
        "municipality {state}",
        "local government {state}",
        "city infrastructure {state}",
        "town operations {state}"
    ],
    "transportation": [
        "transportation authority {state}",
        "traffic management {state}",
        "transit agency {state}",
        "highway department {state}",
        "airport authority {state}"
    ],
    "engineering": [
        "engineering firm {state}",
        "civil engineering {state}",
        "consulting engineers {state}",
        "engineering consultants {state}",
        "engineering services {state}"
    ],
    "healthcare": [
        "hospital {state}",
        "healthcare facility {state}",
        "medical center {state}",
        "healthcare complex {state}"
    ]
}

# Define organization relevance indicators - facilities, processes and infrastructure
# that indicate potential target organizations
ORG_RELEVANCE_INDICATORS = {
    "water": {
        "infrastructure": [
            "treatment plant", "distribution system", "pumping station",
            "well field", "reservoir", "storage tank", "booster station"
        ],
        "processes": [
            "disinfection", "filtration", "chemical treatment", "monitoring",
            "sampling", "testing", "quality control", "pressure management"
        ],
        "size_indicators": [
            "service population", "million gallons", "MGD", "service area",
            "square miles", "multiple facilities", "treatment capacity"
        ]
    },
    "wastewater": {
        "infrastructure": [
            "treatment plant", "collection system", "lift station", "pump station",
            "clarifier", "digester", "aeration basin", "lagoon"
        ],
        "processes": [
            "biological treatment", "solids handling", "aeration", "disinfection",
            "sampling", "testing", "discharge monitoring", "sludge processing"
        ],
        "size_indicators": [
            "treatment capacity", "MGD", "service area", "service population",
            "multiple facilities", "collection network"
        ]
    },
    "agriculture": {
        "infrastructure": [
            "irrigation system", "pumping station", "canal network", "reservoir",
            "distribution network", "center pivot", "drip system"
        ],
        "processes": [
            
        ],
        "size_indicators": [
            "acres", "hectares", "water rights", "multiple fields",
            "multiple crops", "large operation"
        ]
    },
    "oil_gas": {
        "infrastructure": [
            "wellhead", "pipeline", "pump station", "compressor station",
            "storage facility", "processing plant", "terminal"
        ],
        "processes": [
            "extraction", "transportation", "processing", "monitoring",
            "pressure regulation", "flow measurement", "leak detection"
        ],
        "size_indicators": [
            "multiple wells", "miles of pipeline", "production capacity",
            "processing capacity", "remote facilities"
        ]
    },
    "utility": {
        "infrastructure": [
            "substation", "distribution network", "transmission line",
            "generating facility", "switching station", "meter station"
        ],
        "processes": [
            "power distribution", "load management", "outage response",
            "voltage regulation", "power quality", "demand response"
        ],
        "size_indicators": [
            "service area", "capacity", "customer count", "multiple substations",
            "miles of line", "generating capacity"
        ]
    },
    "municipal": {
        "infrastructure": [
            "water system", "sewer system", "stormwater system",
            "public works", "multiple facilities", "city infrastructure"
        ],
        "processes": [
            "utility management", "public works operations", "service delivery",
            "resource management", "infrastructure maintenance"
        ],
        "size_indicators": [
            "population served", "area", "multiple departments",
            "facility count", "budget size"
        ]
    },
    "transportation": {
        "infrastructure": [
            "traffic signal network", "tunnel system", "bridge system",
            "toll system", "transit system", "airport infrastructure"
        ],
        "processes": [
            "traffic management", "system monitoring", "safety systems",
            "ventilation control", "lighting control", "access control"
        ],
        "size_indicators": [
            "lane miles", "traffic volume", "passenger count",
            "multiple facilities", "system complexity"
        ]
    },
    "engineering": {
        "infrastructure": [
            "project portfolio", "client facilities", "infrastructure projects",
            "industrial facilities", "municipal clients"
        ],
        "processes": [
            "design services", "consulting", "system specification",
            "project management", "construction management"
        ],
        "size_indicators": [
            "staff count", "office locations", "project size",
            "client diversity", "project diversity"
        ]
    },
    "healthcare": {
        "infrastructure": [
            "hospital building", "medical campus", "central plant",
            "multiple facilities", "critical care areas"
        ],
        "processes": [
            "environmental monitoring", "temperature control", "air handling",
            "medical gas systems", "equipment monitoring"
        ],
        "size_indicators": [
            "bed count", "square footage", "campus size",
            "facility age", "multiple buildings"
        ]
    }
}

# Competitor exclusion indicators - keywords that suggest the organization
# is a SCADA provider rather than a potential client
COMPETITOR_INDICATORS = [
    "SCADA integration services", "control system integration", "system integrator",
    "automation contractor", "SCADA software provider", "automation solutions provider",
    "controls contractor", "industrial automation company", "SCADA programming",
    "PLC programming services", "HMI development", "control panel manufacturer",
    "automation engineering firm", "SCADA system design", "integration specialist",
    "automation consultant", "control systems engineering", "SCADA expertise",
    "we provide SCADA", "SCADA solutions", "SCADA capabilities"
]

# Original search queries (revised to avoid finding competitors)
SEARCH_QUERIES = {
    "water": [
        "water treatment plants {state}",
        "wastewater treatment facilities {state}",
        "water utility {state} compliance",
        "water treatment {state} regulatory",
        "water districts {state}",
        "water authority {state}",
        "water reclamation district {state}",
        "municipal water {state}"
    ],
    "agriculture": [
        "large farms {state} irrigation",
        "irrigation districts {state}",
        "agricultural operations {state} water management",
        "farm {state} complex irrigation",
        "agriculture {state} water conservation",
        "precision agriculture {state}",
        "farm {state} water",
        "agricultural water management {state}"
    ],
    "healthcare": [
        "hospitals {state} water management",
        "medical centers {state}",
        "healthcare facilities {state} water safety",
        "hospital {state} water program",
        "medical facility {state} water quality",
        "healthcare {state} legionella",
        "hospital engineering {state} water",
        "medical center {state} facilities water"
    ],
    "emergency": [
        "emergency management agencies {state}",
        "critical infrastructure {state}",
        "emergency operations center {state}",
        "public safety {state}",
        "emergency management {state}",
        "critical infrastructure protection {state}",
        "emergency response {state}",
        "disaster management {state}"
    ]
}

# Classification keywords for organization categorization based on refined specifications
CLASSIFICATION_KEYWORDS = {
    "water": [
        "water district", "water authority", "water department", "wastewater",
        "sewage", "water treatment", "water reclamation", "freshwater treatment",
        "potable water", "treatment plant", "water utility", "water quality",
        "water management", "public water", "water compliance", "water systems"
    ],
    "agriculture": [
        "agriculture", "farm", "irrigation", "crop", "agricultural", "farming",
        "irrigation district", "water conservation", "precision agriculture",
        "agricultural water", "farm operations", "irrigation systems", "water resources"
    ],
    "healthcare": [
        "hospital", "medical center", "healthcare facility", "medical facility",
        "patient care", "legionella", "water safety", "healthcare", "patient safety",
        "medical", "health system", "healthcare system", "healthcare services"
    ],
    "emergency": [
        "emergency management", "critical infrastructure", "emergency operations",
        "public safety", "emergency response", "disaster management", "alerting system",
        "notification system", "emergency services", "emergency preparedness", "crisis management"
    ]
}

# Target organization types based on refined specifications
ORG_TYPES = {
    "water": {
        "description": "Water Utilities",
        "subtypes": ["freshwater treatment", "wastewater treatment", "water distribution", "wastewater collection"],
        "job_titles": [
            "Water Operations Manager", "Treatment Plant Director", "Plant Manager",
            "Operations Manager", "Water Quality Manager", "Compliance Manager",
            "Systems Manager", "Process Engineer", "Instrumentation Technician",
            "Chief Engineer", "Engineering Manager", "VP of Operations",
            "Director of Engineering", "Water Systems Director"
        ],
        "relevance_criteria": [
            "compliance monitoring", "regulatory requirements", "water quality control",
            "treatment processes", "data logging"
        ]
    },
    "agriculture": {
        "description": "Agricultural Operations",
        "subtypes": ["farms with irrigation", "irrigation district", "agricultural water management"],
        "job_titles": [
            "Irrigation Manager", "Farm Operations Director", "District Manager",
            "Operations Manager", "Irrigation System Manager", "Agricultural Engineer",
            "Farm Technology Manager", "Water Resources Manager"
        ],
        "relevance_criteria": [
            "complex irrigation needs", "multiple water sources", "water conservation",
            "irrigation systems", "field monitoring", "precision agriculture"
        ]
    },
    "healthcare": {
        "description": "Healthcare Facilities",
        "subtypes": ["hospitals", "medical centers", "long-term care facilities"],
        "job_titles": [
            "Facilities Director", "Hospital Engineer", "Safety Officer",
            "Director of Plant Operations", "Maintenance Manager", "Compliance Officer",
            "Environmental Services Director", "Water Safety Manager"
        ],
        "relevance_criteria": [
            "legionella monitoring", "water safety plans", "systems monitoring",
            "regulatory compliance", "patient safety", "water management program"
        ]
    },
    "emergency": {
        "description": "Emergency Management and Critical Infrastructure",
        "subtypes": ["emergency operations", "critical infrastructure", "public safety"],
        "job_titles": [
            "Emergency Manager", "Operations Director", "Critical Infrastructure Manager",
            "Public Safety Director", "Emergency Services Coordinator", "Disaster Response Manager",
            "Systems Administrator", "Technical Operations Manager"
        ],
        "relevance_criteria": [
            "alerting systems", "emergency response", "critical infrastructure protection",
            "public notification systems", "resilience planning", "backup systems"
        ]
    }
}

# Crawler settings
CRAWLER_MAX_DEPTH = 3
CRAWLER_MAX_PAGES_PER_DOMAIN = 500
CRAWLER_POLITENESS_DELAY = 2  # seconds between requests

# NLP model settings
NLP_CONFIDENCE_THRESHOLD = 0.5

# Dashboard settings
DASHBOARD_PORT = 8050

# Logging configuration
LOG_DIR = BASE_DIR / "logs"
LOG_LEVEL = "INFO"

# Enhanced Discovery settings
CHECKPOINT_DIR = BASE_DIR / "data" / "checkpoints"
CONCURRENCY_LIMIT = 5  # Maximum number of concurrent discovery tasks
CIRCUIT_BREAKER_THRESHOLD = 3  # Number of failures before opening circuit breaker
CIRCUIT_BREAKER_RESET_TIME = 30  # Minutes before resetting circuit breaker
DEFAULT_MAX_ORGS_PER_RUN = 50  # Default maximum organizations per discovery run
DEFAULT_MAX_CONTACTS_PER_ORG = 10  # Default maximum contacts per organization
MIN_RELEVANCE_SCORE = 5  # Only proceed with contacts scoring 5+ for relevance

# Exclusion criteria for organizations
EXCLUDED_ORGANIZATION_TYPES = {
    "competitors": [
        "scada integrator", "automation company", "control systems integrator",
        "industrial automation", "systems integrator", "automation integrator",
        "controls contractor", "automation contractor", "plc programming"
    ],
    "irrelevant_sectors": [
        "retail", "consumer goods", "technology", "software", "financial",
        "banking", "insurance", "education", "restaurant", "hospitality",
        "entertainment", "media", "marketing", "advertising"
    ],
    "exclusion_keywords": [
        "automation systems", "controls systems", "industrial controls",
        "control system integration", "scada integration", "control systems",
        "plc integration", "hmi development", "automation solutions"
    ]
}

# Organization taxonomy settings
ORGANIZATION_TYPES = {
    "engineering": {
        "description": "Engineering Firms",
        "subtypes": ["civil", "electrical", "environmental", "mechanical", "multidisciplinary"],
        "keywords": ["engineering", "design", "consultant", "construction management", "project", "technical services"],
        "search_queries": [
            "engineering firms {state}",
            "civil engineering companies {state}",
            "electrical engineering consultants {state}"
        ],
        "industry_associations": [
            "American Council of Engineering Companies (ACEC)",
            "National Society of Professional Engineers (NSPE)",
            "American Society of Civil Engineers (ASCE)"
        ]
    },
    "government": {
        "description": "Government Agencies",
        "subtypes": ["federal", "state", "county", "agency"],
        "keywords": ["agency", "department", "bureau", "administration", "government", "federal", "regulatory"],
        "search_queries": [
            "government agencies {state} infrastructure",
            "state agencies {state} water management",
            "regulatory agencies {state} utilities"
        ],
        "industry_associations": [
            "National Association of State Departments",
            "National Association of Counties",
            "American Public Works Association"
        ]
    },
    "municipal": {
        "description": "Municipalities",
        "subtypes": ["city", "town", "village", "borough"],
        "keywords": ["city of", "town of", "village of", "municipal", "public works", "local government"],
        "search_queries": [
            "municipalities {state} public works",
            "city government {state} water department",
            "local government {state} utilities"
        ],
        "industry_associations": [
            "National League of Cities",
            "US Conference of Mayors",
            "International City/County Management Association"
        ]
    },
    "water": {
        "description": "Water and Wastewater Districts",
        "subtypes": ["water district", "water authority", "wastewater district", "water treatment"],
        "keywords": ["water", "wastewater", "treatment", "sewage", "reclamation", "potable", "hydrology"],
        "search_queries": [
            "water districts {state}",
            "water treatment facilities {state}",
            "wastewater management {state}"
        ],
        "industry_associations": [
            "American Water Works Association",
            "Water Environment Federation",
            "Association of Metropolitan Water Agencies"
        ]
    },
    "utility": {
        "description": "Utility Companies",
        "subtypes": ["electric", "gas", "power", "telecom", "renewable"],
        "keywords": ["utility", "power", "electric", "gas", "energy", "distribution", "generation"],
        "search_queries": [
            "utility companies {state}",
            "power generation companies {state}",
            "electrical utilities {state}"
        ],
        "industry_associations": [
            "American Public Power Association",
            "Edison Electric Institute",
            "American Gas Association"
        ]
    },
    "transportation": {
        "description": "Transportation Authorities",
        "subtypes": ["transit", "airport", "railway", "highway", "traffic"],
        "keywords": ["transportation", "transit", "traffic", "rail", "airport", "road", "highway"],
        "search_queries": [
            "transportation authority {state}",
            "transit agency {state}",
            "traffic management {state}"
        ],
        "industry_associations": [
            "American Public Transportation Association",
            "American Association of State Highway and Transportation Officials",
            "American Road & Transportation Builders Association"
        ]
    },
    "oil_gas": {
        "description": "Oil and Gas Companies",
        "subtypes": ["extraction", "refining", "pipeline", "distribution", "exploration"],
        "keywords": ["oil", "gas", "petroleum", "pipeline", "refinery", "drilling", "extraction"],
        "search_queries": [
            "oil companies {state}",
            "gas extraction {state}",
            "pipeline operations {state}"
        ],
        "industry_associations": [
            "American Petroleum Institute",
            "Independent Petroleum Association of America",
            "Interstate Natural Gas Association of America"
        ]
    },
    "agriculture": {
        "description": "Agriculture and Irrigation",
        "subtypes": ["irrigation district", "farm", "agricultural operation", "co-op"],
        "keywords": ["irrigation", "agriculture", "farm", "crop", "cultivation", "growing"],
        "search_queries": [
            "irrigation districts {state}",
            "agricultural water management {state}",
            "farming operations {state} irrigation"
        ],
        "industry_associations": [
            "Irrigation Association",
            "National Association of Conservation Districts",
            "Farm Bureau Federation"
        ]
    },
    "commercial_electricians": {
        "description": "Commercial Electricians",
        "subtypes": ["electrical contractor", "commercial electrical", "industrial electrical"],
        "keywords": ["electrical", "electrician", "wiring", "commercial electrical", "electrical contractor", "electrical systems"],
        "search_queries": [
            "commercial electricians {state}",
            "electrical contractors {state}",
            "industrial electricians {state}"
        ],
        "industry_associations": [
            "National Electrical Contractors Association (NECA)",
            "Independent Electrical Contractors (IEC)",
            "Associated Builders and Contractors (ABC)"
        ]
    },
    "well_drillers": {
        "description": "Well Drillers",
        "subtypes": ["water well", "geothermal", "groundwater", "drilling contractor"],
        "keywords": ["well drilling", "water well", "groundwater", "borehole", "drilling", "pump installation"],
        "search_queries": [
            "well drilling companies {state}",
            "water well contractors {state}",
            "groundwater drilling {state}"
        ],
        "industry_associations": [
            "National Ground Water Association (NGWA)",
            "Water Systems Council",
            "International Association of Drilling Contractors"
        ]
    }
}

# Update the configuration to use production settings
USE_DEMO_MODE = False
USE_PRODUCTION_DATA = True
GENERATE_FAKE_DATA = False
GENERATE_TEST_EMAILS = False
