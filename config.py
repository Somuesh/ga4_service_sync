import os
from dotenv import load_dotenv
load_dotenv()

# Mongo config
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/ga4_service_db')
MONGO_DB = os.getenv('MONGO_DB', 'ga4_service_db')
GA_JOBS = os.getenv('GA_JOBS', 'ga_jobs')

# GA4 property and client secrets
CLIENT_SECRETS_FILE = os.getenv('CLIENT_SECRETS_FILE')
GA4_PROPERTY_ID = os.getenv('GA4_PROPERTY_ID')

# Combined report (as found in original repo)
COMBINED_DIMENSIONS = ['country', 'deviceCategory', 'landingPagePlusQueryString', 'streamId']
COMBINED_METRICS = ['bounceRate', 'organicGoogleSearchAveragePosition', 'organicGoogleSearchClicks', 'organicGoogleSearchClickThroughRate', 'organicGoogleSearchImpressions', 'sessions']

# Dimension->metric map (exact mapping from original source)
DIMENSION_METRIC_MAP = {
    'pageTitle': ['activeUsers','averageSessionDuration','bounceRate','engagedSessions','engagementRate','eventCount','eventCountPerUser','sessions','totalUsers','newUsers'],
    'country': ['activeUsers','averageSessionDuration','bounceRate','engagedSessions','engagementRate','eventCount','eventCountPerUser','sessions','totalUsers','newUsers'],
    'deviceCategory': ['activeUsers','averageSessionDuration','bounceRate','engagedSessions','engagementRate','eventCount','eventCountPerUser','sessions','totalUsers','newUsers'],
    'sessionDefaultChannelGroup': ['activeUsers','averageSessionDuration','bounceRate','engagedSessions','engagementRate','eventCount','eventCountPerUser','sessions','totalUsers','newUsers'],
    'eventName': ['activeUsers','averageSessionDuration','bounceRate','engagedSessions','engagementRate','eventCount','eventCountPerUser','eventValue','sessions','totalUsers'],
    'itemName': ['itemsViewed','itemsAddedToCart','itemsPurchased','itemRevenue']
}

# Unique keys for collections (used to upsert)
DIMENSION_UNIQUE_KEYS = {
    'combined_dimensions': ['id']
}

# App behavior
MAX_METRICS_PER_REQUEST = 9
