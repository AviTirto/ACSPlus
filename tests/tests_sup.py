from census import Census
from us import states
from dotenv import load_dotenv
import os
import requests
load_dotenv()
c = Census(os.getenv("CENSUS_KEY"))

# Example: Getting a supplementary estimate variable (check if it's available)
#result = c.acs1.state(('NAME', 'K202101_002E'), states.WI.fips, year=2022)
#print(result)

def get_acsse_data(codes, state_fips, county_fips, api_key):
    """
    Fetches ACS Supplementary Estimates data for given variable codes, state, and county FIPS codes.

    Parameters:
        codes (list of str): List of ACS variable codes (e.g., ['K200101_001E']).
        state_fips (str): 2-digit FIPS code for the state.
        county_fips (str): 3-digit FIPS code for the county.
        api_key (str): Your Census API key.

    Returns:
        list: A list of dictionaries containing the requested data.
    """
    base_url = "https://api.census.gov/data/2023/acs/acsse"
    
    # Prepare the variable list for the API call
    get_vars = ','.join(['NAME'] + codes)

    # Construct the request URL
    params = {
        "get": get_vars,
        "for": f"county:{county_fips}",
        "in": f"state:{state_fips}",
        "key": api_key
    }

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()
        # Turn the first row into keys and zip them with the value rows
        headers = data[0]
        values = data[1:]
        return [dict(zip(headers, row)) for row in values]
    else:
        raise Exception(f"Request failed: {response.status_code} - {response.text}")


codes = ['K200101_001E', 'K202801_001E']
state_fips = '55'  # Wisconsin
county_fips = '025'  # Dane County
api_key = os.getenv("CENSUS_KEY")

data = get_acsse_data(codes, state_fips, county_fips, api_key)
print(data)
c = Census(os.getenv('CENSUS_KEY'))
result = c.acs1.state_county(['NAME'] + ['B24022_060E','B19001B_014E'], state_fips, county_fips, year=2023)
print(result)
