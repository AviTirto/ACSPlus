import os
import time
import pandas as pd
from datetime import timedelta
from rapidfuzz import process, fuzz
from census import Census
from dotenv import load_dotenv
import requests

load_dotenv()

class ACS1:
    def __init__(self):
        project_root = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(project_root, 'data')
        os.makedirs(data_dir, exist_ok=True)

        # Check if ACS1 variables are cached
        if not os.path.exists(os.path.join(data_dir, 'acs1_vars.parquet')):
            self.vars_df = self.load_acs1_vars()
        
        # Check if ACS1 tables are cached
        if not os.path.exists(os.path.join(data_dir, 'acs1_groups.parquet')):
            self.groups_df = self.load_acs1_groups()

        # Check if FipsMapping is cached
        if not os.path.exists(os.path.join(data_dir, 'fips_mapping.csv')):
            self.fips_mapping = self.load_fips_mapping()

        # Load ACS1 variables and tables
        project_root = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(project_root, 'data')
        os.makedirs(data_dir, exist_ok=True)
        self.vars_df = pd.read_parquet(os.path.join(data_dir, 'acs1_vars.parquet'))
        self.groups_df = pd.read_parquet(os.path.join(data_dir, 'acs1_groups.parquet'))
        self.fips_mapping = pd.read_csv(os.path.join(data_dir, 'fips_mapping.csv'))
        self.c = Census(os.getenv('CENSUS_KEY'))
        

    # Loads ACS1 variable metadata from Census API and saves it to a Parquet file
    def load_acs1_vars(self):
        # Initialize ACS1 variable DataFrame
        vars_df = pd.DataFrame(columns=['code', 'concept', 'label', 'year'])

        # Loop through available ACS1 years
        for year in range(2005, 2023):
            # ACS1 did not collect data in 2020
            if year == 2020:
                continue

            # Grabbing ACS1 variable metadata from Census API
            start = time.time()

            # Fetch ACS1 variable for current year
            year_vars_df = pd.read_json(f'https://api.census.gov/data/{year}/acs/acs1/variables.json')

            # Clean up the DataFrame so that it is indexed by variable code
            year_vars_df.reset_index(inplace=True)
            expanded = pd.json_normalize(year_vars_df['variables'])
            year_vars_df.rename(columns={'index': 'code'}, inplace=True)
            year_vars_df = pd.concat([year_vars_df['code'], expanded], axis = 1)[['code', 'label', 'concept', 'group']].copy()
            year_vars_df['year'] = [year] * len(year_vars_df)

            # Stacking yearly variable metadata into one long DataFrame
            vars_df = pd.concat([vars_df, year_vars_df], axis=0)

            # Get time taken to fetch and process Variable metadata for current year
            end = time.time()
            elapsed = timedelta(seconds=end - start)
            print(f'Grabbing ACS1 vars for {year} - {str(elapsed)}')
            
        #Calls the helper function to handle the Supplemental data
        acsse_vars = self.load_acsse_vars()
        vars_df = pd.concat([vars_df, acsse_vars], axis=0)

        # At this point there are a lot of duplicate variable codes in the long dataframe
        # This is because an ACS1 variables exist in multiple years
        # We need to remove duplicates and track all the years that a variable exists in a new column

        start = time.time()

        # Make a a Panda Series of a list of years a code exists in
        years_map = vars_df.groupby('code')['year'].agg(list)

        # Combine with long ACS1 variable metadata DataFrame
        vars_df = vars_df[['code', 'concept', 'label', 'group']].drop_duplicates('code').set_index('code')
        years_map = years_map.reset_index().set_index('code')
        vars_df = vars_df.join(years_map, on='code')

        # Final cleanup of variables DataFrame
        vars_df.rename(columns={'year': 'years'}, inplace=True)

        # Print time taken to flatten ACS1 variable metadata
        end = time.time()
        elapsed = timedelta(seconds=end - start)
        print(f'Flattening ACS1 vars for {year} - {str(elapsed)}')

        # Save ACS1 variable metadata to Parquet file
        project_root = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(project_root, 'data')
        os.makedirs(data_dir, exist_ok=True)

        vars_df.to_parquet(os.path.join(data_dir, 'acs1_vars.parquet'))
        
    
    #Loads ACSSE Variables metadata from Census API and saves it to a Parquet file
    def load_acsse_vars(self):
        vars_df = vars_df = pd.DataFrame(columns=['code', 'concept', 'label', 'year'])
         #SUPPLEMENTAL BEGINNING
        for year in range(2014, 2024):
            # ACSSE did not collect data in 2020
            if year == 2020:
                continue

            # Grabbing ACSSE variable metadata from Census API
            start = time.time()

            # Fetch ACSSE variable for current year
            year_vars_df = pd.read_json(f'https://api.census.gov/data/{year}/acs/acsse/variables.json')

            # Clean up the DataFrame so that it is indexed by variable code
            year_vars_df.reset_index(inplace=True)
            expanded = pd.json_normalize(year_vars_df['variables'])
            year_vars_df.rename(columns={'index': 'code'}, inplace=True)
            year_vars_df = pd.concat([year_vars_df['code'], expanded], axis = 1)[['code', 'label', 'concept', 'group']].copy()
            year_vars_df['year'] = [year] * len(year_vars_df)

            # Stacking yearly variable metadata into one long DataFrame
            vars_df = pd.concat([vars_df, year_vars_df], axis=0)

            # Get time taken to fetch and process Variable metadata for current year
            end = time.time()
            elapsed = timedelta(seconds=end - start)
            print(f'Grabbing ACSSE vars for {year} - {str(elapsed)}')
            
        

    # Loads ACS1 table metadata from Census API and saves it to a Parquet file
    def load_acs1_groups(self):
        # Initialize ACS1 groups DataFrame
        groups_df = pd.DataFrame(columns=['gcode', 'description'])

        # Loop through available ACS1 years
        for year in range(2005, 2023):

            # ACS1 did not collect data in 2020
            if year == 2020:
                continue

            # Grabbing ACS1 table metadata from Census API for current year
            start = time.time()
            year_groups_df = pd.read_json(f'https://api.census.gov/data/{year}/acs/acs1/groups.json')

            # Clean up the DataFrame 
            year_groups_df.reset_index(inplace=True)
            year_groups_df = pd.json_normalize(year_groups_df['groups'])
            year_groups_df.rename(columns={'name': 'gcode'}, inplace=True)
            year_groups_df.drop(columns=['variables'], inplace=True)

            # Stacking yearly table metadata into one long DataFrame
            groups_df = pd.concat([groups_df, year_groups_df], axis=0)

            # Print time taken to fetch and process table metadata for current year
            end = time.time()
            elapsed = timedelta(seconds=end - start)
            print(f'Grabbing ACS1 groups for {year} - {str(elapsed)}')

        # At this point there are a lot of duplicate group codes in the long dataframe
        # This is because an ACS1 group exists in multiple years
        # We just need to drop the duplicates
        groups_df = groups_df.drop_duplicates('gcode')[['gcode','description']]
        groups_df['source'] = 'ACS1'
        
        #Calls the helper function to handle supplemental data
        acsse_groups = self.load_acsse_groups()
        groups_df = pd.concat([groups_df, acsse_groups], axis=0)
        
        # Save ACS1 table metadata to Parquet file
        project_root = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(project_root, 'data')
        os.makedirs(data_dir, exist_ok=True)

        groups_df.to_parquet(os.path.join(data_dir, 'acs1_groups.parquet'))
        
        
    
    ## Loads ACSSE table metadata from Census API and saves it to a Parquet file
    def load_acsse_groups(self):
        groups_df = pd.DataFrame(columns=['gcode', 'description'])
        for year in range(2014, 2024):

            # ACSSE did not collect data in 2020
            if year == 2020:
                continue

            # Grabbing ACSSE table metadata from Census API for current year
            start = time.time()
            year_groups_df = pd.read_json(f'https://api.census.gov/data/{year}/acs/acsse/groups.json')

            # Clean up the DataFrame 
            year_groups_df.reset_index(inplace=True)
            year_groups_df = pd.json_normalize(year_groups_df['groups'])
            year_groups_df.rename(columns={'name': 'gcode'}, inplace=True)
            year_groups_df.drop(columns=['variables'], inplace=True)
            year_groups_df['source'] = 'ACSSE'

            # Stacking yearly table metadata into one long DataFrame
            groups_df = pd.concat([groups_df, year_groups_df], axis=0)

            # Print time taken to fetch and process table metadata for current year
            end = time.time()
            elapsed = timedelta(seconds=end - start)
            print(f'Grabbing ACSSE groups for {year} - {str(elapsed)}')

        # At this point there are a lot of duplicate group codes in the long dataframe
        # This is because an ACS1 group exists in multiple years
        # We just need to drop the duplicates
        groups_df = groups_df.drop_duplicates('gcode')[['gcode','description', 'source']]
        

    def load_fips_mapping(self):
        project_root = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(project_root, 'data')
        os.makedirs(data_dir, exist_ok=True)
        county_fipcodes = pd.read_csv(os.path.join(data_dir, 'county_fips_master.csv'), encoding='cp1252')

        county_names = [
            "Dane County WI",
            "Ramsey County MN",
            "Sangamon County IL",
            "Marion County IN",
            "Franklin County OH",
            "Polk County IA",
            "Lancaster County NE",
            "Boulder County CO",
            "Wake County NC",
            "Champaign County IL",
            "Brown County WI",
            "Milwaukee County WI",
            "Washtenaw County MI",
            "Clarke County GA",
            "Lane County OR",
            "Rockwall County TX"
        ]

        fips_mapping = county_fipcodes[county_fipcodes['long_name'].isin(county_names)][['fips', 'long_name']]

        fips_mapping['fips'] = fips_mapping['fips'].astype('str').apply(lambda x: x.zfill(5))
        fips_mapping.set_index('fips', inplace=True)
        fips_mapping['state_fips'] = fips_mapping.index.map(lambda x: x[:2])
        fips_mapping['county_fips'] = fips_mapping.index.map(lambda x: x[2:])

        fips_mapping.to_csv(os.path.join(data_dir, 'fips_mapping.csv'))


    def scrape_vars_acs1(self, codes):
        year_map = {}
        for code in codes:
            years = self.vars_df.loc[code, 'years']
            for year in years:
                if year not in year_map:
                    year_map[year] = []
            year_map[year].append(code)

        output_df = pd.DataFrame(columns = ['NAME', 'year'])
        output_df.set_index(['NAME', 'year'], inplace=True)

        for year in year_map.keys():
            start = time.time()
            data = []

            print('pulling data')
            for state_fip, county_fip in zip(self.fips_mapping['state_fips'], self.fips_mapping['county_fips']):
                data.append(self.c.acs1.state_county(['NAME'] + codes, state_fip, county_fip, year=year))

            flat_data = [entry[0] for entry in data if entry]
            year_df = pd.DataFrame(flat_data)
            year_df['year'] = year
            year_df.set_index(['NAME', 'year'], inplace=True)
            year_df.drop(columns=['state', 'county'], inplace=True)

            output_df = pd.concat([output_df, year_df], axis=0, sort=True)

            end = time.time()
            elapsed = timedelta(seconds=end - start)
            print(f'Year: {year} - {str(elapsed)}')

        return output_df
    
    def scrape_vars_acsse(self, codes):
        year_map = {}
        for code in codes:
            years = self.vars_df.loc[code, 'years']
            for year in years:
                if year not in year_map:
                    year_map[year] = []
            year_map[year].append(code)

        output_df = pd.DataFrame(columns = ['NAME', 'year'])
        output_df.set_index(['NAME', 'year'], inplace=True)

        for year in year_map.keys():
            start = time.time()
            data = []

            print('pulling data')
            for state_fip, county_fip in zip(self.fips_mapping['state_fips'], self.fips_mapping['county_fips']):
        
                base_url = f"https://api.census.gov/data/{year}/acs/acsse"
    
                # Prepare the variable list for the API call
                get_vars = ','.join(['NAME'] + codes)

                # Construct the request URL
                params = {
                    "get": get_vars,
                    "for": f"county:{county_fip}",
                    "in": f"state:{state_fip}",
                    "key": os.getenv('CENSUS_KEY')
                }
                print(base_url, params)

                response = requests.get(base_url, params=params)

                # if response.status_code == 200:
                #     resj = response.json()
                #     # Turn the first row into keys and zip them with the value rows
                #     headers = resj[0]
                #     values = resj[1:]
                #     data.append([dict(zip(headers, row)) for row in values]) 
                # else:
                #     raise Exception(f"Request failed: {response.status_code} - {response.text}")
                
                if response.status_code == 204:
                    print("Warning: No data returned from the Census API for the requested variables.")
                    continue
                
                resj = response.json()
                # Turn the first row into keys and zip them with the value rows
                headers = resj[0]
                values = resj[1:]
                data.append([dict(zip(headers, row)) for row in values]) 


            flat_data = [entry[0] for entry in data if entry]
            year_df = pd.DataFrame(flat_data)
            year_df['year'] = year
            year_df.set_index(['NAME', 'year'], inplace=True)
            year_df.drop(columns=['state', 'county'], inplace=True)

            output_df = pd.concat([output_df, year_df], axis=0, sort=True)

            end = time.time()
            elapsed = timedelta(seconds=end - start)
            print(f'Year: {year} - {str(elapsed)}')
        return output_df
    
    def scrape_vars(self,codes,source):
        if(source == 'ACS1'):
            return self.scrape_vars_acs1(codes)
        else:
            return self.scrape_vars_acsse(codes)
        
