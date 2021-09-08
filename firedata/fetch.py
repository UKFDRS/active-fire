import requests
import pandas as pd
from io import BytesIO
from dotenv import dotenv_values
#from .archive import

# Place your LANCE NRT token as variable in .env file
# in project root. The below uses dotenv to read
# the token into config dict.
config = dotenv_values('../.env')

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

#response = requests.get('https://nrt4.modaps.eosdis.nasa.gov/archive/FIRMS/modis-c6.1/Global')
#print(response.headers.get('Content-Type',''))
link = 'https://nrt4.modaps.eosdis.nasa.gov/api/v2/content/archives/FIRMS/modis-c6.1/Global/MODIS_C6_1_Global_MCD14DL_NRT_2021191.txt'
#link = 'https://nrt4.modaps.eosdis.nasa.gov/api/v2/content/archives/FIRMS/modis-c6.1/Global'
#link = 'https://nrt4.modaps.eosdis.nasa.gov/api/v2/content/details/FIRMS/modis-c6.1/Global?fields=all&format=json'
response = requests.get(link, auth = BearerAuth(config['NRT_TOKEN']))
data = BytesIO(response.content)
dfr = pd.read_table(data, sep = ',', header = 0)

