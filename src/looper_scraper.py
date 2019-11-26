import json
import re
import time

from bs4 import BeautifulSoup
import pandas as pd
import requests

from listings_consumer import get_shard_iterator


with open('schema_registry.json', 'r') as f:
    schema = json.load(f)
    schema = schema['real_estate']
    f.close()


def c(x): 
    if x:
        return x.text.replace('\r', '').replace('\t', '').replace('\n', ' ').strip()
    return ''

def parse_pages(pages):
    "Return parsed pages"
    results = []

    for page in range(pages):
        print(f'Getting page {page+1} from looperghana')
        results.append(requests.get(f"https://listings.loopghana.com/pageNumber_{page}").text)

    soups = []
    for r in results:
        soup = BeautifulSoup(r, 'lxml')
        soups.append(soup)
    
    return soups    


def extract_listings(soups):
    "Extract listings from list ofbeautiful soup objects"
    
    data = []

    for soup in soups:
        listings = soup.find_all('ul', {'class': 'listings-list'})[1].find_all('li')

        for prop in listings:
            d = {
                'id': prop.find('a').attrs['data-id'],
                'address': c(prop.find('span', {'class': 'name'})),
                'price': c(prop.find('span', {'class': 'price'})),
                'area': c(prop.find('span', {'class': 'size'})), 
                'beds': c(prop.find('span', {'class': 'bedrooms'})),
                'showers': c(prop.find('span', {'class': 'bathrooms'})),
                'url': prop.find('a').attrs['href'],

            }
            data.append(d)

    return data


def clean(df):
    df['currency'] = df.price.apply(lambda x: re.match(r'[A-Z\$]+', x.replace(',', '')).group())
    df['price'] = df.price.apply(lambda x: re.findall(r'[0-9]+', x.replace(',', ''))[0])
    df['area'] = df['area'].str.replace('m2', '')
    df['beds'] = df['beds'].str.replace('Bed', '')
    df['showers'] = df['showers'].str.replace('Bath', '')
    df['source'] = 'loopghana'
    
    return df

def get_extras(url):
    
    try:
        results = requests.get(url)
        soup = BeautifulSoup(results.text, 'lxml')
#         description = soup.find_all('div', {'class': 'detail-tab-content current', 'id': ''}).find('p').text
        js = soup.find_all('script', {'type': "text/javascript"})[0]

        coords = re.findall(r"(ws_l[a-z]+ = '-?[0-9].[0-9]+')", js.text)

        if coords:
            coords = dict(map(lambda x: x.split(' = '), coords))
            lat = coords.get('ws_lat', None)
            lon = coords.get('ws_lon', None)

        return lat, lon, None
    except:
        return None, None, None


def enrich(df):
    df['lat'] = None
    df['lon'] = None
    df[['lat', 'lon', 'description']] = df.url.apply(lambda x: get_extras(x)).apply(pd.Series)

    df['lat'] = df['lat'].apply(lambda x: x[1:-1] if x else None).astype(float)
    df['lon'] = df['lon'].apply(lambda x: x[1:-1] if x else None).astype(float)
    df['description'] = df['description'].apply(lambda x: x[1:-1] if x else None).astype(float)
    
    # TODO: no description for looper??
#     df['description'] = pd.np.NaN
        
    return df

def write(df):
    "Write data out to csv"

    df.to_csv(f'looper_{pd.datetime.now()}.csv', index=False)
    print('Data written!')


def scrape_looper(pages=4, add_gps=False):
    soups = parse_pages(pages)
    data = extract_listings(soups)
    df = pd.DataFrame(data)
    df = clean(df)
    
    if add_gps:
        df = enrich(df)
    
    df = df[schema]

    return df


if __name__ == '__main__':
    df = scrape_looper()
    write(df)