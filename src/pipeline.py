import requests
from datetime import datetime
import time

def extract():
    url = 'https://api.coinbase.com/v2/prices/spot'

    response = requests.get(url)
    data = response.json()

    return data

def transform(data):
    value = data['data']['amount']
    crypto = data['data']['base']
    coin = data['data']['currency']
    timestamp = datetime.now().timestamp()

    data_transformed = {
        'value': value,
        'crypto': crypto,
        'coin': coin,
        'timestamp': timestamp
    }

    return data_transformed

def load(data):
    pass

if __name__ == '__main__':
    while True:
        data = extract()
        data_transformed = transform(data)
        load(data_transformed)
        time.sleep(15)
