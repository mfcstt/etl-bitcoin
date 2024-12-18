import os
import time
from datetime import datetime
from dotenv import load_dotenv

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base, BitcoinPrice

import logging
import logfire
from logging import basicConfig, getLogger

logfire.configure()
basicConfig(handlers=[logfire.LogfireLoggingHandler()])
logger = getLogger(__name__)
logger.setLevel(logging.INFO)
logfire.instrument_requests()
logfire.instrument_sqlalchemy()


load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")


DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def create_table():
    Base.metadata.create_all(engine)

create_table()


def extract():
    url = 'https://api.coinbase.com/v2/prices/spot'

    response = requests.get(url)
    data = response.json()

    return data

def transform(data):
    value = data['data']['amount']
    crypto = data['data']['base']
    coin = data['data']['currency']
    timestamp = datetime.now()

    data_transformed = {
        'value': value,
        'crypto': crypto,
        'coin': coin,
        'timestamp': timestamp
    }

    return data_transformed


def load(data):
    session = Session()
    new_bitcoin_price = BitcoinPrice(**data)
    session.add(new_bitcoin_price)
    session.commit()
    session.close()
    print(f'[{data["timestamp"]}] New record inserted')

def logfire_bitcoin():

    with logfire.span('Starting pipeline'):

        with logfire.span('Extracting data from API CoinBase'):
            data = extract()

        if not data:
            logger.error('No data extracted. Aborting pipeline')
            return
        
        with logfire.span('Transforming data'):
            data_transformed = transform(data)

        with logfire.span('Loading data into database'):
            load(data_transformed)

        logger.info('Pipeline finished successfully')

if __name__ == '__main__':
    create_table()
    logger.info('Starting pipeline')

    while True:
        try:
                logfire_bitcoin()
                time.sleep(15)
        except KeyboardInterrupt:
            logger.info('Pipeline stopped by user')
            break
        except Exception as e:
            logger.error(f'An error occurred: {e}')
            time.sleep(15)
