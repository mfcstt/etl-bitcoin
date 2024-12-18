import os
import time
from datetime import datetime
from dotenv import load_dotenv

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base, BitcoinPrice

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# print(f"POSTGRES_USER={POSTGRES_USER}")
# print(f"POSTGRES_PASSWORD={POSTGRES_PASSWORD}")
# print(f"POSTGRES_HOST={POSTGRES_HOST}")
# print(f"POSTGRES_PORT={POSTGRES_PORT}")
# print(f"POSTGRES_DB={POSTGRES_DB}")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def create_table():
    Base.metadata.create_all(engine)
    print(' [*] Table created')

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

if __name__ == '__main__':
    create_table()
    print(' [*] Table created')

    while True:
        try:
            data = extract()
            if data:
                data_transformed = transform(data)
                print ('Data transformed:', data_transformed)
                load(data_transformed)
                time.sleep(15)
        except KeyboardInterrupt:
            print(' [*] Exiting...')
            break
        except Exception as e:
            print(f'Error: {e}')
            time.sleep(15)
