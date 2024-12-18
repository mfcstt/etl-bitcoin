from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Float, String, Integer, DateTime
from datetime import datetime

Base = declarative_base()

class BitcoinPrice(Base):

    __tablename__ = 'bitcoin_historical_price'

    id = Column(Integer, primary_key=True, autoincrement=True)
    value = Column(Float, nullable=False)
    crypto = Column(String(50), nullable=False)
    coin = Column(String(10), nullable=False)
    timestamp = Column(DateTime)
