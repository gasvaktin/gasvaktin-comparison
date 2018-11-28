#!/usr/bin/python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------- #

from sqlalchemy import Column, Float, Integer, Unicode

from database.db import Base
from database.models import utility_columns


class CrudeOilBarrelUSD(Base):
    __tablename__ = 'crude_oil_barrel_usd'
    price_id = Column(Integer(), primary_key=True)
    date = Column(Unicode(10), unique=True, nullable=False, server_default='')
    rate = Column(Float(), nullable=False, server_default='0.0')
    edited = utility_columns.timestamp_edited()
    created = utility_columns.timestamp_created()


class CrudeOilBarrelUSDfb(Base):
    __tablename__ = 'crude_oil_barrel_usd_fallback'
    price_id = Column(Integer(), primary_key=True)
    date = Column(Unicode(10), unique=True, nullable=False, server_default='')
    rate = Column(Float(), nullable=False, server_default='0.0')
    edited = utility_columns.timestamp_edited()
    created = utility_columns.timestamp_created()


class DieselPriceIcelandLiterISK(Base):
    __tablename__ = 'diesel_price_iceland_liter_isk'
    price_id = Column(Integer(), primary_key=True)
    date = Column(Unicode(10), unique=True, nullable=False, server_default='')
    price = Column(Float(), nullable=False, server_default='0.0')
    edited = utility_columns.timestamp_edited()
    created = utility_columns.timestamp_created()


class PetrolPriceIcelandLiterISK(Base):
    __tablename__ = 'petrol_price_iceland_liter_isk'
    price_id = Column(Integer(), primary_key=True)
    date = Column(Unicode(10), unique=True, nullable=False, server_default='')
    price = Column(Float(), nullable=False, server_default='0.0')
    edited = utility_columns.timestamp_edited()
    created = utility_columns.timestamp_created()
