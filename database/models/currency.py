#!/usr/bin/python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------- #

from sqlalchemy import Column, Float, ForeignKey, Integer, Unicode, UniqueConstraint

from database.db import Base
from database.models import utility_columns


class ExchangeRateOfISK(Base):
    __tablename__ = 'exchange_rate_of_isk'
    rate_id = Column(Integer(), primary_key=True)
    fk_currency = Column(Integer(), ForeignKey('currency.currency_id'))
    date = Column(Unicode(10), nullable=False, server_default='')
    buy = Column(Float(), nullable=False, server_default='0.0')
    sell = Column(Float(), nullable=False, server_default='0.0')
    mean = Column(Float(), nullable=False, server_default='0.0')
    edited = utility_columns.timestamp_edited()
    created = utility_columns.timestamp_created()
    __table_args__ = (
        UniqueConstraint('fk_currency', 'date', name='_currency_date_uc'),
    )

    def __repr__(self):
        return '<ExchangeRateOfISK [curr: %s] "%s" (%s)>' % (
            self.fk_currency,
            self.date,
            self.mean
        )


class Currency(Base):
    __tablename__ = 'currency'
    currency_id = Column(Integer(), primary_key=True)
    name = Column(Unicode(256), unique=True, nullable=False, server_default='')
    code = Column(Unicode(256), unique=True, nullable=False, server_default='')
    edited = utility_columns.timestamp_edited()
    created = utility_columns.timestamp_created()

    def __repr__(self):
        return '<Currency [%s] "%s" (%s)>' % (self.currency_id, self.name, self.code)
