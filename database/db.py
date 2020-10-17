#!/usr/bin/python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------- #

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# SQLAlchemy - Declarative method
# https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/basic_use.html

engine = None
session = None
Base = declarative_base()
Base.query = None


def setup_connection(db_uri, db_echo=False):
    global engine, session, Base
    engine = create_engine(db_uri, convert_unicode=True, echo=db_echo)
    session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    Base.query = session.query_property()


def init_db():
    global Base
    # Import all modules here that define models so that they are registered on the metadata.
    # Or import them first before calling init_db()
    #
    #                            / silencing flake8 "imported but unused" for models
    from database import models  # noqa
    Base.metadata.create_all(bind=engine)
