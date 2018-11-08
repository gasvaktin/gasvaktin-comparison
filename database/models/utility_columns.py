#!/usr/bin/python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------- #
import datetime

from sqlalchemy import Column, DateTime


def timestamp_created():
    return Column(DateTime, default=datetime.datetime.utcnow)


def timestamp_edited():
    return Column(
        DateTime,
        default=datetime.datetime.utcnow,
        onupdate=datetime.datetime.utcnow
    )
