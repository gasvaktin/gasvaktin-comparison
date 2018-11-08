#!/usr/bin/python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------- #
import argparse
import datetime
import logging

# import git

from database.models import Currency, CrudeOilBarrelUSD, ExchangeRateOfISK
import database.db
import endpoints


def setup_logger():
    logger = logging.getLogger('comparison')
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    log_format = '%(asctime)s.%(msecs)03d [%(levelname)s]: %(message)s'
    log_timestamp_format = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(log_format, log_timestamp_format)
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger


def fetch_crude_oil_rate_history(db, logger=None):
    today = datetime.datetime.now()
    today_str = today.strftime('%Y-%m-%d')
    start_date = None
    last_record = db.session.query(CrudeOilBarrelUSD).order_by(
        CrudeOilBarrelUSD.date.desc()
    ).first()
    if last_record is not None:
        yesterday = today - datetime.timedelta(days=1)
        if yesterday.strftime('%Y-%m-%d') == last_record.date:
            if logger is not None:
                logger.info('We already have ISK rate history up to yesterday date.')
                return  # no need to run scraper if we have data up to yesterday date
    if logger is not None:
        logger.info('Fetching crude oil rate history ..')
    if last_record is not None:
        start_date = datetime.datetime.strptime(last_record.date, '%Y-%m-%d')
    crude_data = endpoints.get_crude_oil_rate_history(date_a=start_date, logger=logger)
    commit_required = False
    for date_key in crude_data:
        if date_key == today_str:
            continue
        record = db.session.query(CrudeOilBarrelUSD).filter_by(date=date_key).first()
        if record is None:
            record = CrudeOilBarrelUSD(date=date_key, rate=crude_data[date_key])
            db.session.add(record)
            commit_required = True
    if commit_required:
        db.session.commit()
    if logger is not None:
        logger.info('Finished fetching crude oil rate history.')


def fetch_isk_rate_history(db, logger=None):
    today = datetime.datetime.now()
    last_record = db.session.query(ExchangeRateOfISK).order_by(
        ExchangeRateOfISK.date.desc()
    ).first()
    start_date = datetime.datetime(1981, 1, 1)
    if last_record is not None:
        yesterday = today - datetime.timedelta(days=1)
        if yesterday.strftime('%Y-%m-%d') == last_record.date:
            if logger is not None:
                logger.info('We already have ISK rate history up to yesterday date.')
                return  # no need to run scraper if we have data up to yesterday date
    if logger is not None:
        logger.info('Fetching ISK rate history ..')
    if last_record is not None:
        if logger is not None:
            logger.info(
                'We have ISK rate data up to "%s", querying data from that date to today ..' % (
                    last_record.date,
                )
            )
        start_date = datetime.datetime.strptime(last_record.date, '%Y-%m-%d')
    if last_record is not None:
        start_date = datetime.datetime.strptime(last_record.date, '%Y-%m-%d')
    i_date = start_date
    while i_date.strftime('%Y-%m-%d') < today.strftime('%Y-%m-%d'):
        isk_data = endpoints.get_isk_exchange_rate(i_date, logger=logger)
        assert(isk_data['date'] == i_date.strftime('%Y-%m-%d'))
        commit_required = False
        logger_messages = []
        for curr_key in isk_data['currencies']:
            curr = isk_data['currencies'][curr_key]
            currency = db.session.query(Currency).filter_by(code=curr['code']).first()
            if currency is None:
                currency = Currency(name=curr['name'], code=curr['code'])
                db.session.add(currency)
                db.session.commit()
            record = db.session.query(ExchangeRateOfISK).filter_by(
                fk_currency=currency.currency_id,
                date=isk_data['date']
            ).first()
            if record is None:
                record = ExchangeRateOfISK(
                    fk_currency=currency.currency_id,
                    date=isk_data['date'],
                    buy=curr['buy'],
                    sell=curr['sell'],
                    mean=curr['mean']
                )
                db.session.add(record)
                logger_messages.append('Data "%s" %s [%s, %s, %s] written to database.' % (
                    record.date,
                    currency.code,
                    record.buy,
                    record.sell,
                    record.mean
                ))
                commit_required = True
        if commit_required:
            db.session.commit()  # single commit for all currencies, better for disk drive
        if logger is not None:
            for message in logger_messages:
                logger.info(message)
        i_date += datetime.timedelta(days=1)
    if logger is not None:
        logger.info('Finished fetching ISK rate history.')


def write_crude_oil_rate_history_to_file(db, logger=None):
    if logger is not None:
        logger.info('Writing crude oil rate history data to files ..')
    crude_oil_records = db.session.query(CrudeOilBarrelUSD).order_by(CrudeOilBarrelUSD.date)
    # the plain crude oil data from the Federal Reserve Bank of St Louis
    filename1 = 'data/crude_oil_barrel_usd.csv.txt'
    with open(filename1, mode='w', encoding='utf-8') as crude_oil_file1:
        if logger is not None:
            logger.info('Writing to file "%s" ..' % (filename1, ))
        crude_oil_file1.write('date,price\n')
        for record in crude_oil_records:
            crude_oil_file1.write('%s,%s\n' % (record.date, record.rate))
    us_dollar = db.session.query(Currency).filter_by(code='USD').first()
    assert(us_dollar is not None)
    crude_oil_records = db.session.query(CrudeOilBarrelUSD).order_by(CrudeOilBarrelUSD.date)
    # the crude oil data converted to litres instead of barrels and ISK instead of USD (using
    # exchange rate for ISK from Central Bank of Iceland)
    bbl_to_litres = 158.987294928  # https://twitter.com/gasvaktin/status/993875638435090433
    filename2 = 'data/crude_oil_litres_isk.csv.txt'
    with open(filename2, mode='w', encoding='utf-8') as crude_oil_file2:
        if logger is not None:
            logger.info('Writing to file "%s" ..' % (filename2, ))
        crude_oil_file2.write('date,price\n')
        for crude_oil_record in crude_oil_records:
            isk_usd_rate = db.session.query(ExchangeRateOfISK).filter_by(
                fk_currency=us_dollar.currency_id
            ).filter(
                ExchangeRateOfISK.date <= crude_oil_record.date
            ).order_by(ExchangeRateOfISK.date.desc()).first()
            assert(isk_usd_rate is not None)
            price_per_liter_in_isk = (crude_oil_record.rate * isk_usd_rate.sell / bbl_to_litres)
            price_per_liter_in_isk = round(price_per_liter_in_isk, 2)
            crude_oil_file2.write('%s,%s\n' % (crude_oil_record.date, price_per_liter_in_isk))
    if logger is not None:
        logger.info('Finished writing crude oil rate history data to files.')


def write_isk_rate_history_to_files(db, logger=None):
    if logger is not None:
        logger.info('Writing isk rate history data to file ..')
    currencies = db.session.query(Currency)
    for currency in currencies:
        currency_code = currency.code.lower()
        records = db.session.query(ExchangeRateOfISK).filter_by(
            fk_currency=currency.currency_id
        ).order_by(ExchangeRateOfISK.date)
        # the plain exchange rate for ISK to specified currency from Central Bank of Iceland
        filename = 'data/currency_rate_isk_%s.csv.txt' % (currency_code, )
        with open(filename, mode='w', encoding='utf-8') as isk_file:
            if logger is not None:
                logger.info('Writing to file "%s" ..' % (filename, ))
            isk_file.write('date,buy,sell,mean\n')
            for record in records:
                isk_file.write('%s,%s,%s,%s\n' % (
                    record.date, record.buy, record.sell, record.mean
                ))
    if logger is not None:
        logger.info('Finished writing isk rate history data to file.')


def commit_to_git(db, logger=None):
    print('todo: write autocommit functionality')
    # TODO: implement
    # import pdb; pdb.set_trace()


def main(use_logger=True):
    logger = None
    if use_logger:
        logger = setup_logger()
    parser = argparse.ArgumentParser(description='Gasvaktin Comparison')
    parser.add_argument('-c', '--auto-commit', action='store_true', help=(
        'Auto commit to git repository.'
    ))
    parser.add_argument('-f', '--fetch-data', action='store_true', help=(
        'Fetch additional data if available and store in local database.'
    ))
    parser.add_argument('-w', '--write-data', action='store_true', help=(
        'Write collected data to plain CSV data files.'
    ))
    pargs = parser.parse_args()
    print(pargs)
    # database setup
    if logger is not None:
        logger.info('Initiating database ..')
    db_uri = 'sqlite:///database/database.sqlite'
    db_init = True
    database.db.setup_connection(db_uri)
    if db_init:
        database.db.init_db()
    if logger is not None:
        logger.info('.. database initialized.')
    if pargs.fetch_data:
        if logger is not None:
            logger.info('Running --fetch-data ..')
        fetch_crude_oil_rate_history(database.db, logger)
        fetch_isk_rate_history(database.db, logger)
    if pargs.write_data:
        if logger is not None:
            logger.info('Running --write-data ..')
        write_crude_oil_rate_history_to_file(database.db, logger)
        write_isk_rate_history_to_files(database.db, logger)
    if pargs.auto_commit:
        if logger is not None:
            logger.info('Running --auto-commit ..')
        commit_to_git(database.db, logger)
    if logger is not None:
        logger.info('Done.')


if __name__ == '__main__':
    main()
