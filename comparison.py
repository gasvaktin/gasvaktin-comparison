#!/usr/bin/python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------- #
import argparse
import configparser
import csv
import datetime
import logging
import os

import git

from database.models import Currency, CrudeOilBarrelUSD, CrudeOilBarrelUSDfb, ExchangeRateOfISK
from database.models import DieselPriceIcelandLiterISK, PetrolPriceIcelandLiterISK
import database.db
import endpoints

Logger = None


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
    if logger is None:
        logger = Logger
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
    # fetch fallback crude oil rate data
    fallback_crude_data = endpoints.get_crude_oil_rate_history_fallback(logger)
    for date_key in fallback_crude_data:
        if date_key == today_str:
            continue
        record = db.session.query(CrudeOilBarrelUSDfb).filter_by(date=date_key).first()
        if record is None:
            record = CrudeOilBarrelUSDfb(date=date_key, rate=fallback_crude_data[date_key])
            db.session.add(record)
            commit_required = True
    if commit_required:
        db.session.commit()
    if logger is not None:
        logger.info('Finished fetching crude oil rate history.')


def fetch_isk_rate_history(db, logger=None):
    if logger is None:
        logger = Logger
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


def fetch_icelandic_fuel_price_history(db, logger=None):
    if logger is None:
        logger = Logger
    start_date = None
    last_petrol_record = db.session.query(PetrolPriceIcelandLiterISK).order_by(
        PetrolPriceIcelandLiterISK.date.desc()
    ).first()
    last_diesel_record = db.session.query(DieselPriceIcelandLiterISK).order_by(
        DieselPriceIcelandLiterISK.date.desc()
    ).first()
    if last_petrol_record is not None:
        start_date = datetime.datetime.strptime(last_petrol_record.date, '%Y-%m-%d')
    if last_diesel_record is not None:
        if (start_date is None or
                start_date > datetime.datetime.strptime(last_petrol_record.date, '%Y-%m-%d')):
            start_date = datetime.datetime.strptime(last_petrol_record.date, '%Y-%m-%d')
    fuel_price_data = endpoints.get_icelandic_fuel_price_history(start_date, logger)
    commit_required = False
    logger_messages = []
    for date_key in fuel_price_data['petrol']:
        record = db.session.query(PetrolPriceIcelandLiterISK).filter_by(date=date_key).first()
        if record is None:
            record = PetrolPriceIcelandLiterISK(
                date=date_key,
                price=fuel_price_data['petrol'][date_key]
            )
            db.session.add(record)
            logger_messages.append('Petrol data "%s" %s written to database.' % (
                date_key,
                fuel_price_data['petrol'][date_key]
            ))
            commit_required = True
    for date_key in fuel_price_data['diesel']:
        record = db.session.query(DieselPriceIcelandLiterISK).filter_by(date=date_key).first()
        if record is None:
            record = DieselPriceIcelandLiterISK(
                date=date_key,
                price=fuel_price_data['diesel'][date_key]
            )
            db.session.add(record)
            logger_messages.append('Petrol data "%s" %s written to database.' % (
                date_key,
                fuel_price_data['diesel'][date_key]
            ))
            commit_required = True
    if commit_required:
        db.session.commit()  # single commit for all currencies, better for disk drive
    if logger is not None:
        for message in logger_messages:
            logger.info(message)


def write_crude_oil_rate_history_to_file(db, logger=None):
    if logger is None:
        logger = Logger
    if logger is not None:
        logger.info('Writing crude oil rate history data to files ..')
    crude_oil_records = db.session.query(CrudeOilBarrelUSD).order_by(CrudeOilBarrelUSD.date)
    # the plain crude oil data from the Federal Reserve Bank of St Louis
    filename1 = 'data/crude_oil_barrel_usd.csv.txt'
    with open(filename1, mode='w', encoding='utf-8') as crude_oil_file1:
        if logger is not None:
            logger.info('Writing to file "%s" ..' % (filename1, ))
        crude_oil_file1.write('date,price\n')
        final_date = None
        for record in crude_oil_records:
            final_date = record.date
            crude_oil_file1.write('%s,%s\n' % (record.date, record.rate))
        # add filler data
        if logger is not None:
            logger.info('Adding crude oil filler data .. ')
        assert(final_date is not None)
        crude_oil_fallback_records = db.session.query(
            CrudeOilBarrelUSDfb
        ).filter(final_date < CrudeOilBarrelUSDfb.date).order_by(CrudeOilBarrelUSDfb.date)
        for fallback_record in crude_oil_fallback_records:
            crude_oil_file1.write('%s,%s\n' % (fallback_record.date, fallback_record.rate))
    us_dollar = db.session.query(Currency).filter_by(code='USD').first()
    assert(us_dollar is not None)
    crude_oil_records = db.session.query(CrudeOilBarrelUSD).order_by(CrudeOilBarrelUSD.date)
    # the crude oil data converted to litres instead of barrels and ISK instead of USD (using
    # exchange rate for ISK from Central Bank of Iceland)
    bbl_to_litres = 158.987294928  # https://twitter.com/gasvaktin/status/993875638435090433
    filename2 = 'data/crude_oil_litres_isk.csv.txt'
    if logger is not None:
        logger.info('Calculating crude oil price to ISK and writing to data .. ')
    with open(filename2, mode='w', encoding='utf-8') as crude_oil_file2:
        if logger is not None:
            logger.info('Writing to file "%s" ..' % (filename2, ))
        crude_oil_file2.write('date,price\n')
        final_date = None
        for crude_oil_record in crude_oil_records:
            final_date = crude_oil_record.date
            isk_usd_rate = db.session.query(ExchangeRateOfISK).filter_by(
                fk_currency=us_dollar.currency_id
            ).filter(
                ExchangeRateOfISK.date <= crude_oil_record.date
            ).order_by(ExchangeRateOfISK.date.desc()).first()
            assert(isk_usd_rate is not None)
            if isk_usd_rate.sell != 0.0:
                price_per_liter_in_isk = (
                    crude_oil_record.rate * isk_usd_rate.sell / bbl_to_litres
                )
            else:
                price_per_liter_in_isk = (
                    crude_oil_record.rate * isk_usd_rate.mean / bbl_to_litres
                )
            price_per_liter_in_isk = round(price_per_liter_in_isk, 2)
            crude_oil_file2.write('%s,%s\n' % (crude_oil_record.date, price_per_liter_in_isk))
        # add filler data
        if logger is not None:
            logger.info('Adding crude oil filler data .. ')
        assert(final_date is not None)
        crude_oil_fallback_records = db.session.query(
            CrudeOilBarrelUSDfb
        ).filter(final_date < CrudeOilBarrelUSDfb.date).order_by(CrudeOilBarrelUSDfb.date)
        for crude_oil_fallback_record in crude_oil_fallback_records:
            isk_usd_rate = db.session.query(ExchangeRateOfISK).filter_by(
                fk_currency=us_dollar.currency_id
            ).filter(
                ExchangeRateOfISK.date <= crude_oil_fallback_record.date
            ).order_by(ExchangeRateOfISK.date.desc()).first()
            assert(isk_usd_rate is not None)
            if isk_usd_rate.sell != 0.0:
                price_per_liter_in_isk = (
                    crude_oil_fallback_record.rate * isk_usd_rate.sell / bbl_to_litres
                )
            else:
                price_per_liter_in_isk = (
                    crude_oil_fallback_record.rate * isk_usd_rate.mean / bbl_to_litres
                )
            price_per_liter_in_isk = round(price_per_liter_in_isk, 2)
            crude_oil_file2.write(
                '%s,%s\n' % (crude_oil_fallback_record.date, price_per_liter_in_isk)
            )
    if logger is not None:
        logger.info('Finished writing crude oil rate history data to files.')


def write_isk_rate_history_to_files(db, logger=None):
    if logger is None:
        logger = Logger
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
                    record.date,
                    record.buy if record.buy != 0.0 else '',
                    record.sell if record.sell != 0.0 else '',
                    record.mean if record.mean != 0.0 else ''
                ))
    if logger is not None:
        logger.info('Finished writing isk rate history data to file.')


def write_icelandic_fuel_price_history_to_files(db, logger=None):
    if logger is None:
        logger = Logger
    if logger is not None:
        logger.info('Writing crude oil rate history data to files ..')
    filename1 = 'data/fuel_petrol_iceland_liter_isk.csv.txt'
    filename2 = 'data/fuel_diesel_iceland_liter_isk.csv.txt'
    # petrol
    petrol_records = db.session.query(PetrolPriceIcelandLiterISK).order_by(
        PetrolPriceIcelandLiterISK.date
    )
    with open(filename1, mode='w', encoding='utf-8') as petrol_file:
        if logger is not None:
            logger.info('Writing to file "%s" ..' % (filename1, ))
        petrol_file.write('date,price\n')
        for record in petrol_records:
            petrol_file.write('%s,%s\n' % (record.date, record.price))
    # diesel
    diesel_records = db.session.query(DieselPriceIcelandLiterISK).order_by(
        DieselPriceIcelandLiterISK.date
    )
    # the plain crude oil data from the Federal Reserve Bank of St Louis
    with open(filename2, mode='w', encoding='utf-8') as diesel_file:
        if logger is not None:
            logger.info('Writing to file "%s" ..' % (filename2, ))
        diesel_file.write('date,price\n')
        for record in diesel_records:
            diesel_file.write('%s,%s\n' % (record.date, record.price))
    if logger is not None:
        logger.info('Finished writing isk rate history data to file.')


def write_crude_ratio(logger=None):
    if logger is None:
        logger = Logger
    if logger is not None:
        logger.info('Writing crude petrol price isk ratio data to files ..')
    # get crude oil prices in ISK
    crude_isk_data = []
    crude_isk_filename = 'data/crude_oil_litres_isk.csv.txt'
    with open(crude_isk_filename, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            crude_isk_data.append(row)
    # get petrol prices in ISK
    petrol_isk_data = []
    petrol_isk_filename = 'data/fuel_petrol_iceland_liter_isk.csv.txt'
    with open(petrol_isk_filename, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            petrol_isk_data.append(row)
    # calculate crude petrol price isk ratio
    crude_ratio_data = []
    count_i = 0
    count_i_end = len(crude_isk_data)
    count_j = 0
    count_j_end = len(petrol_isk_data)
    # note: here be accuracy issue, data in is 2 decimal, but data out is 4 decimal
    # but the thing is, the added noise of 4 decimal places looks better in a plotted graph, and
    # the accuracy error is minimal enough to not cause visible falsities on a plotted graph
    # the correct way to go to fix this accuracy issue would be to recalculate neccesary data in,
    # instead of using already calculated data that was only stored with 2 decimal places
    cr_accuracy = 10000  # 4 decimal places
    while (count_i < count_i_end and count_j < count_j_end):
        crude_isk_record = crude_isk_data[count_i]
        petrol_isk_record = petrol_isk_data[count_j]
        # find crude record for current petrol record
        if crude_isk_record['date'] < petrol_isk_record['date']:
            if (count_i + 1) < count_i_end:
                if crude_isk_data[count_i + 1]['date'] <= petrol_isk_record['date']:
                    count_i += 1
                    continue
        # calculate crude ratio for current crude record and petrol record
        crude_ratio = float(crude_isk_record['price']) / float(petrol_isk_record['price'])
        crude_ratio_rounded = round(crude_ratio * cr_accuracy) / float(cr_accuracy)
        crude_ratio_date = petrol_isk_record['date']
        if crude_isk_record['date'] > petrol_isk_record['date']:
            crude_ratio_date = crude_isk_record['date']
        crude_ratio_data.append({
            'date': crude_ratio_date,
            'ratio': crude_ratio_rounded
        })
        # move to next crude or petrol record
        if (count_i + 1) >= count_i_end:
            count_j += 1
        elif (count_j + 1) >= count_j_end:
            count_i += 1
        elif crude_isk_data[count_i + 1]['date'] < petrol_isk_data[count_j + 1]['date']:
            count_i += 1
        else:
            count_j += 1
    # write data to file
    filename = 'data/crude_ratio.csv.txt'
    with open(filename, mode='w', encoding='utf-8') as crude_ratio_file:
        if logger is not None:
            logger.info('Writing to file "%s" ..' % (filename, ))
        crude_ratio_file.write('date,ratio\n')
        for record in crude_ratio_data:
            crude_ratio_file.write('%s,%.4f\n' % (record['date'], record['ratio']))
    if logger is not None:
        logger.info('Finished Writing crude petrol price isk ratio data to files.')


def calculate_comparison_data(mydate=None, logger=None):
    if logger is None:
        logger = Logger
    # WIP ..
    if mydate is None:
        mydate = datetime.datetime.now()
    # read crude oil price (isk/liter) for @mydate
    # find out when price was the same as on @mydate more than 3 months before @mydate
    # read crude oil price (usd/bbl) for those two time periods
    # read isk-usd rate for those two time periods
    # read icelandic petrol price for those two time periods
    # read icelandic diesel price for those two time periods
    # gather data together and return it
    comparison_data = {
        'c_date': mydate.strfdate('%Y-%m-%d'),
        'before_date': None,
        'crude_oil_isk_liter': {
            'c_date': None,
            'before_date_1': None,
            'before_date_2': None
        },
        'crude_oil_bbl_barrel': {
            'c_date': None,
            'before_date_1': None,
            'before_date_2': None
        },
        'rate_isk_usd': {
            'c_date': None,
            'before_date_1': None,
            'before_date_2': None
        },
        'price_petrol_iceland': {
            'c_date': None,
            'before_date_1': None,
            'before_date_2': None
        },
        'price_diesel_iceland': {
            'c_date': None,
            'before_date_1': None,
            'before_date_2': None
        },
        'prediction': {
            'petrol': 0.0,
            'diesel': 0.0
        }
    }
    # later use this to construct csv table with maybe over 20 columns
    #
    # comparison_prediction_filename = 'data/comparison_prediction.csv.txt'
    return comparison_data


def commit_changes_to_git(db, config, logger=None):
    if logger is None:
        logger = Logger
    timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M')
    watchlist = [
        'data/crude_oil_barrel_usd.csv.txt',
        'data/crude_oil_litres_isk.csv.txt',
        'data/currency_rate_isk_aud.csv.txt',
        'data/currency_rate_isk_bgn.csv.txt',
        'data/currency_rate_isk_brl.csv.txt',
        'data/currency_rate_isk_cad.csv.txt',
        'data/currency_rate_isk_chf.csv.txt',
        'data/currency_rate_isk_cny.csv.txt',
        'data/currency_rate_isk_czk.csv.txt',
        'data/currency_rate_isk_dkk.csv.txt',
        'data/currency_rate_isk_eur.csv.txt',
        'data/currency_rate_isk_gbp.csv.txt',
        'data/currency_rate_isk_hkd.csv.txt',
        'data/currency_rate_isk_hrk.csv.txt',
        'data/currency_rate_isk_huf.csv.txt',
        'data/currency_rate_isk_ils.csv.txt',
        'data/currency_rate_isk_inr.csv.txt',
        'data/currency_rate_isk_jmd.csv.txt',
        'data/currency_rate_isk_jpy.csv.txt',
        'data/currency_rate_isk_krw.csv.txt',
        'data/currency_rate_isk_mxn.csv.txt',
        'data/currency_rate_isk_ngn.csv.txt',
        'data/currency_rate_isk_nok.csv.txt',
        'data/currency_rate_isk_nzd.csv.txt',
        'data/currency_rate_isk_pln.csv.txt',
        'data/currency_rate_isk_rub.csv.txt',
        'data/currency_rate_isk_sar.csv.txt',
        'data/currency_rate_isk_sek.csv.txt',
        'data/currency_rate_isk_sgd.csv.txt',
        'data/currency_rate_isk_srd.csv.txt',
        'data/currency_rate_isk_thb.csv.txt',
        'data/currency_rate_isk_try.csv.txt',
        'data/currency_rate_isk_twd.csv.txt',
        'data/currency_rate_isk_usd.csv.txt',
        'data/currency_rate_isk_xdr.csv.txt',
        'data/currency_rate_isk_zar.csv.txt',
        'data/fuel_diesel_iceland_liter_isk.csv.txt',
        'data/fuel_petrol_iceland_liter_isk.csv.txt',
        'data/crude_ratio.csv.txt',
    ]
    git_ssh_identity_file = os.path.expanduser(config.get('Comparison', 'ssh_id_file'))
    assert(os.path.exists(git_ssh_identity_file) and os.path.isfile(git_ssh_identity_file))
    git_ssh_cmd = 'ssh -i %s' % (git_ssh_identity_file, )
    with git.Git().custom_environment(GIT_SSH_COMMAND=git_ssh_cmd):
        repo = git.Repo(os.path.expanduser(config.get('Comparison', 'git_directory')))
        assert(repo.active_branch.name == 'master')
        repo.git.pull()
        if repo.is_dirty():
            if logger is not None:
                logger.info('Repository is dirty ..')
            commit_required = False
            for item in repo.index.diff(None):
                if item.a_path in watchlist:
                    if logger is not None:
                        logger.info('Adding "%s" ..' % (item.a_path, ))
                    repo.git.add(item.a_path)
                    commit_required = True
            if commit_required:
                commit_msg = 'auto.data.update.%s' % (timestamp, )
                repo.git.commit(m=commit_msg)
                if logger is not None:
                    logger.info('Pushing changes (%s) ..' % (commit_msg, ))
                repo.git.push()
                if logger is not None:
                    logger.info('Pushed changes.')
            else:
                if logger is not None:
                    logger.warning('Repository is dirty but no changes to files in watchlist.')
        else:
            logger.info('Repository is clean.')


def main(init_logger=True):
    global Logger
    if init_logger:
        Logger = setup_logger()
    default_config_file = os.path.join(os.getcwd(), 'config.cfg')
    assert(os.path.exists(default_config_file) and os.path.isfile(default_config_file))
    parser = argparse.ArgumentParser(description='Gasvaktin Comparison')
    parser.add_argument('-a', '--auto-commit', action='store_true', help=(
        'Auto commit to git repository.'
    ))
    parser.add_argument('-c', '--config', default=default_config_file, help=(
        'Provide path to configuration file to use (default: "./config.cfg").'
    ))
    parser.add_argument('-f', '--fetch-data', action='store_true', help=(
        'Fetch additional data if available and store in local database.'
    ))
    parser.add_argument('-w', '--write-data', action='store_true', help=(
        'Write collected data to plain CSV data files.'
    ))
    pargs = parser.parse_args()
    config_pwd = os.path.expanduser(pargs.config)
    # read config file
    config = configparser.RawConfigParser()
    if os.path.exists(config_pwd) and os.path.isfile(config_pwd):
        config.read(config_pwd)
        if Logger is not None:
            if config_pwd == default_config_file:
                Logger.info('Using default config file ..')
            else:
                Logger.info('Using config file "%s" ..' % (pargs.config, ))
    else:
        assert(config_pwd != default_config_file)
        if Logger is not None:
            Logger.warning('Config file "%s" not found, using default config ..' % (
                pargs.config,
            ))
        config.read(default_config_file)
    if Logger is not None:
        Logger.info('Initiating database ..')
    db_uri = 'sqlite:///database/database.sqlite'
    db_init = True
    database.db.setup_connection(db_uri)
    if db_init:
        database.db.init_db()
    if Logger is not None:
        Logger.info('.. database initialized.')
    if pargs.fetch_data:
        if Logger is not None:
            Logger.info('Running --fetch-data ..')
        fetch_crude_oil_rate_history(database.db)
        fetch_isk_rate_history(database.db)
        fetch_icelandic_fuel_price_history(database.db)
    if pargs.write_data:
        if Logger is not None:
            Logger.info('Running --write-data ..')
        write_crude_oil_rate_history_to_file(database.db)
        write_isk_rate_history_to_files(database.db)
        write_icelandic_fuel_price_history_to_files(database.db)
        write_crude_ratio()
    if pargs.auto_commit:
        if Logger is not None:
            Logger.info('Running --auto-commit ..')
        commit_changes_to_git(database.db, config)
    if Logger is not None:
        Logger.info('Done.')


if __name__ == '__main__':
    main()
