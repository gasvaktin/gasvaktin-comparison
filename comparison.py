#!/usr/bin/python3
# ----------------------------------------------------------------------------------------------- #
import argparse
import configparser
import csv
import datetime
import json
import logging
import operator
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


def fetch_isk_inflation_index_history_and_write_to_file(logger=None):
    if logger is None:
        logger = Logger
    isk_inflation_index_data = endpoints.get_isk_inflation_index_history(logger=logger)
    filename = 'data/currency_isk_inflation_index.csv.txt'
    with open(filename, mode='w', encoding='utf-8') as outfile:
        outfile.write('date,value\n')
        for month in isk_inflation_index_data['list']:
            outfile.write('%s,%s\n' % (month['date'], month['value']))
    if logger is not None:
        logger.info('Finished writing isk inflation index history data to files.')


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


def read_and_write_price_diff_data(config, logger=None, fromdate=None, todate=None):
    if logger is None:
        logger = Logger
    commit_ids_with_bad_data = [
        'a0783100209f0cf43b28271e3433c2c56c650447',
        '821ccc907fec62415cc6d57f93d953205fd4d331',
        'fbf5eb81cbca3ed2091280b6cb6746975d309616',
        'ab4f01ccbe5486adc1ee838a892344035d4cc86b',
        'f5439ce43ae49d5926a48a9be52888d56075c8f1',
        'b050ad1a673223f5a2fe2993997041614484855f',
        '5c031c459ada0b95347da63c9e33d83954a8e609',
        '62b37a61ce8e946655fda872674689a6b6337205',
        '860c1e206f15e39abd2ea41c6aeb322d28675d8e',
        '96a558c64333ad970cd0c05c5fa79cafdaee7c13',
        'd430fa2a0b7a34df4843f21a717e6d4a971aa7e6',
        '71a03f6fdc48fda2213ed2e37ff6ee66016bb853',
        '473da685077c2ffdec58028737b44d560d1215d5',
        'f77bc20e481067d15b6e4431ed0c8d8f7ffad470',
        'b1b0a413303de2aef7bf37b711576a116a4b45bd',
        '0bebbe40af1d3515700064b962a1e3fc218094a7',
    ]
    gasvaktin_repo_path = os.path.expanduser(config.get('Comparison', 'gasvaktin_git_directory'))
    git_ssh_identity_file = os.path.expanduser(config.get('Comparison', 'ssh_id_file'))
    assert(os.path.exists(git_ssh_identity_file) and os.path.isfile(git_ssh_identity_file))
    git_ssh_cmd = 'ssh -i %s' % (git_ssh_identity_file, )
    price_diff_data = []
    c_data = {  # companies price data
        'ao': [], 'co': [], 'dn': [], 'n1': [], 'ob': [], 'ol': [], 'or': [], 'ox': [], 'sk': []
    }
    c_data['co'].append({
        'timestamp_text': '2017-05-19T08:00',
        'stations_count': 1,
        'lowest_bensin': 169.9,
        'highest_bensin': 169.9,
        'average_bensin': 169.9,
        'common_bensin': 169.9,
        'lowest_diesel': 164.9,
        'highest_diesel': 164.9,
        'average_diesel': 164.9,
        'common_diesel': 164.9
    })
    c_data['co'].append({
        'timestamp_text': '2017-05-25T10:00',
        'stations_count': 1,
        'lowest_bensin': 169.9,
        'highest_bensin': 169.9,
        'average_bensin': 169.9,
        'common_bensin': 169.9,
        'lowest_diesel': 161.9,
        'highest_diesel': 161.9,
        'average_diesel': 161.9,
        'common_diesel': 161.9
    })
    if logger is not None:
        logger.info('Reading price diff data from gasvaktin git ..')
        logger.info('Reading price data for individual companies from gasvaktin git ..')
    with git.Git().custom_environment(GIT_SSH_COMMAND=git_ssh_cmd):
        repo = git.Repo(gasvaktin_repo_path)
        assert(repo.active_branch.name == 'master')
        repo.git.pull()
        assert(repo.is_dirty() is False)
        # read data from repo
        path = 'vaktin/gas.json'
        commits_generator = (
            (commit, (commit.tree / path).data_stream.read())
            for commit in repo.iter_commits(paths=path)
        )
        commits_list = []  # consuming generator to list because want to reverse it :(
        for commit, filecontents in commits_generator:
            if not commit.message.startswith('auto.prices.update'):
                # we only need to read from auto.prices.update commits
                # so we skip all the others
                continue
            if commit.message.startswith('auto.prices.update.min'):
                # skip the 'min' auto commits
                continue
            timestamp_text = commit.message[19:35]
            # skip bad price changes
            if commit.hexsha in commit_ids_with_bad_data:
                continue
            timestamp = datetime.datetime.strptime(timestamp_text, '%Y-%m-%dT%H:%M')
            if fromdate is not None and timestamp < fromdate:
                # ignore price changes before from-date if provided
                continue
            if todate is not None and todate < timestamp:
                # ignore price changes after to-date if provided
                continue
            stations = json.loads(filecontents.decode('utf-8'))
            commits_list.append((timestamp_text, stations))
        commits_list_reversed = reversed(commits_list)
        for timestamp_text, stations in commits_list_reversed:
            # fuel_price_iceland_min_max_diff
            lowest_bensin = None
            highest_bensin = None
            bensin_diff = None
            lowest_diesel = None
            highest_diesel = None
            diesel_diff = None
            for station in stations['stations']:
                bensin95 = station['bensin95']
                diesel = station['diesel']
                if lowest_bensin is None or lowest_bensin > bensin95:
                    lowest_bensin = bensin95
                if highest_bensin is None or highest_bensin < bensin95:
                    highest_bensin = bensin95
                if lowest_diesel is None or lowest_diesel > diesel:
                    lowest_diesel = diesel
                if highest_diesel is None or highest_diesel < diesel:
                    highest_diesel = diesel
            # first week of costco, not in git history unfortunately
            if '2017-05-19T08:00' < timestamp_text and timestamp_text < '2017-06-01T12:00':
                lowest_bensin = 169.9
                lowest_diesel = 164.9
                if '2017-05-25T10:00' < timestamp_text:
                    lowest_diesel = 161.9
            bensin_diff = round((highest_bensin - lowest_bensin), 1)
            diesel_diff = round((highest_diesel - lowest_diesel), 1)
            price_diff_dp = {
                'timestamp_text': timestamp_text,
                'lowest_bensin': lowest_bensin,
                'highest_bensin': highest_bensin,
                'bensin_diff': bensin_diff,
                'lowest_diesel': lowest_diesel,
                'highest_diesel': highest_diesel,
                'diesel_diff': diesel_diff,
            }
            if (  # skip datapoints which are identical to previous datapoint
                len(price_diff_data) == 0 or not (
                    price_diff_data[-1]['lowest_bensin'] == price_diff_dp['lowest_bensin'] and
                    price_diff_data[-1]['highest_bensin'] == price_diff_dp['highest_bensin'] and
                    price_diff_data[-1]['bensin_diff'] == price_diff_dp['bensin_diff'] and
                    price_diff_data[-1]['lowest_diesel'] == price_diff_dp['lowest_diesel'] and
                    price_diff_data[-1]['highest_diesel'] == price_diff_dp['highest_diesel'] and
                    price_diff_data[-1]['diesel_diff'] == price_diff_dp['diesel_diff']
                )
            ):
                price_diff_data.append(price_diff_dp)
            # pull together price data for individual oil companies
            for cid in c_data.keys():
                data_bensin_sum = 0
                data_bensin_prices = {}
                data_diesel_sum = 0
                data_diesel_prices = {}
                data = {
                    'timestamp_text': timestamp_text,
                    'stations_count': 0,
                    'lowest_bensin': None,
                    'highest_bensin': None,
                    'average_bensin': None,
                    'common_bensin': None,
                    'lowest_diesel': None,
                    'highest_diesel': None,
                    'average_diesel': None,
                    'common_diesel': None
                }
                for station in stations['stations']:
                    if station['key'].startswith('%s_' % (cid, )):
                        data['stations_count'] += 1
                        data_bensin_sum += station['bensin95']
                        if station['bensin95'] not in data_bensin_prices:
                            data_bensin_prices[station['bensin95']] = 1
                        else:
                            data_bensin_prices[station['bensin95']] += 1
                        if (
                            data['lowest_bensin'] is None or
                            data['lowest_bensin'] > station['bensin95']
                        ):
                            data['lowest_bensin'] = station['bensin95']
                        if (
                            data['highest_bensin'] is None or
                            data['highest_bensin'] < station['bensin95']
                        ):
                            data['highest_bensin'] = station['bensin95']
                        data_diesel_sum += station['diesel']
                        if station['diesel'] not in data_diesel_prices:
                            data_diesel_prices[station['diesel']] = 1
                        else:
                            data_diesel_prices[station['diesel']] += 1
                        if (
                            data['lowest_diesel'] is None or
                            data['lowest_diesel'] > station['diesel']
                        ):
                            data['lowest_diesel'] = station['diesel']
                        if (
                            data['highest_diesel'] is None or
                            data['highest_diesel'] < station['diesel']
                        ):
                            data['highest_diesel'] = station['diesel']
                if data['stations_count'] > 0:
                    data['average_bensin'] = round(
                        data_bensin_sum / data['stations_count'], 1
                    )
                    data['common_bensin'] = max(
                        data_bensin_prices.items(), key=operator.itemgetter(1)
                    )[0]
                    data['average_diesel'] = round(
                        data_diesel_sum / data['stations_count'], 1
                    )
                    data['common_diesel'] = max(
                        data_diesel_prices.items(), key=operator.itemgetter(1)
                    )[0]
                    if (
                        len(c_data[cid]) == 0 or not (
                            c_data[cid][-1]['stations_count'] == data['stations_count'] and
                            c_data[cid][-1]['lowest_bensin'] == data['lowest_bensin'] and
                            c_data[cid][-1]['highest_bensin'] == data['highest_bensin'] and
                            c_data[cid][-1]['average_bensin'] == data['average_bensin'] and
                            c_data[cid][-1]['common_bensin'] == data['common_bensin'] and
                            c_data[cid][-1]['lowest_diesel'] == data['lowest_diesel'] and
                            c_data[cid][-1]['highest_diesel'] == data['highest_diesel'] and
                            c_data[cid][-1]['average_diesel'] == data['average_diesel'] and
                            c_data[cid][-1]['common_diesel'] == data['common_diesel']
                        )
                    ):
                        c_data[cid].append(data)
    c_data['dn'].append({
        'timestamp_text': '2021-11-13T23:15',
        'stations_count': 0,
        'lowest_bensin': 'null',
        'highest_bensin': 'null',
        'average_bensin': 'null',
        'common_bensin': 'null',
        'lowest_diesel': 'null',
        'highest_diesel': 'null',
        'average_diesel': 'null',
        'common_diesel': 'null'
    })
    c_data['ox'].append({
        'timestamp_text': '2020-01-25T09:00',
        'stations_count': 0,
        'lowest_bensin': 'null',
        'highest_bensin': 'null',
        'average_bensin': 'null',
        'common_bensin': 'null',
        'lowest_diesel': 'null',
        'highest_diesel': 'null',
        'average_diesel': 'null',
        'common_diesel': 'null'
    })
    c_data['sk'].append({
        'timestamp_text': '2018-02-21T15:30',
        'stations_count': 0,
        'lowest_bensin': 'null',
        'highest_bensin': 'null',
        'average_bensin': 'null',
        'common_bensin': 'null',
        'lowest_diesel': 'null',
        'highest_diesel': 'null',
        'average_diesel': 'null',
        'common_diesel': 'null'
    })
    filename = 'data/fuel_price_iceland_min_max_diff.csv.txt'
    with open(filename, mode='w', encoding='utf-8') as crude_ratio_file:
        if logger is not None:
            logger.info('Writing to file "%s" ..' % (filename, ))
        crude_ratio_file.write((
            'timestamp,lowest_bensin,highest_bensin,bensin_diff,lowest_diesel,highest_diesel,'
            'diesel_diff\n'
        ))
        for price_diff_datapoint in price_diff_data:
            crude_ratio_file.write((
                '{timestamp_text},{lowest_bensin},{highest_bensin},{bensin_diff},{lowest_diesel},'
                '{highest_diesel},{diesel_diff}\n'
            ).format_map(price_diff_datapoint))
    cid_filename_map = {
        'ao': 'data/fuel_price_iceland_company_atlantsolia.csv.txt',
        'co': 'data/fuel_price_iceland_company_costco.csv.txt',
        'dn': 'data/fuel_price_iceland_company_daelan.csv.txt',
        'n1': 'data/fuel_price_iceland_company_n1.csv.txt',
        'ob': 'data/fuel_price_iceland_company_ob.csv.txt',
        'ol': 'data/fuel_price_iceland_company_olis.csv.txt',
        'or': 'data/fuel_price_iceland_company_orkan.csv.txt',
        'ox': 'data/fuel_price_iceland_company_orkanx.csv.txt',
        'sk': 'data/fuel_price_iceland_company_skeljungur.csv.txt'
    }
    for cid in c_data.keys():
        filename_c = cid_filename_map[cid]
        with open(filename_c, mode='w', encoding='utf-8') as file_c:
            if logger is not None:
                logger.info('Writing to file "%s" ..' % (filename_c, ))
            file_c.write((
                'timestamp,stations_count,lowest_bensin,highest_bensin,average_bensin,'
                'common_bensin,lowest_diesel,highest_diesel,average_diesel,common_diesel\n'
            ))
            for price_datapoint in c_data[cid]:
                file_c.write((
                    '{timestamp_text},{stations_count},{lowest_bensin},{highest_bensin},'
                    '{average_bensin},{common_bensin},{lowest_diesel},{highest_diesel},'
                    '{average_diesel},{common_diesel}\n'
                ).format_map(price_datapoint))


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
        'data/fuel_price_iceland_min_max_diff.csv.txt',
        'data/crude_ratio.csv.txt',
        'data/currency_isk_inflation_index.csv.txt',
        'data/fuel_price_iceland_company_atlantsolia.csv.txt',
        'data/fuel_price_iceland_company_costco.csv.txt',
        'data/fuel_price_iceland_company_daelan.csv.txt',
        'data/fuel_price_iceland_company_n1.csv.txt',
        'data/fuel_price_iceland_company_ob.csv.txt',
        'data/fuel_price_iceland_company_olis.csv.txt',
        'data/fuel_price_iceland_company_orkan.csv.txt',
        'data/fuel_price_iceland_company_orkanx.csv.txt',
        'data/fuel_price_iceland_company_skeljungur.csv.txt'
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
            if logger is not None:
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
        fetch_isk_inflation_index_history_and_write_to_file()
    if pargs.write_data:
        if Logger is not None:
            Logger.info('Running --write-data ..')
        write_crude_oil_rate_history_to_file(database.db)
        write_isk_rate_history_to_files(database.db)
        write_icelandic_fuel_price_history_to_files(database.db)
        write_crude_ratio()
        read_and_write_price_diff_data(config, Logger)
    if pargs.auto_commit:
        if Logger is not None:
            Logger.info('Running --auto-commit ..')
        commit_changes_to_git(database.db, config)
    if Logger is not None:
        Logger.info('Done.')


if __name__ == '__main__':
    main()
