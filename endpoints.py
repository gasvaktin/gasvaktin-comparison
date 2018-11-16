#!/usr/bin/python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------- #
import csv
import datetime
import io
import time

import lxml.etree
import requests

USER_AGENT = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:63.0) Gecko/20100101 Firefox/63.0'


def get_isk_exchange_rate(req_date, logger=None):
    '''
    Extracts public exchange rate for ISK from Central Bank of Iceland for a given date.

    Central Bank of Iceland currently provides exchange rate from ISK to the following currencies:
    USD, GBP, CAD, DKK, NOK, SEK, CHF, JPY, XDR, EUR (EUR since 1999-01-05)

    Usage:  res_data = get_central_bank_iceland_isk_exchange_rate(req_date)
    Before: @req_date is a datetime.datetime object containing date in the range 1981-01-01 to our
            present date.
    After:  @res_data is a dict containing exchange rate info for given @req_date if available.

    Note: Central Bank of Iceland does not log exchange rate on weekdays or on specific icelandic
          holidays. If @req_date is a weekend we return nothing but that message and don't even
          query sedlabanki.is, if @req_date is revealed to be an icelandic holiday after querying
          sedlabanki.is we also return nothing but that message. Future @req_date dates are not
          possible for obvious reasons.
    '''
    beginning = datetime.datetime(1981, 1, 1)
    assert(beginning <= req_date)
    today = datetime.datetime.now()
    assert(req_date <= today)
    date_str = req_date.strftime('%Y-%m-%d')
    data = {
        'date': date_str,
        'currencies': {},
        'status': {
            'success': True,
            'msg': ''
        }
    }
    if req_date.weekday() in (5, 6):
        data['status']['success'] = False
        data['status']['msg'] = 'No currency rates on weekdays, "%s" %s.' % (
            date_str,
            'is Saturday' if (req_date.weekday() == 5) else 'is Sunday'
        )
        if logger is not None:
            logger.info(data['status']['msg'])
        return data
    session = requests.Session()
    url = 'https://www.sedlabanki.is/hagtolur/opinber-gengisskraning/'
    res1 = session.get(url, headers={'User-Agent': USER_AGENT})
    res1.raise_for_status()
    headers_for_post = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Host': 'www.sedlabanki.is',
        'Referer': url,
        'User-Agent': USER_AGENT
    }
    html1 = lxml.etree.fromstring(res1.content, lxml.etree.HTMLParser())
    form_data_for_post = {
        '__EVENTVALIDATION': html1.find('.//input[@id="__EVENTVALIDATION"]').get('value'),
        '__VIEWSTATE': html1.find('.//input[@id="__VIEWSTATE"]').get('value'),
        '__VIEWSTATEGENERATOR': html1.find('.//input[@id="__VIEWSTATEGENERATOR"]').get('value'),
        'ctl00$ctl00$Content$Content$ctl04$btnGetGengi': 'Sækja',
        'ctl00$ctl00$Content$Content$ctl04$ddlDays': str(req_date.day),
        'ctl00$ctl00$Content$Content$ctl04$ddlMonths': str(req_date.month),
        'ctl00$ctl00$Content$Content$ctl04$ddlYears': str(req_date.year)
    }
    time.sleep(0.8)  # just to look polite
    res2 = session.post(url, headers=headers_for_post, data=form_data_for_post)
    res2.raise_for_status()
    staturory_holiday_msg = 'er lögbundinn frídagur, en það er ekkert gengi skráð á slíkum dögum.'
    if bytes(staturory_holiday_msg, 'utf-8') in res2.content:
        data['status']['success'] = False
        data['status']['msg'] = '%s, "%s" %s.' % (
            'No currency rates were returned because of staturory holiday',
            date_str,
            'is a staturory holiday'
        )
        if logger is not None:
            logger.info(data['status']['msg'])
        return data
    html2 = lxml.etree.fromstring(res2.content, lxml.etree.HTMLParser())
    html2tables = html2.findall('.//table')
    shown_date = datetime.datetime.strptime(
        html2tables[1].find('.//tr/td/span').text,
        'Skráning: %d.%m.%Y'
    )
    assert(req_date == shown_date)
    for row in html2tables[0].findall('.//tr'):
        if row.find('.//th') is not None:
            continue
        columns = row.findall('.//td')
        currency_data = {
            'name': columns[0].text.strip(),
            'code': columns[1].text,
            'buy': float(columns[2].text.replace(',', '.')),
            'sell': float(columns[3].text.replace(',', '.')),
            'mean': float(columns[4].text.replace(',', '.')),
        }
        data['currencies'][currency_data['code'].lower()] = currency_data
    assert(len(data['currencies'].keys()) > 0)
    if logger is not None:
        logger.info('ISK exchange rate for %s extracted.' % (date_str, ))
    return data


def get_crude_oil_rate_history(date_a=None, date_b=None, logger=None):
    '''
    Extracts historical crude oil prices in USD/barrel from the Federal Reserve Bank of St Louis.

    Federal Reserve Bank of St Louis provides this data in the form of a CSV file, with data from
    1987-05-20 to current date.

    Usage:  res_data = get_crude_oil_rate_history(date_a, date_b)
    Before: @date_a and @date_b are both optional parameters, both should be datetime.datetime
            objects, @date_a represents start date (set to 1987-05-20 if omitted) and @date_b
            represents end date (set to todays date if omitted)
    After:  @res_data is a dict containing crude oil price rate for a selected timeframe.

    Note: Crude Oil rate not available for weekends and some US staturory holidays.
    '''
    if date_a is None:
        start_date = datetime.datetime(1987, 5, 20)
    else:
        assert(type(date_a) == datetime.datetime)
        start_date = date_a
    if date_b is None:
        end_date = datetime.datetime.now()
    else:
        assert(type(date_b) == datetime.datetime)
        end_date = date_b
    assert(start_date <= end_date)
    today = datetime.datetime.now()
    url = 'https://fred.stlouisfed.org/series/DCOILBRENTEU'
    session = requests.Session()
    res1 = session.get(url, headers={'User-Agent': USER_AGENT})
    res1.raise_for_status()
    csv_url = (  # no idea what params are required to not offend server
        'https://fred.stlouisfed.org/graph/fredgraph.csv?'
        'bgcolor=%23e1e9f0&'
        'chart_type=line&'
        'drp=0&'
        'fo=open%20sans&'
        'graph_bgcolor=%23ffffff&'
        'height=450&'
        'mode=fred&'
        'recession_bars=on&'
        'txtcolor=%23444444&'
        'ts=12&'
        'tts=12&'
        'width=748&'
        'nt=0&'
        'thu=0&'
        'trc=0&'
        'show_legend=yes&'
        'show_axis_titles=yes&'
        'show_tooltip=yes&'
        'id=DCOILBRENTEU&'
        'scale=left&'
        'cosd={start_date}&'
        'coed={end_date}&'
        'line_color=%234572a7&'
        'link_values=false&'
        'line_style=solid&'
        'mark_type=none&'
        'mw=3&'
        'lw=2&'
        'ost=-99999&'
        'oet=99999&'
        'mma=0&'
        'fml=a&'
        'fq=Daily&'
        'fam=avg&'
        'fgst=lin&'
        'fgsnd=2009-06-01&'
        'line_index=1&'
        'transformation=lin&'
        'vintage_date={today}&'
        'revision_date={today}&'
        'nd=1987-05-20'
    ).format(
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d'),
        today=today.strftime('%Y-%m-%d')
    )
    csv_headers = {
        'Host': 'fred.stlouisfed.org',
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://fred.stlouisfed.org/series/DCOILBRENTEU',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    time.sleep(2)  # just to look polite
    res2 = session.get(csv_url, headers=csv_headers)
    res2.raise_for_status()
    # open('debug_crude.txt', 'wb').write(res2.content)
    fake_file = io.StringIO(res2.content.decode('utf-8'))  # csv module takes in file, not string
    reader = csv.DictReader(fake_file, delimiter=',')
    assert(reader.fieldnames == ['DATE', 'DCOILBRENTEU'])
    data = {}
    for line in reader:
        date = line['DATE']
        value = line['DCOILBRENTEU']
        datetime.datetime.strptime(date, '%Y-%m-%d')  # instead of an assert :3
        if value == '.':
            continue
        data[date] = float(value)
    assert(len(data.keys()) > 0)
    if logger is not None:
        logger.info('Crude Oil rate for dates [%s to %s] (%s lines) extracted.' % (
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d'),
            len(data.keys())
        ))
    return data


def get_icelandic_fuel_price_history(req_date=None, logger=None):
    '''
    Extracts historical fuel price from FÍB and Gasvaktin.

    FÍB has monitored monthly mean price on petrol in Iceland since 1996-09 and monthly mean price
    on diesel since 2005-07. Gasvaktin began monitoring detailed fuel price changes in 2016-04-19.

    Usage:  res_data = get_icelandic_fuel_price_history(req_date)
    Before: @req_date is a datetime.datetime object containing date in the range 1996-09-01 to our
            present date.
    After:  @res_data is a dict containing mean fuel price rate change history from @req_date to
            the present for petrol and diesel.

    Note: If not obvious from the above, prices before 2016-04-19 are read from the FÍB monthly
          mean data. Gasvaktin data is preferred over FÍB data because it's more detailed, however
          FÍB deserves credit and praise for its data collection through the years.
    '''
    data = {
        'petrol': {},
        'diesel': {}
    }
    session = requests.Session()
    gasvaktin_beginning = datetime.datetime(2016, 4, 19)
    headers = {'User-Agent': USER_AGENT}
    last_petrol_price = None
    last_diesel_price = None
    if req_date is None or req_date < gasvaktin_beginning:
        if logger is not None:
            logger.info('Fetching and parsing FÍB data ..')
        fib_url = (
            'https://www.fib.is/is/billinn/eldsneytisvakt-fib/eldsneytisvaktin-throun?'
            'companies=&'
            'start=1995-10-01&'
            'petrol=bensin'
        )
        res1 = session.get(fib_url, headers=headers)
        res1.raise_for_status()
        time.sleep(0.5)  # just to look polite
        fib_petrol = 'https://www.fib.is/eldsneytisvakt_fib/data/mean_prices_bensin.csv'
        fib_diesel = 'https://www.fib.is/eldsneytisvakt_fib/data/mean_prices_diesel.csv'
        res2 = session.get(fib_petrol, headers=headers)
        res2.raise_for_status()
        fake_file1 = io.StringIO(res2.content.decode('utf-8'))
        reader1 = csv.DictReader(fake_file1, delimiter=',')
        for line in reader1:
            date = line['date']
            if gasvaktin_beginning.strftime('%Y-%m-%d') < date:
                continue  # prefer Gasvaktin data over FÍB data because it's more detailed
            if req_date is not None and date < req_date.strftime('%Y-%m-%d'):
                continue  # throw data outside time range selection
            if line['price_without_services'] != 'null':
                price = float(line['price_without_services'])
            else:
                price = float(line['price_with_services'])
            last_petrol_price = price
            datetime.datetime.strptime(date, '%Y-%m-%d')
            data['petrol'][date] = float(price)
        time.sleep(0.2)  # just to look polite
        res3 = session.get(fib_diesel, headers=headers)
        res3.raise_for_status()
        fake_file2 = io.StringIO(res3.content.decode('utf-8'))
        reader2 = csv.DictReader(fake_file2, delimiter=',')
        for line in reader2:
            date = line['date']
            if gasvaktin_beginning.strftime('%Y-%m-%d') < date:
                continue  # prefer Gasvaktin data over FÍB data because it's more detailed
            if req_date is not None and date < req_date.strftime('%Y-%m-%d'):
                continue  # throw data outside time range selection
            if line['price_without_services'] != 'null':
                price = float(line['price_without_services'])
            else:
                price = float(line['price_with_services'])
            last_diesel_price = price
            datetime.datetime.strptime(date, '%Y-%m-%d')
            data['diesel'][date] = float(price)
    if logger is not None:
        logger.info('Fetching and parsing Gasvaktin data ..')
    url = 'https://raw.githubusercontent.com/gasvaktin/gasvaktin/master/vaktin/trends.min.json'
    res4 = session.get(url, headers=headers)
    res4.raise_for_status()
    gasvaktin_trends = res4.json()
    current_date = datetime.datetime(2016, 4, 19)
    today = datetime.datetime.now()
    while current_date.strftime('%Y-%m-%d') < today.strftime('%Y-%m-%d'):
        current_date_str = current_date.strftime('%Y-%m-%dT23:59')
        if req_date is None or req_date.strftime('%Y-%m-%d') <= current_date.strftime('%Y-%m-%d'):
            current_prices = {}
            # programmers note: this double for loop implementation is bad and programmer should
            # feel bad, but when programmer wrote this, programmer was feeling lazy and also
            # programmer thinks bad implementation doesn't matter so much because dataset we're
            # working with here doesn't grow fast enough to become a problem in near future (few
            # years at least)
            for company_key in gasvaktin_trends:
                for change in gasvaktin_trends[company_key]:
                    if change['timestamp'] < current_date_str:
                        current_prices[company_key] = change
                    else:
                        break
            # calculate mean petrol and diesel price
            number_of_stations = 0
            total_petrol_price = 0.0
            total_diesel_price = 0.0
            for company_key in current_prices:
                if current_prices[company_key]['stations_count'] == 0:
                    continue
                stations_count = current_prices[company_key]['stations_count']
                number_of_stations += stations_count
                mean_petrol_price = current_prices[company_key]['mean_bensin95']
                mean_diesel_price = current_prices[company_key]['mean_diesel']
                total_petrol_price += (mean_petrol_price * stations_count)
                total_diesel_price += (mean_diesel_price * stations_count)
            current_petrol_price = round((total_petrol_price / number_of_stations), 2)
            current_diesel_price = round((total_diesel_price / number_of_stations), 2)
            if current_petrol_price != last_petrol_price:
                data['petrol'][current_date.strftime('%Y-%m-%d')] = current_petrol_price
                last_petrol_price = current_petrol_price
            if current_diesel_price != last_diesel_price:
                data['diesel'][current_date.strftime('%Y-%m-%d')] = current_diesel_price
                last_diesel_price = current_diesel_price
        current_date += datetime.timedelta(days=1)
    if logger is not None:
        logger.info('Finished parsing icelandic fuel price data.')
    return data
