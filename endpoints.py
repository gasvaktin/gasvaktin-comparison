#!/usr/bin/python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------- #
import calendar
import csv
import datetime
import io
import json
import re
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
            'buy': None,
            'sell': None,
            'mean': float(columns[2].text.replace(',', '.')),
        }
        data['currencies'][currency_data['code'].lower()] = currency_data
    assert(len(data['currencies'].keys()) > 0)
    if logger is not None:
        logger.info('ISK exchange rate for %s extracted.' % (date_str, ))
    return data


def get_crude_oil_rate_history(date_a=None, date_b=None, logger=None):
    '''
    Extracts historical crude oil prices in USD/bbl from mbl.is (data originates from eia.gov,
    U.S. Energy Information Administration).

    The U.S. Energy Information Administration has data from 1987-05-20 to current date, they do
    offer bulk download and also an API, but to use the API we need to register for an access key.
    The bulk download way is .. bulky. There are at this time of writing 13 separate bulk files
    available, the PET.zip file containing EU brent crude oil price is around 50 MB in size.
    See: https://www.eia.gov/opendata/bulkfiles.pyp

    Fortunately, mbl.is seems to have started fetching this data from eia.gov and makes it easily
    available on its site, so we just fetch it from them instead of signing up for an access key.

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
    # url = (
    #     'https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key={access_key}'
    #     '&frequency=daily'
    #     '&data[0]=value'
    #     '&start={start_date}'
    #     '&end={end_date}'
    #     '&sort[0][column]=period'
    #     '&sort[direction]=desc'
    #     '&offset=0'
    #     '&length=5000'
    # ).format(access_key='DUMMY_KEY', start_date='2023-01-01', end_date='2023-02-01')
    url = 'https://www.mbl.is/vidskipti/oliuverd/'
    res = requests.get(url, headers={'User-Agent': USER_AGENT})
    res.raise_for_status()
    html = lxml.etree.fromstring(res.content, lxml.etree.HTMLParser())
    data_str = None
    data = None
    parsed_data = {}
    start_string = 'var olia_1_graf_data = '
    end_string = ';'
    for script_element in html.findall('.//script'):
        if script_element.text is None:
            continue
        if start_string in script_element.text and end_string in script_element.text:
            start = script_element.text.find(start_string) + len(start_string)
            end = script_element.text.find(end_string)
            if start >= end:
                raise Exception('Data extraction failure.')
            data_str = script_element.text[start:end]
            data = json.loads(data_str)
            break
    if data is None:
        raise Exception('Failed to locate data.')
    for epoch_time, price in data:
        date_datetime = datetime.datetime.fromtimestamp(epoch_time / 1000)
        date_isoformatted = date_datetime.strftime('%Y-%m-%d')
        if (date_isoformatted < start_date.strftime('%Y-%m-%d') or
            end_date.strftime('%Y-%m-%d') <= date_isoformatted or
                today.strftime('%Y-%m-%d') <= date_isoformatted):
            continue  # strip away data for unwanted days
        value_float = float(price)
        parsed_data[date_isoformatted] = value_float
    assert(len(parsed_data.keys()) > 0)
    if logger is not None:
        logger.info('Crude Oil rate for dates [%s to %s] (%s lines) extracted.' % (
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d'),
            len(parsed_data.keys())
        ))
    return parsed_data


def get_crude_oil_rate_history_fallback(logger=None):
    '''
    Extracts historical crude oil prices in USD/bbl from markets.businessinsider.com

    This function exists because datasource behind the `get_crude_oil_rate_history` function tends
    to lag behind.

    Usage:  res_data = get_crude_oil_rate_history_fallback()
    Before: Nothing
    After:  @res_data is a dict containing crude oil price rate for the past year.

    Note: Data extracted from this should not be directly mixed with data from
          `get_crude_oil_rate_history` because these two data sources aren't guaranteed to be in
          harmony with each other.
    '''
    if logger is not None:
        logger.info('Fetching fallback crude data from markets.businessinsider.com ..')
    url = 'https://markets.businessinsider.com/commodities/oil-price/usd?type=brent'
    session = requests.Session()
    res = session.get(url, headers={'User-Agent': USER_AGENT})
    res.raise_for_status()
    html = lxml.etree.fromstring(res.content, lxml.etree.HTMLParser())
    script_text = None
    for script_element in html.findall('.//script'):
        if script_element.keys() == [] and 'var detailChartViewmodel = {' in script_element.text:
            script_text = script_element.text
            break
    assert(script_text is not None)
    re_instrument_type = re.search(r'(?<="InstrumentType" : ")([^"]*)(?=")', script_text)
    re_tkdata = re.search(r'(?<="TKData" : ")([^"]*)(?=")', script_text)
    assert(re_instrument_type is not None)
    assert(re_tkdata is not None)
    now = datetime.datetime.utcnow()
    now_str = now.strftime('%Y-%m-%d')
    year_days = 365
    if (
        (calendar.isleap(now.year) and now.month > 2) or
        (calendar.isleap(now.year - 1) and now.month <= 2)
    ):
        year_days = 366
    last_year = (datetime.datetime.utcnow() - datetime.timedelta(days=year_days))
    then = (last_year - datetime.timedelta(days=(31 + 30)))
    chart_data_url = (
        'https://markets.businessinsider.com/Ajax/Chart_GetChartData?'
        'instrumentType={instrument_type}&'
        'tkData={tk_data}&'
        'from={date_from}&'
        'to={date_to}'
    ).format(
        instrument_type=re_instrument_type.group(),
        tk_data=re_tkdata.group(),
        date_from=then.strftime('%Y%m%d'),
        date_to=now.strftime('%Y%m%d')
    )
    chart_data_headers = {
        'User-Agent': USER_AGENT,
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate',
        'accept-language': 'en-US,en;q=0.9',
        'referer': 'https://markets.businessinsider.com/commodities/oil-price/usd?type=brent',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin'
    }
    res2 = session.get(chart_data_url, headers=chart_data_headers)
    res2.raise_for_status()
    chart_data = res2.json()
    assert(type(chart_data) is list)
    data = {}
    for data_item in chart_data:
        assert('Close' in data_item)
        assert(type(data_item['Close']) in (float, int))
        assert(type(data_item['Date']) is str)
        item_datetime = datetime.datetime.strptime(data_item['Date'], '%Y-%m-%d %H:%M')
        item_date_str = item_datetime.strftime('%Y-%m-%d')
        item_value = float(data_item['Close'])
        if now_str > item_date_str:
            data[item_date_str] = item_value
    if logger is not None:
        logger.info('Successfully fetched fallback crude data from markets.businessinsider.com')
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


def get_isk_inflation_index_history(logger=None):
    '''
    Extracts ISK inflation index data from Hagstofa Íslands.

    Hagstofa Íslands maintains and exposes monthly ISK inflation index data dating back to 1939-01
    where the index is set to 100. In the year 1981 there was a currency change in Iceland, from
    old ISK to new ISK, where two zeros were cut off the ISK so to speak. This fact is not
    reflected in the inflation index so if you want to calculate to/from old ISK amounts for one or
    another reason then you would want to simply multiply/divide the amount by 100.

    Usage:  res_data = get_isk_inflation_index_history()
    Before: Nothing
    After:  @res_data is a dict containing monthly isk inflation index historical data from 1939-01
            up to last month.

    Note: [From website, in icelandic, regarding index definition, which has changed over time]
          "Til grundvallar útreikningnum eru notuð birt gildi vísitölu framfærslukostnaðar og
          vísitölu neysluverðs. Á árunum 1968 – 1983 var vísitala framfærslukostnaðar reiknuð
          fjórum sinnum á ári. Athugið að verðtrygging hefur ekki alltaf miðast við vísitölu
          framfærslukostnaðar eða vísitölu neysluverðs eingöngu."
    '''
    today = datetime.datetime.strftime(datetime.datetime.utcnow(), '%Y-%m-%d')
    session = requests.Session()
    session.get('https://hagstofa.is/verdlagsreiknivel', headers={'User-Agent': USER_AGENT})
    time.sleep(0.1)
    url = 'https://px.hagstofa.is/pxis/api/v1/is/Efnahagur/visitolur/1_vnv/1_vnv/VIS01002.px'
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Host': 'px.hagstofa.is',
        'Origin': 'https://hagstofa.is',
        'Referer': 'https://hagstofa.is/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': USER_AGENT
    }
    form_data_for_post = (
        '{"query":[{"code":"Vísitala","selection":{"filter":"item","values":["CPI"]}},{"code":'
        '"Grunnur","selection":{"filter":"item","values":["B1939"]}}],"response":{"format":"csv"}}'
    ).encode('utf-8')
    res = session.post(url, headers=headers, data=form_data_for_post)
    res.raise_for_status()
    content = res.content.decode('utf-8')
    first_line = True
    line_regex = r'(?:\")([0123456789]*)(?:M)([0123456789]*)(?:\",)([.]|[0123456789]*)'
    content_lines = content.split('\r\n')
    datalist = []
    last_value = None
    last_date_str = None
    for count, line in enumerate(content_lines):
        line = line.replace('\ufeff', '')
        if first_line:
            assert(line == '"Mánuður","Vísitala neysluverðs Grunnur 1939"')
            first_line = False
            continue
        match = re.match(line_regex, line)
        if match is None:
            assert(count == (len(content_lines) - 1))
            continue
        year, month, value = match.groups()
        if value == '.':
            value = last_value
        date_str = '%s-%s-01' % (year, month)
        assert('1939-01-01' <= date_str)
        if last_date_str is not None:
            assert(last_date_str < date_str)
        assert(date_str < today)
        datalist.append({'date': date_str, 'value': value})
        if value != '.':
            last_value = value
        last_date_str = date_str
    return {'list': datalist}


if __name__ == '__main__':
    import pprint
    print('running get_crude_oil_rate_history_fallback ..')
    pprint.pprint(get_crude_oil_rate_history_fallback())
