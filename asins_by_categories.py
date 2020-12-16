import re
import requests
import pandas as pd
from time import time

from bs4 import BeautifulSoup

from amazoncaptcha import AmazonCaptcha

import warnings

warnings.filterwarnings("ignore")

_MAX_TRIAL_REQUESTS = 3

headers = {
    'authority': 'www.amazon.com',
    'dnt': '1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/87.0.4280.88 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,'
              'image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'none',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-dest': 'document',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
}


def solve_captcha(r):
    btf = BeautifulSoup(r.text)
    form = btf.find('form', attrs={'action': '/errors/validateCaptcha'})
    amzn = form.find('input', attrs={'name': 'amzn'})['value']
    img_url = form.find('img')['src']
    solution = AmazonCaptcha.fromlink(img_url).solve()
    session.get(f'https://www.amazon.com/errors/validateCaptcha?amzn={amzn}&amzn-r=%2F&field-keywords={solution}')


def valid_page(html_content):
    """Check if the page is a valid result page
    (even if there is no result)"""
    if "Sign in for the best experience" in html_content:
        valid_page_bool = False
    elif 'Enter the characters you see below' in html_content:
        valid_page_bool = False
    elif "The request could not be satisfied." in html_content:
        valid_page_bool = False
    elif "We couldn&#39;t find that page" in html_content:
        valid_page_bool = False
    elif "Robot Check" in html_content:
        valid_page_bool = False
    else:
        valid_page_bool = True
    return valid_page_bool


def query(url):
    for i in range(_MAX_TRIAL_REQUESTS):
        req = session.get(url)
        if not valid_page(req.text):
            if 'Enter the characters you see below' in req.text:
                solve_captcha(req)
            else:
                raise Exception('not valid page from "SessionThread"')
        else:
            return req
    raise Exception('not valid page from "SessionThread"')


def scrap_asins(url):
    page_one = query(url)
    page_two = query(f'{url}/ref=zg_bs_pg_2?_encoding=UTF8&pg=2')

    asins = []
    for page in [page_one, page_two]:
        soup = BeautifulSoup(page.text)
        asins += [re.findall(r"\/dp\/[\d\w]{10}", li.find_all('a')[0]['href'])[0][len('/dp/'):] for li in
                  soup.findAll('li', attrs={'class': 'zg-item-immersion'}) if li.find_all('a')]
    return asins


session = requests.Session()
session.headers.update(headers)

start_time = time()

xl = pd.ExcelFile('data/input categories.xlsx')
writer = pd.ExcelWriter(f'data/output asins.xlsx', engine='xlsxwriter')

df = xl.parse(xl.sheet_names[0], header=None)
for _, row in df.iterrows():
    title = row[0]
    url_link = row[1]
    data = scrap_asins(url_link)
    pd.DataFrame(data).to_excel(writer, sheet_name=title, header=False, index=False)

writer.save()

print(time() - start_time, 'seconds')
