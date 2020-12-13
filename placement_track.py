import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC

import requests
import numpy as np
import pandas as pd
from threading import Thread, Lock

from bs4 import BeautifulSoup
from amazoncaptcha import AmazonCaptcha

import warnings

warnings.filterwarnings("ignore")

# config  ------------------------------------------------------------------------------------------
absolute_path = "/Users/eugene404/Documents/GitHub/Amazon_work/config/"
data_folder = 'data/placement track data/'

pref = {
    "download.default_directory": absolute_path,
    "browser.download.dir": absolute_path,
    "savefile.default_directory": absolute_path,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "download.safebrowsing.enabled": True
}
chromeOptions = webdriver.ChromeOptions()
chromeOptions.add_experimental_option("prefs", pref)

zip_codes = [
    10001,  # New York (New York)
    90001,  # California (Los Angeles)
    33124,  # Florida (Miami)
    78701,  # Texas (Austin)
    15201,  # Pennsylvania (Pittsburgh)
]

number_of_pages_to_search = 3
_MAX_TRIAL_REQUESTS = 3


# config end ------------------------------------------------------------------------------------------

# todo Add new function to check "Deliver to" span with appropriate city for each session

def amazon_url_search(search_k, page=1):
    search_k = search_k.replace(' ', '+')
    return f"https://www.amazon.com/s?k={search_k}&page={page}"


def valid_page(html_content):
    """Check if the page is a valid result page
    (even if there is no result)"""
    # if "Sign in for the best experience" in html_content:
    #     valid_page_bool = False

    if 'Enter the characters you see below' in html_content:
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


def solve_captcha(session, r):
    btf = BeautifulSoup(r.text)
    form = btf.find('form', attrs={'action': '/errors/validateCaptcha'})
    amzn = form.find('input', attrs={'name': 'amzn'})['value']
    img_url = form.find('img')['src']
    solution = AmazonCaptcha.fromlink(img_url).solve()
    session.get(f'https://www.amazon.com/errors/validateCaptcha?amzn={amzn}&amzn-r=%2F&field-keywords={solution}')


def create_session(zip_code_amazon):
    browser = webdriver.Chrome('config/chromedriver', chrome_options=chromeOptions)

    browser.get('https://www.amazon.com/')
    wait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "//span[text()='\n                   "
                                                                  "Deliver to\n                ']"))).click()

    wait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "//input[@id='GLUXZipUpdateInput']")))
    find_ell = browser.find_element_by_id('GLUXZipUpdateInput')
    find_ell.clear(), find_ell.send_keys(str(zip_code_amazon))

    find_ell = browser.find_element_by_id('GLUXZipUpdate')
    find_ell.click()

    browser.get('https://www.amazon.com/')

    cookies = browser.get_cookies()
    agent = browser.execute_script("return navigator.userAgent")

    headers = {
        'authority': 'www.amazon.com',
        'dnt': '1',
        'upgrade-insecure-requests': '1',
        'user-agent': agent,
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,'
                  'image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-dest': 'document',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    }

    s = requests.Session()
    s.headers.update(headers)

    for cookie in cookies:
        s.cookies.update({cookie['name']: cookie['value']})

    req = s.get('https://www.amazon.com/')
    browser.close()

    if not valid_page(req.text):
        for i in range(_MAX_TRIAL_REQUESTS):
            if 'Enter the characters you see below' in req.text:
                solve_captcha(s, req.text)
                req = s.get('https://www.amazon.com/')
                if valid_page(req.text):
                    break
            else:
                raise Exception('not valid page from "getting session configs"')

    return s


def placement_find(asin_str, keywords_df, session_list, session_lock_list, writer_excel, writer_lock):
    keywords_df['row'] = 0
    keywords_df['column'] = 0
    keywords_df['page'] = 0

    main_lock = Lock()
    df = []

    def run(s, lock):
        df_ = keywords_df.copy()
        for index, item in df_.iterrows():
            keyword = item['keyword']
            for page_index in range(1, number_of_pages_to_search + 1):
                with lock:
                    req = s.get(amazon_url_search(keyword, page=page_index)).text

                # get SPAN with data-component-type equal s-search-results
                if len(BeautifulSoup(req).find_all('span', attrs={'data-component-type': 's-search-results'})) != 1:
                    raise Exception('at placement_find function find not single span')
                span = BeautifulSoup(req).find('span', attrs={'data-component-type': 's-search-results'})

                # get second div with class equal s-main-slot s-result-list s-search-results sg-row
                if len(BeautifulSoup(req).find_all('div', attrs={'class': 's-main-slot s-result-list '
                                                                          's-search-results sg-row'})) != 1:
                    raise Exception('at placement_find function find not single div')
                products_div = span.find('div', attrs={'class': 's-main-slot s-result-list s-search-results sg-row'})

                # look for all div with data-asin not empty
                divs = [div for div in products_div.find_all('div') if div.has_attr("data-asin") and
                        div.has_attr("data-index") and div.get('data-asin') and div.has_attr('data-component-type')
                        and div.get('data-component-type') == 's-search-result']

                divs_with_asin = [div for div in divs if div.get('data-asin') == asin_str]
                _ = []
                if len(divs_with_asin):
                    for div_i in divs_with_asin:
                        # div with data-component-type equal sp-sponsored-result
                        if div_i.find('div', attrs={'data-component-type': 'sp-sponsored-result'}):
                            _.append(div_i)
                    if len(_) > 1:
                        raise Exception('at placement_find function find more than needed sponsored products')
                    if len(_) < 1:
                        continue
                    product = _[0]
                    divs_index = divs.index(product)
                    row = divs_index // 4 + 1
                    column = divs_index % 4 + 1
                    df_.loc[df_['keyword'] == keyword, 'row'] = row
                    df_.loc[df_['keyword'] == keyword, 'column'] = column
                    df_.loc[df_['keyword'] == keyword, 'page'] = page_index
                    break

        df_['zip_code'] = zip_codes[session_list.index(s)]
        with main_lock:
            df.append(df_)

    thread_session = [Thread(target=run, args=[session, session_lock]) for session, session_lock in
                      zip(session_list, session_lock_list)]

    for thread in thread_session:
        thread.start()

    for thread in thread_session:
        thread.join()

    df = pd.concat(df, ignore_index=True)
    df = df.set_index(['campaign', 'keyword', 'zip_code']).sort_index()
    with writer_lock:
        df.to_excel(writer_excel, sheet_name=asin_str)


sessions, session_locks = [], []
for zip_code in zip_codes:
    sessions.append(create_session(zip_code))
    session_locks.append(Lock())

xl = pd.ExcelFile('data/placement track data/input.xlsx')
asins = xl.sheet_names

writer = pd.ExcelWriter(f'data/placement track data/output.xlsx', engine='xlsxwriter')
writer_l = Lock()

thread_asin = [Thread(target=placement_find, args=[str(asin), xl.parse(str(asin)), sessions, session_locks,
                                                   writer, writer_l]) for asin in asins]

for process in thread_asin:
    process.start()

for process in thread_asin:
    process.join()

writer.save()
