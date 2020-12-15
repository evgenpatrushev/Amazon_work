import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC

import requests
from time import time
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
    btf = BeautifulSoup(r)
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


class SessionThread(Thread):
    def __init__(self, asin_find, s, zip_c, session_lock, df, number_threads=10):
        Thread.__init__(self)
        self.asin = asin_find
        self.session = s
        self.zip_code = zip_c
        self.session_lock = session_lock
        self.number_threads = number_threads
        self.df = df.copy()

    def run(self):
        with print_lock:
            print(f'started thread for asin {self.asin} for {self.zip_code} zip code')
        if 0 < self.number_threads < len(self.df):
            threads = []
            indexes = np.round(np.linspace(0, len(self.df), self.number_threads))
            for i, _ in enumerate(indexes):
                if i == len(indexes) - 1:
                    continue
                else:
                    threads.append(Thread(target=self.thread_run, args=[indexes[int(i)], indexes[int(i) + 1]]))
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        else:
            self.thread_run(0, len(self.df))

        self.df['zip_code'] = self.zip_code
        with print_lock:
            print(f'ended thread for asin {self.asin} for {self.zip_code} zip code')

    def thread_run(self, start, end):
        for index, item in self.df.iloc[int(start):int(end), :].iterrows():
            keyword = item['keyword']
            for page_index in range(1, number_of_pages_to_search + 1):
                req = self.query(keyword, page_index)

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

                divs_with_asin = [div for div in divs if div.get('data-asin') == self.asin]
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
                    self.df.loc[self.df['keyword'] == keyword, 'row'] = row
                    self.df.loc[self.df['keyword'] == keyword, 'column'] = column
                    self.df.loc[self.df['keyword'] == keyword, 'page'] = page_index
                    break

    def query(self, keyword, page):
        for i in range(_MAX_TRIAL_REQUESTS):
            with self.session_lock:
                req = self.session.get(amazon_url_search(keyword, page=page)).text
            if not valid_page(req):
                if 'Enter the characters you see below' in req:
                    solve_captcha(self.session, req)
                else:
                    raise Exception('not valid page from "SessionThread"')
            else:
                return req


def placement_find(asin_str, keywords_df, session_list, session_lock_list, writer_excel, writer_lock):
    with print_lock:
        print(f'Asin {asin_str} started')
    keywords_df['row'] = 0
    keywords_df['column'] = 0
    keywords_df['page'] = 0

    thread_session = [SessionThread(asin_find=asin_str, s=session, zip_c=zip_codes[session_list.index(session)],
                                    session_lock=session_lock, df=keywords_df, number_threads=10) for
                      session, session_lock in zip(session_list, session_lock_list)]

    for thread in thread_session:
        thread.start()

    for thread in thread_session:
        thread.join()

    df = pd.concat([thread.df for thread in thread_session], ignore_index=True)
    df = df.set_index(['campaign', 'keyword', 'zip_code']).sort_index()
    with writer_lock:
        df.to_excel(writer_excel, sheet_name=asin_str)
    with print_lock:
        print(f'Asin {asin_str} ended')


start_time = time()
print_lock = Lock()

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

print(time() - start_time, 'seconds')
