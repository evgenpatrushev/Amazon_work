import requests
import time as t
import pandas as pd

import re

from bs4 import BeautifulSoup
from amazoncaptcha import AmazonCaptcha

from threading import Thread, Lock

import warnings

warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', 500)
pd.set_option('display.width', 10000)

_USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0']

_MAX_TRIAL_REQUESTS = len(_USER_AGENT_LIST) * 3
_WAIT_TIME_BETWEEN_REQUESTS = 0.5

CHANGE_LOCATION_URL = 'https://www.amazon.com/gp/delivery/ajax/address-change.html?locationType=LOCATION_INPUT' \
                      '&zipCode=32210&storeContext=generic&deviceType=web&pageType=Gateway&actionSource=glow '


class ScrapThreadVariations(Thread):
    def __init__(self, session, header, asins, lock, thread_name):
        Thread.__init__(self)
        self.session = session
        self.header = header
        self.asins = asins
        self.lock = lock
        self.thread_name = thread_name
        self.return_val = {}

    def run(self):
        # print(f"THREAD INFO: {self.thread_name} start working")

        for asin in self.asins:
            self.return_val[asin] = self.get_variations(amazon_url_product(asin))

        # print(f"THREAD INFO: {self.thread_name} end working ")

    def get_response(self, url):
        for i in range(5):
            res = self.session.get(url, headers=self.header)
            if not check_page(res.text):
                if 'Enter the characters you see below' in res.text:
                    # print(f'{self.thread_name} get CAPTCHA')
                    solve_captcha(self.session, self.header, res)
                else:
                    raise ConnectionError('during get response running get not valid page')
            else:
                return res.text
        raise Exception('smth went wrong at get_response')

    def get_variations(self, url):
        asins = []

        with self.lock:
            data = self.get_response(url)
            t.sleep(0.5)
        # data = data.decode('utf-8')

        btf = BeautifulSoup(data)

        center = btf.find_all('div', attrs={'id': 'centerCol'})[0]

        if not center.find_all('div', attrs={'id': 'twisterContainer'}):
            return [url.split('/')[-1]]

        variations = center.find_all('div', attrs={'id': 'twisterContainer'})[0]

        # todo test how we get button click variations
        for li in variations.find_all('li'):
            if li.has_attr('data-defaultasin') and li['data-defaultasin']:
                asins.append(li['data-defaultasin'])

        # # todo test how we get select variations after
        # if variations.find_all('select', attrs={'name': 'dropdown_selected_size_name'}):
        #     new_asins = []
        #     for asin in asins:
        #         data = await get_response_async(client, amazon_url_product(asin))
        #         data = data.decode('utf-8')
        #         if not check_page(data):
        #             raise ValueError(
        #                 'No valid pages found! Perhaps the page returned is a CAPTCHA? Check products.last_html_page')
        #         btf = BeautifulSoup(data)
        #
        #         center = btf.find_all('div', attrs={'id': 'centerCol'})[0]
        #         select = center.find_all('select', attrs={'name': 'dropdown_selected_size_name'})[0]
        #         for option in select.find_all('option'):
        #             value = option['value']
        #             if value != '-1':
        #                 new_asins.append(value.split(',')[-1])
        #     asins = new_asins

        if not asins:
            asins = [url.split('/')[-1]]

        return asins


def amazon_url_list(search_i):
    search_i = search_i.replace(' ', '+')
    return f"https://www.amazon.com/s?k={search_i}&ref=nb_sb_noss"


def amazon_url_product(asin_pr):
    return f"https://www.amazon.com/dp/{asin_pr}"


def check_page(html_content):
    """Check if the page is a valid result page
    (even if there is no result)"""
    if "Sign in for the best experience" in html_content:
        valid_page = False
    elif 'Enter the characters you see below' in html_content:
        valid_page = False
    elif "The request could not be satisfied." in html_content:
        valid_page = False
    elif "We couldn&#39;t find that page" in html_content:
        valid_page = False
    elif "Robot Check" in html_content:
        valid_page = False
    else:
        valid_page = True
    return valid_page


def get_mainpage_categories(data):
    """
    scrap main page asin categories from product info
    :param data: text response from amazon of page
    :return: dict {category_title: [rating, category_id]}
    """

    if not check_page(data):
        raise ValueError(
            'No valid pages found! Perhaps the page returned is a CAPTCHA? Check products.last_html_page')

    # todo add data slice by id="detailBullets" for cutting processing time
    if 'Best Sellers Rank' in data:
        if 'Videos for related products' in data:
            data = data[data.index('Best Sellers Rank'):data.index('Videos for related products') + 1]
        elif 'Customer questions & answers' in data:
            data = data[data.index('Best Sellers Rank'):data.index('Customer questions & answers') + 1]
        elif 'Customer reviews' in data:
            data = data[data.index('Best Sellers Rank'):data.index('Customer reviews') + 1]
        else:
            data = data[data.index('Best Sellers Rank'):]

    else:
        return {'No amazon category found': 1}

    data = data[data.index('See Top') + 1:]
    btf = BeautifulSoup(data)

    links = btf.findAll('a')
    categories = {rating.split(' in ')[1].strip(): [int(re.sub('[#,]', '', rating.split(' in ')[0])),
                                                    ''.join([i for i in links[i]['href'] if i.isdigit()])]
                  for i, rating in enumerate(re.findall(r"#\d+,?\d* in [\w &-']+", btf.text.replace('\xa0', ' ')))}

    return categories


def scrap_tree_of_categories(url, get_request):
    """
    Scrap list of subcategories from amazon page of category
    :param url: link to category
    :param get_request: function for getting request
    :return: list of lists [..., [title, link, id], ...]
    """
    return_val = []
    r, i, head = get_request(url)

    btf = BeautifulSoup(r.text)
    if len(btf.find_all('span', attrs={'class': 'zg_selected'})) > 1:
        raise Exception('why more than 1 ((( ')

    li = btf.find_all('span', attrs={'class': 'zg_selected'})[0].parent
    ul = li.parent

    if ul.find_all('ul'):
        ul = ul.find_all('ul')[0]

    for li in ul.find_all('li'):
        if li.find_all('a'):
            link = li.find_all('a')[0]['href']
            link = link[:link.index('/ref=')]
            s = ''
            for index in range(len(link)):
                if link[len(link) - 1 - index].isdigit():
                    s = link[len(link) - 1 - index] + s
                else:
                    break
            id_node = int(s)
            link = f'https://www.amazon.com/bestsellers/{s}'
        else:
            link = url
            id_node = url.split('/')[-1]
        return_val.append([li.text, link, id_node])

    return return_val


def solve_captcha(session, header, r):
    btf = BeautifulSoup(r.text)
    form = btf.find('form', attrs={'action': '/errors/validateCaptcha'})
    amzn = form.find('input', attrs={'name': 'amzn'})['value']
    img_url = form.find('img')['src']
    solution = AmazonCaptcha.fromlink(img_url).solve()
    session.get(f'https://www.amazon.com/errors/validateCaptcha?amzn={amzn}&amzn-r=%2F&field-keywords={solution}',
                headers=header)


def input_numbers():
    """
    input user function for command line
    :return:
    """
    while True:
        try:
            numbers = []
            answer = input('\ninput: ')

            if answer.strip() == '':
                print('You entered empty line, which is mean no category chose. Are you sure? (y/n): ')
                while True:
                    exit_answer = input()
                    if exit_answer.strip().lower() == 'n':
                        break
                    elif exit_answer.strip().lower() == 'y':
                        return []
                continue

            answers = answer.split(',')
            for answer in answers:
                answer = answer.strip()
                if '-' in answer:
                    answer = answer.split('-')
                    for number in range(int(answer[0]) - 1, int(answer[1])):
                        numbers.append(number)
                else:
                    numbers.append(int(answer) - 1)
            break
        except:
            print('error, try again')

    numbers = list(set(numbers))
    numbers.sort()
    return numbers


def scrap_asins_from_category(asin):
    """
    Scrap from amazon asins

    :param asin: our asin to search for (str)
    :return:
    """

    def _change_user_agent(current_user_agent_index, header):
        """ Change the User agent of the requests
        (useful if anti-scraping)
        """
        index = (current_user_agent_index + 1) % len(_USER_AGENT_LIST)
        header['user-agent'] = _USER_AGENT_LIST[index]
        return index, header

    def get_query(index, search_url, head, change_location=False):
        trials = 0
        valid_page = False
        res = None
        while trials < _MAX_TRIAL_REQUESTS:

            trials += 1
            try:
                if change_location:
                    res = session.get(CHANGE_LOCATION_URL, headers=head)
                    if res.status_code != 200:
                        raise ConnectionError()

                res = session.get(search_url, headers=head)
                if res.status_code != 200:
                    raise ConnectionError()

                valid_page = check_page(res.text)

            # To counter the "SSLError bad handshake" exception
            except requests.exceptions.SSLError:
                valid_page = False

            except ConnectionError:
                valid_page = False

            if valid_page:
                break

            if 'Enter the characters you see below' in res.text and trials < _MAX_TRIAL_REQUESTS // 2:
                solve_captcha(session, head, res)
            else:
                index, head = _change_user_agent(index, head)
            t.sleep(_WAIT_TIME_BETWEEN_REQUESTS)

        if not valid_page:
            raise ValueError(
                'No valid pages found! Perhaps the page returned is a CAPTCHA? Check products.last_html_page')

        return res, index, head

    def thread_subcategory_asins_find(numbers_subcategory, index, header, writer_excel, data_):
        for subnumber in numbers_subcategory:
            print(f'\n  ## started "{data_[subnumber][0]}" subcategory ##\n')
            # LOAD two pages of amazon for each category --------------------------------------------------------

            page_one, index, header = get_query(index, data_[subnumber][1], header,
                                                change_location=True)

            page_two, index, header = get_query(index,
                                                f'https://www.amazon.com/bestsellers/zgbs/'
                                                f'{data_[subnumber][2]}/ref=zg_bs_pg_2?_encoding=UTF8&pg=2',
                                                header,
                                                change_location=True)

            # PARSE and get all asins (no advertising placements) ------------------------------------------------

            asins = []
            for page in [page_one, page_two]:
                soup = BeautifulSoup(page.text)
                asins += [re.findall(r"\/dp\/[\d\w]{10}", li.find_all('a')[0]['href'])[0][len('/dp/'):] for li in
                          soup.findAll('li', attrs={'class': 'zg-item-immersion'}) if li.find_all('a')]

            # Open each asin and find other asins (async use) ------------------------------------------------
            # for each 10 asins create thread for loading, but using common lock for loading data from amazon

            index_ = 0
            threads = []
            count_in_thread = 10
            lock_obj = Lock()

            for number, j in enumerate(range(count_in_thread, len(asins) + count_in_thread, count_in_thread)):
                name_of_thread = f"thread_{number + 1}"
                if j == len(asins):
                    thread = ScrapThreadVariations(thread_name=name_of_thread, lock=lock_obj, asins=asins[index_:],
                                                   session=session, header=headers)
                else:
                    thread = ScrapThreadVariations(thread_name=name_of_thread, lock=lock_obj, asins=asins[index_:j],
                                                   session=session, header=headers)
                thread.start()
                threads.append(thread)
                t.sleep(1)
                index_ = j

            for thread in threads:
                thread.join()

            # every thread return a dict with keys as main asins (from best seller pages) and values of these keys as
            # list of asins (from variations of asin)

            df = []
            for thread in threads:
                for thread_asin in thread.return_val:
                    index_ = 0
                    for thread_variations_asin in thread.return_val[thread_asin]:
                        if index_ == 0:
                            df.append([thread_asin, thread_variations_asin])
                        else:
                            df.append(['', thread_variations_asin])
                        index_ += 1

            df = pd.DataFrame(df, columns=['asin from category', 'asin from listing'])
            df.to_excel(writer_excel, sheet_name=f'{data_[subnumber][0]}')
            print(f'\n  ## ended "{data_[subnumber][0]}" subcategory ##\n')

        writer_excel.save()

    # DEFAULT VALUES -------------------------------------------------------------------------------------------

    user_agent_index = 0

    session = requests.Session()
    headers = {
        'authority': 'www.amazon.com',
        'dnt': '1',
        'upgrade-insecure-requests': '1',
        'user-agent': _USER_AGENT_LIST[user_agent_index],
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,'
                  'image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-dest': 'document',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    }
    # ----------------------------------------------------------------------------------------------------------

    r, user_agent_index, headers = get_query(user_agent_index, CHANGE_LOCATION_URL, headers)

    assert r.status_code == 200, f'status code country change problem'

    for i in range(_MAX_TRIAL_REQUESTS):
        r, user_agent_index, headers = get_query(user_agent_index, amazon_url_product(asin), headers,
                                                 change_location=True)
        main_categories = get_mainpage_categories(r.text)
        if 'No amazon category found' not in main_categories:
            break
        else:
            user_agent_index, headers = _change_user_agent(user_agent_index, headers)

    if 'No amazon category found' in main_categories:
        print('sorry, dindt find any category for this product')
        # todo add input field to search new category
        return
    else:
        print('Which categories you wanna look at?')
        main_categories_list = []
        for i, category in enumerate(main_categories):
            main_categories_list.append(category)
            print(f'{i + 1}: {category} ({main_categories[category][0]}) '
                  f'[https://www.amazon.com/bestsellers/{main_categories[category][1]}]')
        # choose categories from main page of main asin
        numbers = input_numbers()

    thread_subcategory = []

    # for each of main page category scratch subcategories, which are really sub or on the same level
    for number_of_category in numbers:

        # title
        category = main_categories_list[number_of_category]
        # writer for excel
        writer = pd.ExcelWriter(f'data/{category}.xlsx', engine='xlsxwriter')

        print('\n', '-' * 100, '\n')
        print(f'For this category "{category}" (https://www.amazon.com/bestsellers/'
              f'{main_categories[category][1]})'
              f'I have found this subcategories:')

        # scrap all subcategories from amazon
        subcategories = scrap_tree_of_categories(f'https://www.amazon.com/bestsellers/{main_categories[category][1]}',
                                                 lambda x: get_query(user_agent_index, x, headers,
                                                                     change_location=True))
        # get max len for printing
        max_len = max([len(i[0]) for i in subcategories])
        for i, (subcategory_title, link, id_node) in enumerate(subcategories):
            print(f'{i + 1}: {subcategory_title: <{max_len + 5}} ({link})')

        # choose subcategories
        subnumbers = input_numbers()
        # create thread for these subcategories and process them consequently
        process = Thread(target=thread_subcategory_asins_find,
                         args=[subnumbers, user_agent_index, headers, writer, subcategories])
        process.start()
        thread_subcategory.append(process)

    for process in thread_subcategory:
        process.join()


# start = t.time()
# scrap_asins_from_category('B07FPVR858')
# print(t.time() - start, 'seconds')

if __name__ == '__main__':
    print('Hi :)')
    start = t.time()
    scrap_asins_from_category(input('Please enter asins here: '))
    print('-'*100)
    print('Program run', round(t.time() - start, 3), 'seconds')
