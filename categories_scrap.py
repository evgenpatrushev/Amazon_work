import requests
from time import time
import numpy as np
import pandas as pd

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


def scrap_tree_of_categories(url, get_request, path=''):
    """
    Scrap list of subcategories from amazon page of category
    :param path:
    :param url: link to category
    :param get_request: function for getting request
    :return: list of lists [..., [title, link, id], ...]
    """

    def scrap_subcategories(ul_obj):
        return_val = []
        for li_i in ul_obj.find_all('li'):
            if li_i.find_all('a'):
                link = li_i.find_all('a')[0]['href']
                link = link[:link.index('/ref=')]
                # s = ''
                # for index in range(len(link)):
                #     if link[len(link) - 1 - index].isdigit():
                #         s = link[len(link) - 1 - index] + s
                #     else:
                #         break
                # # id_node = int(s)
                # link = f'https://www.amazon.com/bestsellers/zgbs/{s}'
            else:
                link = url
                # id_node = url.split('/')[-1]
            return_val.append([li_i.text.strip(), link])
        return return_val

    tree = []
    links_dict = {}
    if path == '':
        root = True
    else:
        root = False

    r = get_request(url)

    btf = BeautifulSoup(r.text)
    if len(btf.find_all('span', attrs={'class': 'zg_selected'})) > 1:
        raise Exception('why more than 1 ((( ')

    li = btf.find_all('span', attrs={'class': 'zg_selected'})[0].parent
    category_title = li.text.strip()
    ul = li.parent

    root_path = ''
    parent = ul.parent
    while True:
        if parent.find('li').find('a').text.strip() != 'Any Department':
            root_path = parent.find('li').find('a').text.strip() + '/' + root_path
            parent = parent.parent
        else:
            root_path = parent.find('li').find('a').text.strip() + '/' + root_path
            root_path = root_path[:-1]
            break

    if path != '' and root_path != path:
        return '', {}

    if ul.find_all('ul') or path == '':
        if ul.find_all('ul'):
            ul = ul.find_all('ul')[0]
            links_dict[category_title] = url
            path = root_path + '/' + category_title
        elif path == '':
            path = root_path
        for li in scrap_subcategories(ul):
            branch, links = scrap_tree_of_categories(li[1], get_request, path=path)
            if type(branch) is str:
                if branch == '':
                    tree.append(path + '/' + li[0])
                    links[li[0]] = li[1]
                else:
                    tree.append(branch)
            else:
                [tree.append(i) for i in branch]
            links_dict.update(links)

        if root:
            # links_dict[category_title] = url
            df = pd.DataFrame([branch[len(root_path) + 1:].split('/') for branch in tree])
            df['new'] = None
            df = df.applymap(lambda x: f'=HYPERLINK("{links_dict[x]}", "{x}")' if x is not None else None)
            df.replace(to_replace=[None], value='', inplace=True)
            df = df.set_index(list(df.columns[:-1]))
            df = df.sort_index()
            return df, pd.DataFrame.from_dict(links_dict, orient='index')
        return tree, links_dict

    else:
        return path + '/' + category_title, {category_title: url}


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


session = requests.Session()
session.headers.update(headers)

start_time = time()
# a = scrap_tree_of_categories('https://www.amazon.com/Best-Sellers/zgbs/wireless/ref=zg_bs_unv_1_2407749011_1',
#                              query)

data_frame, links_df = scrap_tree_of_categories(
    'https://www.amazon.com/Best-Sellers-Carrier-Cell-Phones/zgbs/wireless/2407748011/ref=zg_bs_nav_1_wireless',
    query)

writer = pd.ExcelWriter(f'data/categories.xlsx', engine='xlsxwriter')
data_frame.to_excel(writer, sheet_name='tree', header=False)
links_df.to_excel(writer, sheet_name='links', header=False)

worksheet = writer.sheets['tree']
worksheet.set_column(0, 100, 40)

worksheet = writer.sheets['links']
worksheet.set_column(0, 0, 40)
worksheet.set_column(1, 1, 150)

writer.save()

print(time() - start_time, 'seconds')
