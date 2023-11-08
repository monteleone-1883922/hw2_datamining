import sys
import time
import warnings
from typing import *

import requests as r
from fake_useragent import UserAgent
from lxml import etree

from utils_and_classes import *


def build_url(keyword: str, page: str) -> str:
    return URL.format(keyword, page)


def get_page(url: str, headers: Dict[str, str]) -> r.Response:
    response = r.get(url, headers=headers)
    handle_status_codes(response)
    return response


def set_user_agent(user_agent: str) -> Dict[str, str]:
    headers = HEADERS
    headers["User-Agent"] = user_agent
    return headers


def set_random_user_agent(ua: UserAgent) -> Dict[str, str]:
    return set_user_agent(ua.random)


def test():
    pass


def retrieve_page_products(start_page: int, store_file, ua: UserAgent, data_info: DataInfo):
    try:
        while start_page < MAX_NUM_PAGES:
            headers = set_random_user_agent(ua)
            url = build_url(KEYWORD, start_page + 1)
            print("getting page ", start_page + 1)

            page = get_page(url, headers)
            tree = etree.HTML(page.text)
            products = html_find(tree, "div", PRODUCTS_CLASS) + html_find(tree, "div", PRODUCTS_AD_CLASS)
            for product_html in products:
                product = Product(product_html, data_info)
                store_file.write(product.to_string_tsv() + "\n")
            print("start sleeping")
            time.sleep(100)
            print("wake up")
            start_page += 1
    except Exception as e:
        retrieve_page_products(start_page, store_file, ua, data_info)


def store_amazon_products():
    warnings.filterwarnings('ignore', category=UserWarning, module='fake_useragent')
    ua = UserAgent()

    data_info = DataInfo()
    with open("../data/amazon_products_gpu.tsv", "w") as file:
        file.write("description\tprice\tprime\turl\tstars\tnum_reviews\n")
        retrieve_page_products(0, file, ua, data_info)
    warnings.filterwarnings('default', category=UserWarning, module='fake_useragent')
    print(data_info)




if __name__ == "__main__":
    keyword = KEYWORD if len(sys.argv) < 2 else sys.argv[1]
    max_num_pages = MAX_NUM_PAGES if len(sys.argv) < 3 else sys.argv[2]
    store_amazon_products()
