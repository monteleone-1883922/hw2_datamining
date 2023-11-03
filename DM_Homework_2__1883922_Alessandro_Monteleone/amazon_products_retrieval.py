import requests as r
import lxml
from lxml import etree
import sys
from fake_useragent import UserAgent
from utils_and_classes import *
import time




def build_url(keyword,page):
    return URL.format(keyword,page)


def get_page(url, headers):

    response = r.get(url, headers=headers)
    # soup = BeautifulSoup(response.text, 'html.parser')
    # pars = soup.find("span", class_="a-size-base-plus a-color-base a-text-normal")

    handle_status_codes(response)
    return response

def set_user_agent(user_agent):
    headers = HEADERS
    headers["User-Agent"] = user_agent    
    return headers

def set_random_user_agent(ua):
    return set_user_agent(ua.random)




def test():
   pass
        

def store_amazon_products():
    ua = UserAgent()
    with open("amazon_products_gpu.tsv","w") as file:
        file.write("description\tprice\tprime\turl\tstars\tnum_reviews\n")
        for i in range(MAX_NUM_PAGES):
            print("getting page ", i+1)
            url = build_url(KEYWORD,i+1)
            headers = set_random_user_agent(ua)
            page = get_page(url, headers)
            tree = etree.HTML(page.text)
            products = html_find(tree,"div",PRODUCTS_CLASS) + html_find(tree,"div",PRODUCTS_AD_CLASS)
            for product_html in products:
                product = Product(product_html)
                file.write(product.to_string_tsv() + "\n")
            time.sleep(20)







if __name__ == "__main__":
    keyword = KEYWORD if len(sys.argv) < 2 else sys.argv[1]
    max_num_pages = MAX_NUM_PAGES if len(sys.argv) < 3 else sys.argv[2]
    test()





