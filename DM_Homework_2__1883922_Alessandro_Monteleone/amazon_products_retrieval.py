import requests as r
import lxml
from lxml import etree
import sys
from fake_useragent import UserAgent
from utils_and_classes import *
import time
import warnings
from typing import *




def build_url(keyword : str,page : str) -> str:
    return URL.format(keyword,page)


def get_page(url : str, headers : Dict[str,str]) -> r.Response:

    response = r.get(url, headers=headers)
    handle_status_codes(response)
    return response

def set_user_agent(user_agent : str) -> Dict[str,str]:
    headers = HEADERS
    headers["User-Agent"] = user_agent    
    return headers

def set_random_user_agent(ua : UserAgent) -> Dict[str,str]:
    return set_user_agent(ua.random)


def test():
   pass
        
def retrieve_page_products(start_page : int,store_file, ua : UserAgent, data_info : DataInfo):
    while start_page < MAX_NUM_PAGES:
        headers = set_random_user_agent(ua)
        url = build_url(KEYWORD,start_page+1)
        print("getting page ", start_page+1)
        try:
            page = get_page(url, headers)
            tree = etree.HTML(page.text)
            products = html_find(tree,"div",PRODUCTS_CLASS) + html_find(tree,"div",PRODUCTS_AD_CLASS)
            for product_html in products:
                product = Product(product_html,data_info)
                store_file.write(product.to_string_tsv() + "\n")
            print("start sleeping")
            time.sleep(100)
            print("wake up")
            start_page += 1
        except:
            retrieve_page_products(start_page,store_file,ua,data_info)
       
        
# with open("test.html","w") as f:
#                 f.write(lxml.etree.tostring(tree, pretty_print=True, encoding='unicode'))


def store_amazon_products():
    
    warnings.filterwarnings('ignore', category=UserWarning, module='fake_useragent')
    ua = UserAgent()
    
    data_info = DataInfo()
    with open("DM_Homework_2__1883922_Alessandro_Monteleone/amazon_products_gpu.tsv","w") as file:
        file.write("description\tprice\tprime\turl\tstars\tnum_reviews\n")
        retrieve_page_products(0,file,ua,data_info)
    warnings.filterwarnings('default', category=UserWarning, module='fake_useragent')
    print(data_info)







if __name__ == "__main__":
    keyword = KEYWORD if len(sys.argv) < 2 else sys.argv[1]
    max_num_pages = MAX_NUM_PAGES if len(sys.argv) < 3 else sys.argv[2]
    store_amazon_products()





