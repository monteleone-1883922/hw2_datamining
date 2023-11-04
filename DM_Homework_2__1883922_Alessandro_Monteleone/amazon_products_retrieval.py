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



#ASUS TUF Gaming NVIDIA GeForice RTX 3070 V2 OC Edition Scheda Grafica, 8GB GDDR6, PCIe 4.0, HDMI 2.1, DisplayPort 1.4a, GPU Tweak II, Componenti di Livello Militare, LHR
def test():
   pass
        

def store_amazon_products():
    
    warnings.filterwarnings('ignore', category=UserWarning, module='fake_useragent')
    ua = UserAgent()
    headers = set_random_user_agent(ua)
    data_info = DataInfo()
    with open("DM_Homework_2__1883922_Alessandro_Monteleone/amazon_products_gpu.tsv","w") as file:
        file.write("description\tprice\tprime\turl\tstars\tnum_reviews\n")
        for i in range(MAX_NUM_PAGES):
            print("getting page ", i+1)
            url = build_url(KEYWORD,i+1)
            
            page = get_page(url, headers)
            tree = etree.HTML(page.text)
            with open("test.html","w") as f:
                f.write(lxml.etree.tostring(tree, pretty_print=True, encoding='unicode'))
            products = html_find(tree,"div",PRODUCTS_CLASS) + html_find(tree,"div",PRODUCTS_AD_CLASS)
            for product_html in products:
                product = Product(product_html,data_info)
                file.write(product.to_string_tsv() + "\n")
            time.sleep(40)
    warnings.filterwarnings('default', category=UserWarning, module='fake_useragent')







if __name__ == "__main__":
    keyword = KEYWORD if len(sys.argv) < 2 else sys.argv[1]
    max_num_pages = MAX_NUM_PAGES if len(sys.argv) < 3 else sys.argv[2]
    store_amazon_products()





