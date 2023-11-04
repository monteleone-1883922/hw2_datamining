
import lxml
from typing import *
from requests import Response

URL =  "https://www.amazon.it/s?k={}&page={}"
KEYWORD = "gpu"
MAX_NUM_PAGES = 10
HEADERS = {
        "Host": "www.amazon.it",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36",#"{}",       #"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "it-IT,it;q=0.8,en-US;q=0.5,en;q=0.3",
        "Connection": "close",
        "Cache-Control": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",

    }
PRODUCTS_AD_CLASS = "sg-col-4-of-24 sg-col-4-of-12 s-result-item s-asin sg-col-4-of-16 AdHolder sg-col s-widget-spacing-small sg-col-4-of-20"
PRODUCTS_CLASS = "sg-col-4-of-24 sg-col-4-of-12 s-result-item s-asin sg-col-4-of-16 sg-col s-widget-spacing-small sg-col-4-of-20"
PRODUCT_DESCRIPTION_CLASS = "a-size-base-plus a-color-base a-text-normal"
PRODUCT_PRICE = "a-offscreen"
PRIME = "a-icon a-icon-prime a-icon-medium"
PRODUCT_PAGE_LINK = "a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal"
PRODUCT_STARS = "a-icon-alt"
PRODUCT_REVIEWS = "a-size-base s-underline-text"

def html_find(html_fragment, element : str, html_class : str = ""):
     if html_class == "":
        return html_fragment.xpath(f'.//{element}') 
     else :
         return html_fragment.xpath(f'.//{element}[@class="{html_class}"]') 


def handle_status_codes(response : Response) -> None:
    if response.status_code == 200:
        print("The request was successfully processed (HTTP 200 OK)")
    elif response.status_code == 204:
        print("The request was successfully processed, but no data is returned (HTTP 204 No Content)")
        exit(1)
    elif response.status_code == 400:
        print("Bad request (HTTP 400 Bad Request)")
        exit(1)
    elif response.status_code == 401:
        print("Unauthorized (HTTP 401 Unauthorized)")
        exit(1)
    elif response.status_code == 403:
        print("Access forbidden (HTTP 403 Forbidden)")
        exit(1)
    elif response.status_code == 404:
        print("Resource not found (HTTP 404 Not Found)")
        exit(1)
    elif response.status_code == 500:
        print("Internal server error (HTTP 500 Internal Server Error)")
        exit(1)
    elif response.status_code == 503:
        print("Service unavaliable or request refused (HTTP 503 Unavailable)")
        exit(1)
    else:
        print("Unknown status code: ", response.status_code)
        exit(1)


class DataInfo():

    def __init__(self):
        self.description_errors = 0
        self.prime_errors = 0
        self.price_errors = 0
        self.stars_errors = 0
        self.num_reviews_errors = 0
        self.url_errors = 0
        self.num_products = 0

    def __str__(self) -> str:
        return "total number of products = " + str(self.num_products) + \
            "\nERRORS\n" + \
            "description errors = " + str(self.description_errors) + \
            "prime errors = " + str(self.prime_errors) + \
            "price errors = " + str(self.price_errors) + \
            "stars errors = " + str(self.stars_errors) + \
            "number of reviews errors = " + str(self.num_reviews_errors) + \
            "url errors = " + str(self.url_errors)
    



class Product():
    
    def __init__(self,html_product, data_info : DataInfo):

        data_info.num_products += 1
        
        self.description = self.get_field_handle_errors("description", html_product, "span",PRODUCT_DESCRIPTION_CLASS )
        self.price = self.get_field_handle_errors("price",html_product, "span",PRODUCT_PRICE )
        self.prime = self.get_field_handle_errors("prime",html_product,"i",PRIME)
        self.url = self.get_field_handle_errors("url",html_product,"a",PRODUCT_PAGE_LINK)
        self.stars = self.get_field_handle_errors("stars",html_product, "span", PRODUCT_STARS)
        self.num_reviews = self.get_field_handle_errors("num_reviews",html_product, "span", PRODUCT_REVIEWS)
    

    def to_string_tsv(self) -> str:
        return self.description + "\t" + self.price + "\t" + str(self.prime) + "\t" + self.url + "\t" + self.stars + "\t" + self.num_reviews
        
    def get_field_handle_errors(self,field : str,html_product,element : str, html_class : str, data_info : DataInfo) -> str:
        try:
            if field == "description" or field == "num_reviews":
                result = html_find(html_product,element, html_class)[0].text
            elif field == "price":
                result = html_find(html_product, "span",PRODUCT_PRICE )[0].text[:-2]
            elif field == "prime":
                result = len(html_find(html_product,"i",PRIME)) != 0
            elif field == "url":
                result = "https://www.amazon.it" + html_find(html_product,"a",PRODUCT_PAGE_LINK)[0].get("href")
            elif field == "stars":
                result = html_find(html_product, "span", PRODUCT_STARS)[0].text[:3]
        except:
            result = ""
            if field == "description":
                data_info.description_errors += 1
            elif field == "price":
                data_info.price_errors += 1
            elif field == "prime":
                data_info.prime_errors += 1
            elif field == "url":
                data_info.url_errors += 1
            elif field == "stars":
                data_info.stars_errors += 1
            elif field == "num_reviews":
                data_info.num_reviews_errors += 1
        return result