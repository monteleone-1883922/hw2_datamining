
import lxml

URL =  "https://www.amazon.it/s?k={}&page={}"
KEYWORD = "gpu"
MAX_NUM_PAGES = 10
HEADERS = {
        "Host": "www.amazon.it",
        "User-Agent": "{}",       #"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36",
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

def html_find(html_fragment, element, html_class = ""):
     if html_class == "":
        return html_fragment.xpath(f'.//{element}') 
     else :
         return html_fragment.xpath(f'.//{element}[@class="{html_class}"]') 


def handle_status_codes(response):
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


class Product():
    
    def __init__(self,html_product):
        self.description = html_find(html_product, "span",PRODUCT_DESCRIPTION_CLASS )[0].text
        self.price = html_find(html_product, "span",PRODUCT_PRICE )[0].text[:-2]
        self.prime = len(html_find(html_product,"i",PRIME)) != 0
        self.url = "https://www.amazon.it" + html_find(html_product,"a",PRODUCT_PAGE_LINK)[0].get("href")
        self.stars = html_find(html_product, "span", PRODUCT_STARS)[0].text[:3]
        self.num_reviews = html_find(html_product, "span", PRODUCT_REVIEWS)[0].text

    def to_string_tsv(self):
        return self.description + "\t" + self.price + "\t" + str(self.prime) + "\t" + self.url + "\t" + self.stars + "\t" + self.num_reviews
        