import json

from nltk.tokenize import word_tokenize
from requests import Response

URL = "https://www.amazon.it/s?k={}&page={}"
KEYWORD = "gpu"
MAX_NUM_PAGES = 10
HEADERS = {
    "Host": "www.amazon.it",
    "User-Agent": "{}",
    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "it-IT,it;q=0.8,en-US;q=0.5,en;q=0.3",
    "Connection": "close",
    # "Cache-Control": "no-cache",
    # "Sec-Fetch-Dest": "document",
    # "Sec-Fetch-Mode": "navigate",
    # "Sec-Fetch-Site": "none",
    # "Sec-Fetch-User": "?1",

}
PRODUCTS_AD_CLASS = "sg-col-4-of-24 sg-col-4-of-12 s-result-item s-asin sg-col-4-of-16 AdHolder sg-col s-widget-spacing-small sg-col-4-of-20"
PRODUCTS_CLASS = "sg-col-4-of-24 sg-col-4-of-12 s-result-item s-asin sg-col-4-of-16 sg-col s-widget-spacing-small sg-col-4-of-20"
PRODUCT_DESCRIPTION_CLASS = "a-size-base-plus a-color-base a-text-normal"
PRODUCT_PRICE = "a-offscreen"
PRIME = "a-icon a-icon-prime a-icon-medium"
PRODUCT_PAGE_LINK = "a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal"
PRODUCT_STARS = "a-icon-alt"
PRODUCT_REVIEWS = "a-size-base s-underline-text"
STOPWORDS_FILE_PATH = "data/stopwords_list_it.json"
SPECIAL_CHARACTERS_FILE_PATH = "data/special_characters.json"
INDEX_FILE_PATH = "data/indexes.json"
CATEGORIES = ['4090', '4080', '7900', '4070', '3090', '6950', '6900', '3080', '6800', '3070', '6850', '2080', '6700',
              '4060', '3060', '6650', '7600', '6600', '1080', '2070', '4050', '5700', '2060', '1070', '3050', '1660',
              '680', '980', '590', '660', '6500', '6550', '2060', '580', '1650', '5500', '5600', '6450', '2050', '1060',
              '480', '970', '390', '570', '780', '290', '470', '280', '960', '380', '280', '290', '5300', '1050', '950',
              '270', '560']
MAX_NUM_RESULTS_FOR_QUERY = 10
DATA_FILE_PATH = "data/amazon_products_gpu.tsv"


def html_find(html_fragment, element: str, html_class: str = ""):
    if html_class == "":
        return html_fragment.xpath(f'.//{element}')
    else:
        return html_fragment.xpath(f'.//{element}[@class="{html_class}"]')


def handle_status_codes(response: Response) -> None:
    if response.status_code == 200:
        print("The request was successfully processed (HTTP 200 OK)")
    elif response.status_code == 204:
        print("The request was successfully processed, but no data is returned (HTTP 204 No Content)")
        raise Exception()
    elif response.status_code == 400:
        print("Bad request (HTTP 400 Bad Request)")
        raise Exception()
    elif response.status_code == 401:
        print("Unauthorized (HTTP 401 Unauthorized)")
        raise Exception()
    elif response.status_code == 403:
        print("Access forbidden (HTTP 403 Forbidden)")
        raise Exception()
    elif response.status_code == 404:
        print("Resource not found (HTTP 404 Not Found)")
        raise Exception()
    elif response.status_code == 500:
        print("Internal server error (HTTP 500 Internal Server Error)")
        raise Exception()
    elif response.status_code == 503:
        print("Service unavaliable or request refused (HTTP 503 Unavailable)")
        raise Exception()
    else:
        print("Unknown status code: ", response.status_code)
        raise Exception()


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
            "description errors = " + str(self.description_errors) + "\n" + \
            "prime errors = " + str(self.prime_errors) + "\n" + \
            "price errors = " + str(self.price_errors) + "\n" + \
            "stars errors = " + str(self.stars_errors) + "\n" + \
            "number of reviews errors = " + str(self.num_reviews_errors) + "\n" + \
            "url errors = " + str(self.url_errors) + "\n"


class Product():

    def __init__(self, html_product, data_info: DataInfo):

        data_info.num_products += 1

        self.description = self.get_field_handle_errors("description", html_product, "span", PRODUCT_DESCRIPTION_CLASS,
                                                        data_info)
        self.price = self.get_field_handle_errors("price", html_product, "span", PRODUCT_PRICE, data_info)
        self.prime = self.get_field_handle_errors("prime", html_product, "i", PRIME, data_info)
        self.url = self.get_field_handle_errors("url", html_product, "a", PRODUCT_PAGE_LINK, data_info)
        self.stars = self.get_field_handle_errors("stars", html_product, "span", PRODUCT_STARS, data_info)
        self.num_reviews = self.get_field_handle_errors("num_reviews", html_product, "span", PRODUCT_REVIEWS, data_info)

    def to_string_tsv(self) -> str:
        return self.description + "\t" + self.price + "\t" + str(
            self.prime) + "\t" + self.url + "\t" + self.stars + "\t" + self.num_reviews

    def get_field_handle_errors(self, field: str, html_product, element: str, html_class: str,
                                data_info: DataInfo) -> str:
        try:
            if field == "description" or field == "num_reviews":
                result = html_find(html_product, element, html_class)[0].text
            elif field == "price":
                result = html_find(html_product, "span", PRODUCT_PRICE)[0].text[:-2]
            elif field == "prime":
                result = len(html_find(html_product, "i", PRIME)) != 0
            elif field == "url":
                result = "https://www.amazon.it" + html_find(html_product, "a", PRODUCT_PAGE_LINK)[0].get("href")
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


class SentencePreprocessing():

    def __init__(self, stopwords_file_path: str, special_characters_file_path: str):
        # nltk.download('punkt')
        with open(stopwords_file_path, 'r') as stopwords_file:
            data = json.load(stopwords_file)
        self.stopwords = set(data["words"])
        with open(special_characters_file_path, 'r') as special_characters_file:
            data = json.load(special_characters_file)
        self.special_characters = set(data["special_characters"])

    def remove_stopwords(self, words: list[str]) -> list[str]:
        result = []
        for word in words:
            if word.lower() not in self.stopwords and word not in self.special_characters:
                result.append(word.lower())
        return result

    def remove_special_characters(self, words: list[str]) -> list[str]:
        result = []
        for word in words:
            if word not in self.special_characters:
                result.append(word.lower())
        return result

    def preprocess(self, sentence: str, remove_stopwords: bool = True) -> list[str]:
        tokenized = word_tokenize(sentence)
        return self.remove_stopwords(tokenized) if remove_stopwords else self.remove_special_characters(tokenized)


