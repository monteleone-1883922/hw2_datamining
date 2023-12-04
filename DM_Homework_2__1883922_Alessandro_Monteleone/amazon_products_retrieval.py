import requests as r
import lxml as x 
import sys
from fake_useragent import UserAgent
from selenium import webdriver
from bs4 import BeautifulSoup

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


def build_url(keyword,page):
    return URL.format(keyword,page)

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

def get_page(url, headers):

    response = r.get(url, headers=headers)
    # soup = BeautifulSoup(response.text, 'html.parser')
    # pars = soup.find("span", class_="a-size-base-plus a-color-base a-text-normal")

    handle_status_codes(response)

def set_user_agent(user_agent):
    headers = HEADERS
    headers["User-Agent"] = user_agent    
    return headers

def set_random_user_agent(ua):
    return set_user_agent(ua.random)



def test():
    url = build_url(KEYWORD,1)
    ua = UserAgent()
    h = set_random_user_agent(ua)-
    page = get_page(url, h)
    a = 3






if __name__ == "__main__":
    keyword = KEYWORD if len(sys.argv) < 2 else sys.argv[1]
    max_num_pages = MAX_NUM_PAGES if len(sys.argv) < 3 else sys.argv[2]
    test()





