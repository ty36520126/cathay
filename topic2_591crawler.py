import os
import time
import random
from typing import List , Optional
from urllib.parse import urlparse, parse_qs
import typer
import joblib
from bs4 import BeautifulSoup
import re
import shutil
import logging
from datetime import date
import pandas as pd
from tqdm import tqdm
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    before_sleep_log,
    RetryError,
    retry_if_exception_type,
)

#================================================================
#                           爬取所有ids
#================================================================
def get_total_pages(url):
    browser = webdriver.Chrome()
    browser.get(url)
    #關閉選取地區pop-up 否則無法點選下一頁
    #     browser.find_element_by_id('area-box-close').click()
    #     area = browser.find_element_by_id("area-box-close")
    #     area.click()  # 取消「選擇縣市」的div視窗
    time.sleep(random.uniform(5, 6))
    #輸入 ESC 關閉google 提示，否則無法點選
#     browser.find_element_by_class_name('pageNext').send_keys(Keys.ESCAPE) #ECS鍵


    bs = BeautifulSoup(browser.page_source, 'html.parser')
    totalpages = int(bs.find('span', {'class':'TotalRecord'}).text.split(' ')[-2])/30 + 1
    print('Total pages: ', totalpages)
    print(int(totalpages))
    return(int(totalpages))

def get_id_list(
    URL, max_pages: int = 2, quiet: bool = False
):
    try:
        region = parse_qs(urlparse(URL).query)["region"][0]
    except AttributeError as e:
        print("The URL must have a 'region' query argument!")
        raise e
    options = webdriver.ChromeOptions()
    if quiet:
        options.add_argument("headless")
    browser = webdriver.Chrome(options=options)
    browser.get(URL)
    try:
        browser.find_element_by_css_selector(f'dd[data-id="{region}"]').click()
    except NoSuchElementException:
        pass
    time.sleep(2)
    
    listings: List[str] = []
    for i in range(max_pages):
        print(f"Page {i+1}")
        soup = BeautifulSoup(browser.page_source, "lxml")
        for item in soup.find_all("section", attrs={"class": "vue-list-rent-item"}):
            link = item.find("a")
            listings.append(link.attrs["href"].split("-")[-1].split(".")[0])

        browser.find_element_by_class_name("pageNext").click()
        time.sleep(random.uniform(4, 5))
        try:
            browser.find_element_by_css_selector("a.last")
            break
        except NoSuchElementException:
            pass
    print(len(set(listings)))
    print(f"Done! Collected {len(listings)} entries.")
    return(listings)

URL1 = "https://rent.591.com.tw/?kind=0&region=1"
taipei_total = get_total_pages(URL1)
taipei_id_list = get_id_list(URL=URL1, max_pages=taipei_total)

URL3 = "https://rent.591.com.tw/?kind=0&region=3"
newtaipei_total = get_total_pages(URL3)
newtaipei_id_list = get_id_list(URL=URL3, max_pages=newtaipei_total)

#================================================================
#                讀入前一日所儲存的id並存入當日新id
#================================================================

import json
with open('ids.json', 'r') as fp:
    ids_org = json.load(fp)
    
all_org = ids_org['all_ids']
taipei_org = ids_org['taipei_ids']
newtaipei_org = ids_org['newtaipei_ids']

total_id_list = taipei_id_list + newtaipei_id_list
id_dict = {}
id_dict['all_ids'] = total_id_list
id_dict['taipei_ids'] = taipei_id_list
id_dict['newtaipei_ids'] = newtaipei_id_list

with open('ids.json', 'w') as ids:
    json.dump(id_dict, ids)




#================================================================
#                           開始爬取資料
#================================================================

def parse_price(price_str: str) -> int:
    if price_str == "" or "--" in price_str or "無" in price_str:
        return 0
    return int(re.match(r"^([\d,]+)\w+", price_str).group(1).replace(",", ""))
    return df

LOGGER = logging.getLogger(__name__)


class NotExistException(Exception):
    pass


def get_attributes(soup):
    result = {}
    poster = re.sub(r"\s+", " ", soup.select_one("p.name").text.strip())
    x = poster.replace(" ","").split(":", 1)
    result["出租者"] = x[1]
    result["出租者身分"] = x[0] 
    
    try:
        result["養寵物"] = (
            "No" if "不可養寵物" in soup.select_one("div.service-rule").text else "Yes"
        )
    except AttributeError:
        result["養寵物"] = None
    
    try:
        result["性別要求"] = (
            "限女生租住" if "限女生租住" in soup.select_one("div.service-rule").text else 
            "限男生租住" if "限男生租住" in soup.select_one("div.service-rule").text else
            "男女皆可租住"
        )
    except AttributeError:
        result["性別要求"] = None
    try:    
        contents = soup.select_one("div.main-info-left div.content").children
        for item in contents:
            try:
                name = item.select_one("div div.name").text
                if name in ("管理費"):
                    result[name] = name = item.select_one("div div.text").text.strip()
            except AttributeError as e:
                print(e)
                continue
            
    except:
        print('管理費None')
        result["管理費"] = None
    
    try:
        service_list = soup.select_one("div.service-list-box").select(
            "div.service-list-item"
        )
        services = []
        for item in service_list:
            if "del" in item["class"]:
                continue
            services.append(item.text.strip())
        result["提供設備"] = ", ".join(services)
    except:
        print('提供設備None')
        result["提供設備"] = None
    
    attributes = soup.select_one("div.house-pattern").find_all("span")
    for i, key in enumerate(("格局", "坪數", "樓層", "型態")):
        result[key] = attributes[i * 2].text.strip()
    return result


@retry(
    reraise=False,
    retry=retry_if_exception_type(TimeoutException),
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    before_sleep=before_sleep_log(LOGGER, logging.INFO),
)
def get_page(browser: webdriver.Chrome, listing_id):
    browser.get(f"https://rent.591.com.tw/home/{listing_id}".strip())
    wait = WebDriverWait(browser, 5)
    try:
        wait.until(
            ec.visibility_of_element_located((By.CSS_SELECTOR, "div.main-info-left"))
        )
    except TimeoutException as e:
        soup = BeautifulSoup(browser.page_source, "lxml")
        tmp = soup.select_one("div.title")
        # print(tmp)
        if tmp and "不存在" in tmp.text:
            raise NotExistException()
        else:
            raise e
    return True


def get_listing_info(browser: webdriver.Chrome, listing_id):
    try:
        get_page(browser, listing_id)
    except RetryError:
        pass
    soup = BeautifulSoup(browser.page_source, "lxml")
    result = {"id": listing_id}
    result["標題"] = soup.select_one(".house-title h1").text
    result["地址"] = soup.select_one("span.load-map").text.strip()
    complex = soup.select_one("div.address span").text.strip()
    if complex != result["地址"]:
        result["社區"] = complex
    result["價格"] = parse_price(soup.select_one("span.price").text)
    try:
        result["聯絡電話"] = soup.select("span.tel-txt")[-1].text
    except:
        result["聯絡電話"] = None
    result["poster"] = re.sub(r"\s+", " ", soup.select_one("p.name").text.strip())
    result.update(get_attributes(soup))
    return result


def main(
    city: str ,
    ids:list ,
    ids_org:list ,
    limit: int = -1,
    headless: bool = False

):
    listing_ids = ids
    
    listing_ids = list(set(listing_ids) - set(ids_org))
    print(len(listing_ids))
    
    if limit > 0:
        listing_ids = listing_ids[:limit]

    print(f"Collecting {len(listing_ids)} entries...")

    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("headless")
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)
    browser = webdriver.Chrome(options=options)

    data = []
    for id_ in tqdm(listing_ids, ncols=100):
        try:
            data.append(get_listing_info(browser, id_))
        except NotExistException:
            LOGGER.warning(f"Does not exist: {id_}")
            pass
        time.sleep(random.uniform(2.5, 3)) #random.random() * 5

    df_new = pd.DataFrame(data)

    df_new["爬取日"] = date.today().isoformat()
    df_new["縣市"] = city

    df_new["網頁連結"] = (
        "https://rent.591.com.tw/rent-detail-" + df_new["id"].astype("str") + ".html"
    )
    column_ordering = [
        "id",
        "縣市",
        "標題",
        "出租者",
        "出租者身分",
        "聯絡電話",
        "地址",
        "社區",
        "價格",
        "管理費",
        "養寵物",
        "性別要求",
        "提供設備",
        "格局",
        "坪數",
        "樓層",
        "型態",
        "爬取日",
        "網頁連結"
    ]

    print(df_new.columns)
    print("Finished!")
    return(df_new[column_ordering])


taipei_data = main(city="台北市", ids=taipei_id_list, ids_org=taipei_org) 
taipei_data = taipei_data.fillna("無")
taipei_data_json = taipei_data.to_dict('records')

newtaipei_data = main(city="新北市", ids=newtaipei_id_list, ids_org=newtaipei_org) 
newtaipei_data = newtaipei_data.fillna("無")
newtaipei_data_json = newtaipei_data.to_dict('records')

#================================================================
#                        連線mongodb並存入資料
#================================================================

import pymongo
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['cathay']
collection = db['rent591']
collection.insert_many(taipei_data_json)
collection.insert_many(newtaipei_data_json)