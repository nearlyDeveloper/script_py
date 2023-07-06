# -*- coding: utf8 -*-
import datetime
import time
import re
from random import randrange
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import json
import xlsxwriter
import requests

def main(url_of_group):
    tg = datetime.datetime.now()
    prices = []
    name_shop = []
    s = []
    data = []
    url_mappings = {
        "https://catalog.onliner.by/office_chair": ("office_chair", "Офисные_кресла_и_стулья"),
        "https://catalog.onliner.by/table": ("table", "Письменные_и_компьютерные_столы"),
        "https://catalog.onliner.by/chair": ("chair", "Стулья_для_кухни_и_бара"),
        "https://catalog.onliner.by/kidsdesk": ("kidsdesk", "Детские_парты,_столы,_стулья"),
        "https://catalog.onliner.by/gardenfurniture": ("gardenfurniture", "Садовая_мебель"),
        "https://catalog.onliner.by/divan": ("divan", "Диваны"),
        "https://catalog.onliner.by/interior_chair": ("interior_chair", "Кресла"),
        "https://catalog.onliner.by/kitchen_table": ("kitchen_table", "Кухонные столы и обеденные группы")
    }
    ber, rt = url_mappings.get(url_of_group, (None, None))

    w = 0
    dr = ""
    hj = ""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    chrome_driver_path = "C:/Users/Bartosh/Desktop/Main files/script_py/chromedriver.exe"
    # chrome_driver_path = "D:/epsxe/script_py/chromedriver.exe"
    driver = webdriver.Chrome(executable_path=chrome_driver_path)

    with driver as driver:
        for i in range(1, 2):
            req = requests.get(url=f"https://catalog.onliner.by/sdapi/catalog.api/search/{ber}?group=1&page={i}")
            jso = json.loads(req.text)
            for j in range(0, 30):
                from_selenium = jso['products'][j]['html_url']
                name = jso['products'][j]['full_name']
                html = jso['products'][j]['html_url'].split('/')[-1]
                r = requests.get(
                    url=f"https://catalog.onliner.by/sdapi/shop.api/products/{html}/positions?town=all")
                json_data = json.loads(r.text)
                driver.get(from_selenium + "/prices")
                # print(quantity)
                try: 
                # Ждем, пока кнопка станет видимой
                    button = WebDriverWait(driver, 1.7).until(EC.visibility_of_element_located((By.CLASS_NAME, "button-style_specific")))
                    # Используйте метод execute_script для выполнения JavaScript кода и клика по кнопке
                    driver.execute_script("arguments[0].click();", button)
                except TimeoutException:
                    pass
                # time.sleep(1.7) 
                a = driver.find_elements(By.CLASS_NAME,"offers-list__description_nowrap")
                price_element = driver.find_element(By.CLASS_NAME, "offers-description__price")
                if driver.find_elements(By.CLASS_NAME, "offers-description__price_secondary"):
                    secondary_price_element = driver.find_element(By.CLASS_NAME, "offers-description__price_secondary")
                    price_list = [secondary_price_element]
                else:
                    price_list = [price_element]             
                min_price = None              
                for price in price_list:
                        try:
                            price_value = price.get_attribute("textContent").strip().replace(' р.','').replace(',', '.')
                            if min_price is None or float(price_value) < float(min_price):
                                min_price = price_value
                        except ValueError:
                                pass
                        try:
                            saved_price = None
                            delivery_price_offers = driver.find_element(By.CLASS_NAME, "offers-list__item")
                            delivery_price_shop = driver.find_element(By.CLASS_NAME, "offers-list__shop")
                            delivery_price_link = delivery_price_shop.find_element(By.CSS_SELECTOR, "a")
                            if "https://5410.shop.onliner.by/" in delivery_price_link.get_attribute("href"):
                                if "под заказ" in delivery_price_offers.text:
                                        price_element = delivery_price_offers.find_element(By.CLASS_NAME, "offers-list__description")
                                        if "Onlíner Pay" in price_element.text:
                                            price_element = delivery_price_offers.find_element(By.CLASS_NAME, "offers-list__description_alter-other")
                                            price_value = price_element.text.strip()
                                            saved_price = price_value
                                        else:
                                            price_value = price_element.text.strip()
                                            saved_price = price_value
                                else:
                                        price_element = delivery_price_offers.find_element(By.CLASS_NAME, "offers-list__description")
                                        if "Onlíner Pay" in price_element.text:
                                            price_element = delivery_price_offers.find_element(By.CLASS_NAME, "offers-list__description_alter-other")
                                            price_value = price_element.text.strip()
                                            saved_price = price_value
                        except ValueError:
                                pass
                        if saved_price is not None:
                            saved_delivery = "Под заказ"
                        else:
                            saved_price = "Нашего предложения нет"
                            saved_delivery = " "
                time = None 
                for o in a:
                    if len(o.text.split()) == 4 and o.text.split()[0] == '—':
                        del o.text.split()[0]
                        del o.text.split()[1]
                        s.append(''.join(o.text.split()[2:3]).replace(',', ''))
                #print(s)
                for k in range(0, len(s)):
                    if not json_data["shops"][str(json_data["positions"]["primary"][k]["shop_id"])]["title"] == "KingStyle":
                        prices.append(json_data['positions']['primary'][k]['position_price']['amount'])
                        name_shop.append(
                            json_data["shops"][str(json_data["positions"]["primary"][k]["shop_id"])]["title"])
                    else:
                        dr = json_data['positions']['primary'][k]['position_price']['amount']                       
                        hj = s[k]
                        w = 1
                        if len(s) < 2:
                            s = []
                        else:
                            del s[k]
                # print(json_data)
                if w == 1:
                    na = "Есть в KingStyle"
                else:
                    na = "Нет в KingStyle"
                if len(s) > 0:
                    if len(s) == 1:
                        if saved_price == "Нашего предложения нет":
                            item = {
                                    'name': name,
                                    'price': min_price,
                                    'delivery': "Доставка " + ', '.join(s) + " д.",
                                    'price_King': saved_price,
                                    'delivery_King': " ",
                                    'name_shop': ', '.join(name_shop),
                                    'url': from_selenium + "/prices",
                                    'nal': "Нет в KingStyle"
                                }
                        else:
                            item = {
                                'name': name,
                                'price': min_price,
                                'delivery': "Доставка " + ', '.join(s) + " д.",
                                'price_King': saved_price,
                                'delivery_King': 'Доставка ' + str(s[prices.index(min(prices))]) + ' д.',
                                'name_shop': ', '.join(name_shop),
                                'url': from_selenium + "/prices",
                                'nal': "Есть в KingStyle"
                            }
                    else:
                        if w == 1:
                            item = {
                                'name': name,
                                'price': min_price,
                                'delivery': "Доставка " + str(s[prices.index(min(prices))]) + " д. 3",
                                'price_King': dr,
                                'delivery_King': "Доставка " + hj + " д.",
                                'name_shop': ', '.join(name_shop),
                                'url': from_selenium + "/prices",
                                'nal': na
                            }
                        else:
                            item = {
                                'name': name,
                                'price': min_price,
                                'delivery': "Доставка " + str(s[prices.index(min(prices))]) + " д. 4 ",
                                'price_King': "Нашего предложения нет ",
                                'delivery_King': " ",
                                'name_shop': ', '.join(name_shop),
                                'url': from_selenium + "/prices",
                                'nal': na
                            }
                else:
                    if len(s)>0:
                        print('Непонятная штука')
                    else:
                        if w == 1:
                            item = {
                                'name': name,
                                'price': min_price,
                                'delivery': " 4 ",
                                'price_King': saved_price,
                                'delivery_King': saved_delivery + " d",
                                'name_shop': ', '.join(name_shop),
                                'url': from_selenium + "/prices",
                                'nal': na
                            }
                        else:
                             item = {
                                'name': name,
                                'price': min_price,
                                'delivery': " 5 ",
                                'price_King': saved_price,
                                'delivery_King': saved_delivery + " ",
                                'name_shop': ', '.join(name_shop),
                                'url': from_selenium + "/prices",
                                'nal': na
                            }
                data.append(item)
                prices = []
                s = []
                w = 0
                name_shop = []
                dr = ''
                hj = ''
                # создание файла с названием rt 
                with xlsxwriter.Workbook(f"{rt}.xlsx") as workbook:
                    ws = workbook.add_worksheet()
                    bold = workbook.add_format({'bold': True})
                    # создание заголовков столбцов, для ввода данных в таблицу
                    headers = ['Название товара', 'Мин. цена', "Мин.доставка конкурентов", "Цена KingStyle", "Доставка KingStyle",
                                "Магазины мин.доставки", "Наличие", "Ссылка"]
                    ws.write_row(0, 0, headers, cell_format=bold)
                    # запись данных в столбцы, согласно логическим именам из headers
                    for row, item in enumerate(data, start=1):
                        ws.write_row(row, 0, [item["name"], item["price"], item['delivery'], item["price_King"], item["delivery_King"], item['name_shop'], item["nal"], item['url']])
            print(f"Обработана {i}/{3}")
        print(datetime.datetime.now() - tg)

if __name__ == "__main__":
    main(input("Введите ссылку на категорию: "))
