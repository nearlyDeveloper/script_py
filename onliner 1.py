# -*- coding: utf8 -*-
import datetime
from random import randrange
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import json
import xlsxwriter
import requests


def main(url_of_group):
    tg = datetime.datetime.now()
    prices = []
    name_shop = []
    s = []
    data = []
    if url_of_group == "https://catalog.onliner.by/office_chair":
        ber = "office_chair"
        rt = "Офисные_кресла_и_стулья"
    else:
        if url_of_group == "https://catalog.onliner.by/table":
            ber = "table"
            rt = "Письменные_и_компьютерные_столы"
        else:
            if url_of_group == "https://catalog.onliner.by/chair":
                ber = "chair"
                rt = "Стулья_для_кухни_и_бара"
            else:
                if url_of_group == "https://catalog.onliner.by/kidsdesk":
                    ber = "kidsdesk"
                    rt = "Детские_парты,_столы,_стулья"
                else:
                    if url_of_group == "https://catalog.onliner.by/gardenfurniture":
                        ber = "gardenfurniture"
                        rt = "Садовая_мебель"
                    else:
                        if url_of_group == "https://catalog.onliner.by/divan":
                            ber = "divan"
                            rt = "Диваны"
                        else:
                            if url_of_group == "https://catalog.onliner.by/interior_chair":
                               ber = "interior_chair"
                               rt = "Кресла"
                            else:
                                if url_of_group == "https://catalog.onliner.by/kitchen_table":
                                   ber = "kitchen_table"
                                   rt = "Кухонные столы и обеденные группы"
    w = 0
    dr = ""
    hj = ""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    chrome_driver_path = "C:/Users/Bartosh/Desktop/Main files/script_py/chromedriver.exe"
    driver = webdriver.Chrome(executable_path=chrome_driver_path)

    with driver as driver:
        for i in range(1, 2):
            req = requests.get(url=f"https://catalog.onliner.by/sdapi/catalog.api/search/{ber}?group=1&page={i}")
            jso = json.loads(req.text)
            for j in range(29, 30):
                from_selenium = jso['products'][j]['html_url']
                name = jso['products'][j]['full_name']
                html = jso['products'][j]['html_url'].split('/')[-1]
                r = requests.get(
                    url=f"https://catalog.onliner.by/sdapi/shop.api/products/{html}/positions?town=all")
                json_data = json.loads(r.text)
                driver.get(from_selenium + "/prices")
                # print(quantity)
                try:
                    button = driver.find_element(By.CLASS_NAME,"button-style_specific")
                    button.click()
                except NoSuchElementException:
                    pass
                sleep(1.7)
                a = driver.find_elements(By.CLASS_NAME,"offers-list__description_nowrap")
                price_element = driver.find_element(By.CLASS_NAME, "offers-description__price")
                if driver.find_elements(By.CLASS_NAME, "offers-description__price_secondary"):
                    secondary_price_element = driver.find_element(By.CLASS_NAME, "offers-description__price_secondary")
                    price_list = [secondary_price_element]
                else:
                    price_list = [price_element]
                
                min_price = None
                saved_price = 'None'
                for price in price_list:
                        try:
                            price_value = price.get_attribute("textContent").strip().replace(' р.','').replace(',', '.')
                            if min_price is None or float(price_value) < float(min_price):
                                min_price = price_value
                        except ValueError:
                                pass
               # try:
               #     delivery_price = driver.find_element(By.CLASS_NAME, "offers-list__item")
               #     delivery_price_link = delivery_price.find_element(By.TAG_NAME, "a")
                #    if delivery_price_link.get_attribute("href") == "https://5410.shop.onliner.by/":
                #        if "под заказ" in delivery_price.text:
                #            price_element = delivery_price.find_element(By.CLASS_NAME, "offers-list__description offers-list__description_alter-other")
                #            price_value = price_element.get_attribute("textContent").strip().replace(' р.','').replace(',', '.')
                #            if saved_price is None or float(price_value) < float(price_value):
                #                saved_price = price_value
                                
               # except NoSuchElementException:
               #     pass 
                for o in a:
                    # print(o.text)
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
                        # print(dr)
                        # print(hj)
                        w = 1
                        if len(s) < 2:
                            s = []
                        else:
                            del s[k]
                # print(s)

                if w == 1:
                    na = "Есть в магазине KingStyle"
                else:
                    na = "Нет в магазине KingStyle"
                # print(quantity)
                # print(s)
                if len(s) > 0:
                    if len(s) == 1:
                        item = {
                            'name': name,
                            'price': min_price,
                            'delivery': "Доставка" + ' '.join(s) + " д.",
                            'price_King': dr,
                            'delivery_King': "Доставка " + hj + " д.",
                            'name_shop': ', '.join(name_shop),
                            'url': from_selenium + "/prices",
                            'nal': na
                        }

                    else:
                        if w == 1:
                            item = {
                                'name': name,
                                'price': min_price,
                                'delivery': "Доставка " + str(s[prices.index(min(prices))]) + " д.",
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
                                'delivery': "Доставка " + str(s[prices.index(min(prices))]) + " д.",
                                'price_King': " ",
                                'delivery_King': "Доставка",
                                'name_shop': ', '.join(name_shop),
                                'url': from_selenium + "/prices",
                                'nal': na
                            }
                else:
                    if len(s)>0:
                        if w == 1:
                            item = {
                                'name': name,
                                'price': min_price,
                                'delivery': "Пока нет доставки по адресу и в пункты выдачи",
                                'price_King': dr,
                                'delivery_King': "Доставка " + hj + " д.",
                                'name_shop': ', '.join(name_shop),
                                'url': from_selenium + "/prices",
                                'nal': na
                            }
                        else:
                            item = {
                                'name': name,
                                'price': ''.join(prices),
                                'delivery': "Пока нет доставки по адресу и в пункты выдачи",
                                'price_King': dr,
                                'delivery_King': hj,
                                'name_shop': ', '.join(name_shop),
                                'url': from_selenium + "/prices",
                                'nal': na
                            }

                    else:
                            item = {
                                'name': name,
                                'price': min_price,
                                'delivery': " ",
                                'price_King': saved_price,
                                'delivery_King': "Под заказ" + hj,
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

                with xlsxwriter.Workbook(f"{rt}.xlsx") as workbook:
                    ws = workbook.add_worksheet()
                    bold = workbook.add_format({'bold': True})

                    headers = ['Название товара', 'Минимальная цена', "Мин. сроки доставки среди тех, у кого минимальная цена", "Цена KingStyle", "Сроки доставки KingStyle",
                               "Магазины с мин. сроком доставки", "Наличие", "Ссылка"]
                    for col, h in enumerate(headers):
                        ws.write_string(0, col, h, cell_format=bold)
                    for row, item in enumerate(data, start=1):
                        ws.write_string(row, 0, item["name"])
                        ws.write_string(row, 1, item["price"])
                        ws.write_string(row, 2, item['delivery'])
                        ws.write_string(row, 3, item["price_King"])
                        ws.write_string(row, 4, item["delivery_King"])
                        ws.write_string(row, 5, item['name_shop'])
                        ws.write_string(row, 6, item["nal"])
                        ws.write_string(row, 7, item['url'])

            print(f"Обработана {i}/{4}")
        print(datetime.datetime.now() - tg)


if __name__ == "__main__":
    main(input("Введите ссылку на категорию: "))
