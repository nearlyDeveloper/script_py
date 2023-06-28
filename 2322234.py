import pandas as pd
import requests
import re
import os
import asyncio
import aiohttp
from fuzzywuzzy import fuzz
import time
from rich.progress import Progress
from pprint import pprint



PATH = r"C:/Program Files/Новая папка/positions.csv"
URL = 'https://b2bapi.onliner.by'
URL_C = 'https://catalog.onliner.by'
CLIENT_ID = 'b0e96d5f8cd989c3bee0'
CLIENT_SEKRET = 'ff3267b8fecf5e268650408df663c46c34889ad1'

# самые оптимальные параметры, если не хотите чтовы вас ip сайт заблокировал

TIME = 1.5    #  timeout между каждой стопкой асинхронных запросов
STEP = 8      #  по скольку записей берём в стопку


losts = 0    # сколько утеряно данных
not_find = 0
headers = {"Accept":"application/json"}


# запись изменённых строк
def write(path, name, L):
    with open(path + '/strings.txt', 'a', encoding='utf-8') as file:
        file.write(name + ':\n')
        file.writelines(['\t'+ str(i) +'\n' for i in L])
        file.write('\n\n')



class Table:

    def __init__(self, path, sep=';'):
        self.data = pd.read_csv(path, encoding='cp1251', sep=sep)
        self.path = path
        self.sep = sep


    def _get_index_makers(self, title):
        makers = self.data['Производитель'].str.findall(title, flags=re.IGNORECASE)
        apply = makers.apply(lambda x: x != [])
        return apply.loc[apply == True].index


    def get_by_makers(self, title) -> pd.DataFrame:
        index_makers = self._get_index_makers(title)
        return self.data.iloc[index_makers]


    def get_by_makers_generator(self, title, lens) -> dict:
        """Получаем генератор строк по выбранному производителю"""
        rows = self.get_by_makers(title)

        step = lens
        count = 0
        while True:
            df = rows.iloc[count:count+step]

            if len(df):
                df = df.to_dict('index')

                df = [
                    {'id':i, 
                     'Производитель':d['Производитель'],
                     'Срок доставки по Минску': d['Срок доставки по Минску'],
                     'Товар': d['Товар'],
                     'full_name': d['Производитель'] + ' ' + d['Товар'],
                     'without_brackets': d['Товар'],
                     'Цена': d['Цена']
                        } 
                    for i,d in df.items()]

                yield df
            else:
                break
            count += step


    def insert(self, i, price='', delivery=''):
        """Изменяем (Цену и доставку) строку i в таблице"""
        if price:
            self.data.loc[i, ['Цена']] = price
        if delivery:
            self.data.loc[i, ['Срок доставки по Минску', 'Срок доставки по РБ']] = delivery, delivery + 2


    def upload(self):
        try:
            self.data['Срок рассрочки по Халве'] = pd.to_numeric(self.data['Срок рассрочки по Халве'], errors='coerce').convert_dtypes()

            self.data.to_csv(self.path, index = False, encoding = 'cp1251', sep = self.sep)
        except PermissionError:
            print("Файл .csv открыт где-то ещё. Нужно закрыть!")
            exit()




class Catalog_onliner():

    def __init__(self):
        self.loop = asyncio.get_event_loop()


    def _get_changes(self, catalogs, products):
        """Возвращает изменения"""

        if not (not catalogs is None or not catalogs):
            return 

        shop = products['shop']


        try:
            for info_shop in catalogs['shops'].values():
                compare = fuzz.ratio(info_shop['title'].lower(), shop.lower())
                if compare >= 90:
                    for shops in catalogs['positions']['primary']:
                        if shops['shop_id'] == info_shop['id']:
                            catalog = shops
                            break
                

                    price = catalog['position_price']['amount'].replace('.', ',')
                    delivery = catalog['delivery'].get('pickup_point')

                    changes = False  # изменено ли


                    if price != products['Цена']:
                        changes = True
                        products['Цена'] = price

                    if delivery and delivery['time'] != products['Срок доставки по Минску']:
                        changes = True
                        products['Срок доставки по Минску'] = delivery['time']

                    if changes:
                        return products
                    return
            products['Цена'] = '0'
            return products

        except AttributeError:
            # with open(f'{os.path.dirname(PATH)}/error.json', 'w', encoding='utf-8') as file:
            #     json.dump(catalogs,file, indent=4)
            return {}



    def _get_similar(self, results, products):
        """Возвращает самый похожий продукт"""
        L = []

        for result, product in zip(results, products):
            # находим самое похожее полное имя
            index = 0
            compare = 0
            if not result:
                return 
            for i, tovar in enumerate(result['products']):
                product_full_name = product['full_name'].lower()  # без начала будет
                index_start = product_full_name.find(product['Производитель'].lower())
                token_compare = fuzz.token_sort_ratio(tovar['full_name'], product['full_name'][index_start:])
                if (token_compare > compare) and token_compare >= 100:
                    index = i
                    compare = token_compare
            if result['products'] and result['products'][index].get('prices'):
                L.append({'catalog': result['products'][index], 'table': product})
        return L



    async def request(self, url, session, params = {}):
        global losts
        try:
            if not url:
                return []
            async with session.get(url, 
                                   params=params,
                                   headers=headers) as r:
                return await r.json()
        except (aiohttp.client_exceptions.ContentTypeError, aiohttp.client_exceptions.ClientConnectorError,  
                aiohttp.client_exceptions.ServerTimeoutError, asyncio.TimeoutError):
            losts += 1
            return {}
    


    async def _search_products(self, products: list):
        global not_find
        time.sleep(TIME)

        # получаем товары

        # сначала ищем товары со скобками если таковые есть
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(15)) as session:
            tasks = []
            for product in products:
                # поиск без скобок, иначе может не найти
                full_name = product['full_name'] 

                tasks.append(self.request(f'{URL_C}/sdapi/catalog.api/search/products', 
                                            session, {'query': full_name}))

            results = await asyncio.gather(*tasks)


        # находим все товары у которых ничего не нашлось по скобкам
        index_after_replace = []
        for i, result in enumerate(results):
            if bool(result.get('total')) is False:
                index_after_replace.append(i)


        # если со скобками ничего не нашлось
        if index_after_replace:
            time.sleep(TIME)

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(15)) as session:
                tasks = []
                for i in index_after_replace:
                    full_name = products[i]['full_name'] 

                    if full_name.count('(') and full_name.count(')'):
                        left = full_name.rfind('(')
                        right = full_name.rfind(')')
                        full_name = full_name[:left] + full_name[right + 1:]

                    tasks.append(self.request(f'{URL_C}/sdapi/catalog.api/search/products', 
                                                session, {'query': full_name}))

                new_results = await asyncio.gather(*tasks)

            # меняем результаты товаров со скобками на результаты товаров без скобок
            for i, result in zip(index_after_replace, new_results):
                results[i] = result


        # находим самые похожие
        catalogs = self._get_similar(results, products)

        time.sleep(TIME)

        # # берём данные про товар и его магазины
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(20)) as session:
            tasks = []
            if not catalogs:
                return
            for catalog in catalogs:
                if not catalog:
                    not_find += 1
                    tasks.append(self.request('', session))
                else:
                    tasks.append(self.request(catalog['catalog']['prices']['url'], session))

            catalogs = [{'catalog':catalog, 'table': table['table']}  
                        for catalog, table in zip(await asyncio.gather(*tasks), catalogs)]


        results = []
        for catalog in catalogs:
            if catalog['catalog']:
                result = self._get_changes(catalog['catalog'], catalog['table'])
                if result:
                    results.append(result)


        return results


    def search_products(self, products: list):
        """Получаем изменения продуктов"""
        return self.loop.run_until_complete(self._search_products(products))





def main():
    global losts, not_find


    catalog = Catalog_onliner()
    table = Table(PATH)


    # это проверка, открыть ли файл в другой программе
    table.upload()


    makers = input('Введите бренды и магазины:\n   ')
    makers = makers.split(',')
    makers_and_shops = [i for i in makers]

    # чтобы файл был новым и пустым
    with open(os.path.dirname(PATH) + '/strings.txt', 'w', encoding='utf-8') as file:
        file.write('')


    for maker_and_shop in makers_and_shops:
        print('\n\n')

        maker, shop = maker_and_shop.split(':')
        maker = maker.strip()
        shop = shop.strip()

        # сколько всего записей
        count = len(table.get_by_makers(maker))
        print(f'Записей найдено: {count}')

        # какие строки изменены
        strings = []

        table.upload()

        with Progress() as prog:
            # добавляем прогресс загрузки
            progress = prog.add_task(f"[cyan]Загрузка: {maker} | {shop}", total=count)

            # получаем данные из таблицы
            for row in table.get_by_makers_generator(maker, STEP):
                # добавляем в запись магазин
                row = [{**i, 'shop': shop} for i in row]

                # получаем изменения
                change = catalog.search_products(row)

                if change:
                    # добавляем их номер строки каждого изменённого
                    strings.extend([i['id'] + 2 for i in change])

                    # изменяем таблицу
                    for ch in change:
                        table.insert(ch['id'], ch['Цена'], ch['Срок доставки по Минску'])

                # обновляем прогресс
                prog.update(progress, advance=STEP)

            # записываем все изменённые строки
            write(os.path.dirname(PATH), maker + ' | ' + shop, strings)

            # выгружаем таблицу
            table.upload()


        print(f'\nИнформация обработки {maker} | {shop}:')
        print(f'\tИзменено: {len(strings)}/{count}')
        print(f"\tУтеряно данных: {losts}")
        print(f'\tНе найдено из-за названий: {not_find}')

        losts = 0

    print(f'\n[+] Все данные успешно сохранены!')



if __name__ == "__main__":
    if not os.path.exists(PATH):
        print('Неправильный путь!')
        exit()
    if not os.path.splitext(PATH)[1] == '.csv':
        print('Можно использовать только .csv файлы')
        exit()

    main()







    # pprint(catalog.search_products([                    
    #                 {'id':55, 
    #                  'Производитель':'Nika',
    #                  'Срок доставки по Минску': 0.0,
    #                  'Товар': 'Адде (черный) [603.608.67]',
    #                  'full_name': 'Nika Фабрик 4 (горчичный)',
    #                  'Цена': '0',
    #                  'shop': 'kingstyle'
    #                     }]))













class Catalog_api():
    headers = {"Accept":"application/json"}

    def __init__(self):
        self.token = self._get_token()
        self.params = {'access_token': self.token}


    def _get_token(self):
        r = requests.post('https://b2bapi.onliner.by/oauth/token', 
                            auth=(CLIENT_ID,CLIENT_SEKRET), 
                            headers=self.headers, 
                            data={'grant_type':"client_credentials"})

        assert r.ok, "не получен токен"
        return r.json()['access_token']


    def _get_id_category(self, title):
        r = requests.get(f'{URL}/sections', 
                            headers=self.headers, 
                            params={**self.params, 'title': title})
        if len(r.json()) > 1:
            print('many')
        return list(r.json().keys())[0]


    def _get_id_manufacture(self, category_id, title):
        r = requests.get(f'{URL}/sections/{category_id}/manufacturers', 
                            headers=self.headers, 
                            params={**self.params, 'title': title})
        if len(r.json()) > 1:
            print('many')
        return list(r.json().keys())[0]


    def _get_id_product(self, category_id, manufacture_id, title):
        r = requests.get(f'{URL}/sections/{category_id}/manufacturers/{manufacture_id}/products', 
                            headers=self.headers, 
                            params={**self.params, 'title': title})
        if len(r.json()) > 1:
            print('many')
        return list(r.json().keys())[0]


    def _get_positions(self, category_id, manufacture_id, product_id):
        r = requests.get(f'{URL}/sections/{category_id}/manufacturers/{manufacture_id}/products/{product_id}/positions', 
                            headers=self.headers, 
                            params=self.params)
        return r.json()


    def get_product(self, category, manufacture, product):
        category_id = self._get_id_category(category)
        manufacture_id = self._get_id_manufacture(category_id, manufacture)
        product_id = self._get_id_product(category_id, manufacture_id, product)
        result = self._get_positions(category_id, manufacture_id, product_id)
        return result


    def get_positions(self) -> dict:
        r = requests.get(f'{URL}/positions', 
                                headers=self.headers, 
                                params=self.params)
        result = {}
        for product in r.json():
            result[product['id']] = product

        return result    
        
    """
    makers = input('Введите бренд (или бренды через ","): ')
    makers = makers.split(',')

    table = Table(PATH)
    catalog = Catalog()
    products = catalog.get_positions()


    for maker in makers:
        maker = maker.strip()
        print(f'\n\nОбрабатывается {maker}')

        changes = [0,0,0,0]    #  0 - Цены    1 -  доставка по Минску     2 - доставка по РБ       3 - всего
        modified_rows = []  # какие строки изменены

        for i, row in table.get_by_makers_generator(maker):
            changes[3] += 1
            product = products.get(str(row['id-предложения']))
            if product:
                b = changes.copy()

                product_price = str(product['price']).replace('.', ',')
                delivery = product.get('deliveryTownTime')

                if product_price != row['Цена']:
                    changes[0] += 1
                    row['Цена'] = product_price

                if delivery and int(float(delivery)) != int(float(row['Срок доставки по Минску'])):
                    changes[0] += 1
                    row['Срок доставки по Минску'] = delivery

                if delivery and int(float(delivery)) + 2 != int(float(row['Срок доставки по РБ'])):
                    changes[0] += 1
                    row['Срок доставки по РБ'] = int(float(delivery)) + 2

                if changes != b:
                    modified_rows.append(i + 2)
                    table.insert(i, row)
                
        if modified_rows:
            table.upload()

        print('\n[+] Данные успешно изменены')

        print(f"\n[+] Всего найдено записей с брендом {maker} - {changes[3]}")

        print(f"\n[+] Изменено:")
        print(f"\tЦен - {changes[0]}")
        print(f"\tДоставке по Минску - {changes[1]}")
        print(f"\tДоставке по РБ - {changes[2]}")

        print(f"\n[+] Изменённые строки: {','.join([str(i) for i in modified_rows])}")"""