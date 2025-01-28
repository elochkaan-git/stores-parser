from selenium import webdriver
from simple_term_menu import TerminalMenu
from prompt_toolkit import prompt
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from bs4 import BeautifulSoup as bs
from rich.console import Console
from rich.table import Table
from rich.text import Text
import sqlite3
from typing import List, Tuple, Dict
import json
import time
import logging
import multiprocessing
import traceback
import sys
import os
import re

YARCHEPLUS = 1
PYATERKA = 2
WARNING = ('NOTE: Обязательный параметр!',
           'Адрес задается в формате "Город, улица, номер дома"',
           'Но будьте внимательны, адрес у Пятерочки очень чувствительный',
           'То есть "Улица Красноармейская" и "Красноармейская улица" для их сайта -- разные улицы',
           'Поэтому советуем перед тем, как задать адрес, зайти на сайт каталога Пятерочки,',
           'вручную ввести адрес, чтобы убедиться в его правильности, а потом скопировать в программу.',
           'Например, я ввожу адрес "Томск, улица Красноармейская, 114" на сайте магазина, выбираю',
           'первый адрес в списке, который выглядит как "Красноармейская улица, 114, Россия, Томск".',
           'Ага, значит в эту программу мне нужно внести "Томск, Красноармейская улица, 114"')
MESSAGE = 'NOTE: если вы не хотите менять какой-то параметр, то просто нажмите Enter'


start = time.time()
def logger_initialization(logger: logging.Logger) -> None:
    logger.setLevel(logging.DEBUG)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler('parser.log')
    file_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


def load_urls(path: str) -> Dict[str, str]:
    with open(path, 'r') as file:
        log.debug(f'Загружены ссылки из {path}')
        return json.load(file)


def split_list(dict: Dict[str, str], n: int) -> list[list[str], ...]:
    """Разбивает список lst на n примерно равных частей."""
    lst = list(dict.values())
    avg = len(lst) / float(n)
    out = []
    last = 0.0

    while last < len(lst):
        out.append(lst[int(last):int(last + avg)])
        last += avg

    return out


def driver_initialization() -> webdriver.Firefox:
    log.info(f'В процессе {multiprocessing.current_process().pid} запускается браузер')
    driver_options = Options()
    driver_options.add_argument("--headless")
    driver_service = Service(executable_path="/usr/bin/geckodriver")
    driver = webdriver.Firefox(options=driver_options, service=driver_service)
    log.info(f'Браузер настроен и запущен в процессе {multiprocessing.current_process().pid}')
    return driver


def database_writer(queue: multiprocessing.Queue, name_of_table: str) -> None:
    connection = sqlite3.connect("products.db")
    cursor = connection.cursor()
    log.info('Успешные подключение к базе данных и инициализация курсора')

    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {name_of_table} (
    name TEXT NOT NULL,
    amount REAL,
    unit TEXT,
    rating REAL,
    cost REAL,
    link TEXT,
    store TEXT
    )
    ''')
    cursor.execute(f'DELETE FROM {name_of_table}')
    log.info('Таблица успешно создана')
    
    while True:
        data = queue.get()
        if data is None:
            log.info('Все данные занесены в таблицу')
            break
        cursor.execute(f'''INSERT INTO {name_of_table} (name, amount, unit, rating, cost, link, store) 
                      VALUES (?, ?, ?, ?, ?, ?, ?)''', data)

    log.info('Все изменения в таблице сохранены')
    cursor.close()
    connection.commit()
    connection.close()


def adress_setup_yarcheplus(adress: str, driver: webdriver.Firefox) -> None:
    driver.get("https://yarcheplus.ru/")

    WebDriverWait(driver, 20).until(
        ec.element_to_be_clickable((By.XPATH, '//*[@class="a8MJ8NOjn e8MJ8NOjn aXjHckwsA t8MJ8NOjn n8MJ8NOjn l8MJ8NOjn"]'))
    ).click()

    address_input = WebDriverWait(driver, 20).until(
        ec.presence_of_element_located((By.XPATH, '//*[@id="receivedAddress"]'))
    )
    address_input.send_keys(adress)
    address_input.send_keys(Keys.ENTER)

    WebDriverWait(driver, 20).until(
        ec.element_to_be_clickable((By.XPATH, '//button[@class="atLAAl6Nb gtLAAl6Nb"]'))
    ).click()

    log.info(f'Адрес установлен для браузера в процессе {multiprocessing.current_process().pid}')


def adress_setup_5ka(adress: str, driver: webdriver.Firefox) -> None:
    driver.get("https://5ka.ru/catalog/")

    try:
        WebDriverWait(driver, 20).until(
            ec.element_to_be_clickable((By.XPATH, "//*[@class='chakra-button k6J7twGM- css-12yf9td']"))
        )

        WebDriverWait(driver, 20).until(
            ec.element_to_be_clickable((By.XPATH, "//*[@class='chakra-button k6J7twGM- css-12yf9td']"))
        ).click()

        adress_input = WebDriverWait(driver, 20).until(
            ec.element_to_be_clickable((By.XPATH, '/html/body/div[3]/div[3]/div/section/div/div[2]/div/div[2]/div/input'))
        )
        for letter in adress:
            adress_input.send_keys(letter)

        adress = adress.split(' ', 1)[1]
        WebDriverWait(driver, 20).until(
            ec.element_to_be_clickable((By.XPATH, f'//p[contains(text(), "{adress}")]'))
        ).click()

        button_accept = WebDriverWait(driver, 20).until(
            ec.element_to_be_clickable((By.XPATH, '//button[@class="chakra-button nRbDkUwL- css-j9bhfa"]'))
        )
        button_accept.click()
    except Exception as e:
        driver.quit()
        log.error(f'В процессе {multiprocessing.current_process().pid} произошла ошибка - адресс не установлен. Пожалуйста, перезапустите программу')
        log.error(traceback.format_exc())
        sys.exit()

    log.info(f'Адрес установлен для браузера в процессе {multiprocessing.current_process().pid}')


def scraping_yarcheplus(driver: webdriver.Firefox, urls: list[str]) -> Tuple[str, float, str, float, float, str, str]:
    log.info('Начинаем обрабатывать страницы')
    WebDriverWait(driver, 20).until(
        ec.presence_of_element_located((By.XPATH, '//*[@class="loFy5xub4 WoFy5xub4 coFy5xub4"]'))
    )
    log.debug(f'Текущий адрес: {driver.find_element(By.XPATH, '//*[@class="loFy5xub4 WoFy5xub4 coFy5xub4"]').text + \
                                driver.find_element(By.XPATH, '//*[@class="loFy5xub4 dXjHckwsA WoFy5xub4"]').text}')
    for url in urls:
        try:
            log.debug(f'Процесс {multiprocessing.current_process().pid} обрабатывает {url}')
            driver.get(url)
            soup = bs(driver.page_source, 'html.parser')

            for item in soup.select("[class*='akn2Ylc1S bkn2Ylc1S']"):
                name = item.find("div", class_="doFy5xub4 jkn2Ylc1S ToFy5xub4 bBoFy5xub4 coFy5xub4").text
                value, unit = float(item.find("div", class_="eoFy5xub4 rkn2Ylc1S RoFy5xub4 bBoFy5xub4 aoFy5xub4").text.replace('\xa0', ' ').rsplit(' ', 1)[0]),\
                            item.find("div", class_="eoFy5xub4 rkn2Ylc1S RoFy5xub4 bBoFy5xub4 aoFy5xub4").text.replace('\xa0', ' ').rsplit(' ', 1)[1]

                if unit in ['г', 'мл']:
                    value, unit = value / 1000, 'кг' if unit == 'г' else 'л'

                link = "https://yarcheplus.ru" + item.find("a", class_="lkn2Ylc1S").get('href')
                try:
                    rating = item.find("div", class_="ioFy5xub4 e773bOrUb UoFy5xub4 bAoFy5xub4 noFy5xub4 aoFy5xub4").text
                except:
                    rating = None
                cost = item.select("[class*='cwDg02i5o LoFy5xub4 byoFy5xub4 aoFy5xub4']")[0].text

                log.debug(f'{(name, value, unit, rating, cost, link, "Ярче!")} переданы в базу данных')
                yield (name, value, unit, rating, cost, link, "Ярче!")
        except:
            log.error(f'В процессе {multiprocessing.current_process().pid} возникла ошибка при обработке {url}')
            log.error(traceback.format_exc())


def scraping_5ka(driver: webdriver.Firefox, urls: List[str]) -> Tuple[str, float, str, float, float, str, str]:
    time.sleep(3)
    log.info('Начинаем обрабатывать страницы')
    log.debug(f'Текущий адрес: {driver.find_element(By.XPATH, '//p[@class="chakra-text fyOFNehN- SdFzIDV1- css-15mul6p"]').text}')
    for url in urls:
        log.debug(f'Процесс {multiprocessing.current_process().pid} обрабатывает {url}')
        driver.get(url)
        try:
            WebDriverWait(driver, 20).until(
                ec.presence_of_element_located((By.XPATH, '//div[@class="chakra-stack KnkuqE3h- fLmfW7LE- css-8g8ihq"]'))
            )
        except:
            log.warning(f'В данный момент сайт {url} недоступен или на нем отсутствуют товары')
        soup = bs(driver.page_source, 'html.parser')

        for item in soup.select("[class='chakra-stack KnkuqE3h- fLmfW7LE- css-8g8ihq']"):
            name = item.find("p", class_="chakra-text SdLEFc2B- css-1jdqp4k").text
            value, unit = float(re.sub(r'\D', '', item.find("p", class_="chakra-text hPKYUDdM- css-15thl77").text.rsplit(' ', 1)[0])),\
                          item.find("p", class_="chakra-text hPKYUDdM- css-15thl77").text.rsplit(' ', 1)[1]

            if unit in ['г', 'мл']:
                value, unit = value / 1000, 'кг' if unit == 'г' else 'л'

            link = url[:-1] + item.find("a", class_="chakra-link xlSVIYdp- css-13jvj27").get('href')
            try:
                rating = item.find("p", class_="chakra-text o1tGK2uB- css-1jdqp4k").text
            except:
                rating = None
            cost = float(re.sub(r'\D', '', item.select("div.j_IdgaDq-.css-k008qs p")[0].text) + '.' + re.sub(r'\D', '', item.select("div.j_IdgaDq-.css-k008qs p")[1].text))

            log.debug(f'{(name, value, unit, rating, cost, link, "Пятерочка")} переданы в базу данных')
            yield (name, value, unit, rating, cost, link, "Пятерочка")


def worker(args: Tuple[list[str], str, int]) -> Tuple[str, float, str, float, float, str, str]:
    log.info('Программа запускается')
    results = []
    try:
        browser = driver_initialization()
        if args[2] == YARCHEPLUS:
            adress_setup_yarcheplus(args[1], browser)
            for action in scraping_yarcheplus(browser, args[0]):
                results.append(action)
        elif args[2] == PYATERKA:
            adress_setup_5ka(args[1], browser)
            for action in scraping_5ka(browser, args[0]):
                results.append(action)
        
    except:
        log.error(f'Браузер отключен, процесс {multiprocessing.current_process().pid} завершен с ошибкой')
        log.error(traceback.format_exc())
        browser.quit()
    else:
        browser.quit()
        log.info(f'Браузер отключен, процесс {multiprocessing.current_process().pid} завершен без ошибок')
        return results


def main(options: Dict[str, str]) -> None:
    number_of_processes = int(options['number_of_processes'])
    urls = split_list(options['selected_urls'], number_of_processes)
    if [] in urls:
        number_of_processes = len(urls) - urls.count([])
        urls = [i for i in urls if i != []]
    queue = multiprocessing.Queue()

    dbwriter = multiprocessing.Process(target=database_writer, args=(queue, options['name_of_table'],))
    dbwriter.start()

    args = [(part_of_urls, options['adress'], int(options['store_id'])) for part_of_urls in urls]
    with multiprocessing.Pool(processes=number_of_processes) as pool:
        for result in pool.imap(worker, args):
            for value in result:
                queue.put(value)

    queue.put(None)
    dbwriter.join()
    log.info(f"Время выполнения составило {time.time() - start} секунд")


def menu() -> list[int, str, str, str, int]:

    console = Console()

    table = Table(title='Выбор магазина для парсинга (введите q для выхода)',
                  show_lines=True)
    table.add_column('Номер')
    table.add_column('Название')
    table.add_column('Параметры запуска по умолчанию')

    table.add_row('1', 'Ярче', f'''Количество процессов: 3
Путь до ссылок: {os.getcwd() + "/urls1.json"}
Адрес магазина: {None}
Название таблицы: Products_yarcheplus''')
    table.add_row('2', 'Пятерочка', f'''Количество процессов: 3
Путь до ссылок: {os.getcwd() + "/test_urls1.json"}
Адрес магазина: {None}
Название таблицы: Products_5ka''')

    while True:
        console.clear()
        console.print(table)
        store = prompt('Какой магазин выбираем? (введите номер): ')

        options = []
        urls = {}
        if store == 'q':
            sys.exit()
        elif int(store) == 1:
            options = {
                'number_of_processes' : '3',
                'path_to_urls' : './urls1.json',
                'adress' : '',
                'name_of_table' : 'Products_yarcheplus',
                'store_id' : str(YARCHEPLUS),
                'selected_urls' : {}
            }
        elif int(store) == 2:
            options = {
                'number_of_processes' : '3',
                'path_to_urls' : './test_urls1.json',
                'adress' : '',
                'name_of_table' : 'Products_5ka',
                'store_id' : str(PYATERKA),
                'selected_urls' : {}
            }

        is_default = prompt('Хотите изменить базовые настройки? [y/n]: ')
        if is_default.lower() == 'y':

            console.print(Text(MESSAGE, style='italic yellow', justify='full'))

            answer = prompt('Введите количество процессов: ') # Number of processes
            if answer != '':
                options['number_of_processes'] = answer

            answer = prompt('Введите путь до ссылок: ') # Path to urls
            if answer != '':
                options['path_to_urls'] = answer
            
            console.print(Text(' '.join(WARNING), style='italic yellow', justify='full'))
            answer = prompt('Введите адрес магазина: ') # Adress of store
            if answer != '':
                options['adress'] = answer

            answer = prompt('Введите название таблицы в базе данных, в которую хотите сохранить продукты: ') # Name of table in db
            if answer != '':
                options['name_of_table'] = answer
        else:
            console.print(Text(MESSAGE, style='italic yellow', justify='full'))
            console.print(Text(' '.join(WARNING), style='italic yellow', justify='full'))
            options['adress'] = prompt('Введите адрес магазина: ') # Adress of store

        urls = load_urls(options['path_to_urls'])
        options['selected_urls'] = urls
        answer = prompt('Хотите выбрать конкретные категории для парсинга? [y/n]: ')
        if answer.lower() == 'y':
            term_menu = TerminalMenu(
                urls,
                multi_select=True
            )

            term_menu.show()
            new_urls = {}
            for item in term_menu.chosen_menu_entries:
                new_urls[item] = urls[item]
            options['selected_urls'] = new_urls

        console.print()
        answer = prompt('Начинаем парсинг или желаете что-то изменить? (программа начнет работу с выбора магазина) [y/n]: ')
        if answer.lower() == 'y':
            break

    return options


if __name__ == "__main__":
    log = logging.getLogger('parser_logger')
    logger_initialization(log)
    main(menu())