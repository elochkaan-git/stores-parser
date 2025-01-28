from selenium import webdriver
from prompt_toolkit import prompt
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from bs4 import BeautifulSoup as bs
import sqlite3
import typing
import json
import time
import logging
import multiprocessing
import traceback
import sys
import os

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


def load_urls(path: str) -> list[str]:
    with open(path, 'r') as file:
        log.debug(f'Загружены ссылки из {path}')
        return json.load(file)


def split_list(dict, n):
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


def adress_setup(adress: str, driver: webdriver.Firefox) -> None:
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


def scraping(driver: webdriver.Firefox, urls: typing.Dict[str, str]) -> typing.Tuple[str, float, str, float, float, str, str]:
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


def main(args: typing.Tuple[list[str], str]) -> typing.Tuple[str, float, str, float, float, str, str]:
    log.info('Программа запускается')
    results = []
    try:
        browser = driver_initialization()
        adress_setup(args[1], browser)
        for action in scraping(browser, args[0]):
            results.append(action)
    except:
        log.error(f'Браузер отключен, процесс {multiprocessing.current_process().pid} завершен с ошибкой')
        log.error(traceback.format_exc())
        browser.quit()
    else:
        browser.quit()
        log.info(f'Браузер отключен, процесс {multiprocessing.current_process().pid} завершен без ошибок')
        return results


def start_processes(options: list[int, str, str, str]) -> None:
    number_of_processes = options[0]
    urls = split_list(load_urls(options[1]), number_of_processes)
    queue = multiprocessing.Queue()

    dbwriter = multiprocessing.Process(target=database_writer, args=(queue, options[3],))
    dbwriter.start()

    args = [(part_of_urls, options[2]) for part_of_urls in urls]
    with multiprocessing.Pool(processes=number_of_processes) as pool:
        for result in pool.imap(main, args):
            for value in result:
                queue.put(value)

    queue.put(None)
    dbwriter.join()
    log.info(f"Время выполнения составило {time.time() - start} секунд")


def menu() -> list[int, str, str, str]:
    
    options = [1, '', '', 'Products']

    os.system('clear')
    options[0] = int(prompt('Введите количество процессов (по умолчанию 1): ')) # Number of processes
    options[1] = prompt('Введите путь до ссылок: ') # Path to urls
    options[2] = prompt('Введите адрес магазина: ') # Adress of store
    options[3] = prompt('Введите название таблицы в базе данных, в которую хотите сохранить продукты: ') # Name of table in db
    return options


if __name__ == "__main__":
    log = logging.getLogger('parser_logger')
    logger_initialization(log)
    start_processes(menu())
    