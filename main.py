from selenium import webdriver
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

def split_list(lst, n):
    """Разбивает список lst на n примерно равных частей."""
    avg = len(lst) / float(n)
    out = []
    last = 0.0

    while last < len(lst):
        out.append(lst[int(last):int(last + avg)])
        last += avg

    return out


def driver_initialization() -> webdriver.Firefox:
    log.info('Начинаем запускать браузер')
    driver_options = Options()
    driver_options.add_argument("--headless")
    driver_service = Service(executable_path="/usr/bin/geckodriver")
    driver = webdriver.Firefox(options=driver_options, service=driver_service)
    log.info('Браузер настроен и готов к работе')
    return driver


def database_writer(queue: multiprocessing.Queue) -> None:
    connection = sqlite3.connect("products.db")
    cursor = connection.cursor()
    log.info('Успешные подключение к базе данных и инициализация курсора')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Products (
    name TEXT NOT NULL,
    amount REAL,
    unit TEXT,
    rating REAL,
    cost REAL,
    link TEXT,
    store TEXT
    )
    ''')
    cursor.execute('DELETE FROM Products')
    log.info('Таблица успешно создана')
    
    while True:
        data = queue.get()
        if data is None:
            log.info('Все данные занесены в таблицу')
            break
        cursor.execute('''INSERT INTO Products (name, amount, unit, rating, cost, link, store) 
                      VALUES (?, ?, ?, ?, ?, ?, ?)''', data)

    log.info('Все изменения в таблице сохранены')
    connection.commit()
    connection.close()


# def products_counter(driver: webdriver.Firefox, urls: typing.Dict[str, str]) -> int:
#     counter = 0
#     for index, url in urls.items():
#         driver.get(url)
#         soup = bs(driver.page_source, 'html.parser')
#         counter += len(soup.select("[class*='akn2Ylc1S bkn2Ylc1S']"))
#     return counter


def adress_setup(adress: str, driver: webdriver.Firefox) -> None:
    driver.get("https://yarcheplus.ru/")

    WebDriverWait(driver, 20).until(
        ec.element_to_be_clickable((By.XPATH, '//*[@class="a8MJ8NOjn e8MJ8NOjn aXjHckwsA t8MJ8NOjn n8MJ8NOjn l8MJ8NOjn"]'))
    ).click()

    address_input = WebDriverWait(driver, 20).until(
        ec.presence_of_element_located((By.XPATH, '//*[@id="receivedAddress"]'))
    )
    address_input.send_keys('Томск, Учебная улица, 42')
    address_input.send_keys(Keys.ENTER)

    WebDriverWait(driver, 20).until(
        ec.element_to_be_clickable((By.XPATH, '//button[@class="atLAAl6Nb gtLAAl6Nb"]'))
    ).click()

    log.info('Адрес установлен')


def scraping(driver: webdriver.Firefox, urls: typing.Dict[str, str]) -> typing.Tuple[str, float, str, float, float, str, str]:
    log.info('Начинаем обрабатывать страницы')
    for url in urls:
        driver.get(url)
        log.debug(f'Обработка {url}')
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


def main(urls: list[str]) -> typing.Tuple[str, float, str, float, float, str, str]:
    log.info('Программа запускается')
    results = []
    try:
        browser = driver_initialization()
        adress_setup('Томск, Учебная улица, 42', browser)
        for action in scraping(browser, urls):
            results.append(action)
    except Exception as e:
        log.error('Веб-драйвер отключен, база данных отключена. Программа завершила работу с ошибкой')
        log.error(e)
        log.error(traceback.format_exc())
        browser.quit()
    else:
        browser.quit()
        log.info('Веб-драйвер отключен, база данных отключена. Программа успешно завершила работу')
        return results



if __name__ == "__main__":
    log = logging.getLogger('parser_logger')
    logger_initialization(log)
    number_of_processes = int(sys.argv[1])
    urls = split_list(load_urls(sys.argv[2]), number_of_processes)
    queue = multiprocessing.Queue()

    dbwriter = multiprocessing.Process(target=database_writer, args=(queue,))
    dbwriter.start()

    with multiprocessing.Pool(processes=number_of_processes) as pool:
        for result in pool.imap(main, urls):
            for value in result:
                queue.put(value)

    queue.put(None)
    dbwriter.join()
    log.info(f"Время выполнения составило {time.time() - start - 5} секунд")
