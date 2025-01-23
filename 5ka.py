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
import re
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
    log.info(f'В процессе {multiprocessing.current_process().pid} запускается браузер')
    driver_options = Options()
    driver_options.add_argument("--headless")
    driver_service = Service(executable_path="/usr/bin/geckodriver")
    driver = webdriver.Firefox(options=driver_options, service=driver_service)
    log.info(f'Браузер настроен и запущен в процессе {multiprocessing.current_process().pid}')
    return driver


def database_writer(queue: multiprocessing.Queue) -> None:
    connection = sqlite3.connect("products.db")
    cursor = connection.cursor()
    log.info('Успешные подключение к базе данных и инициализация курсора')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Products_5ka (
    name TEXT NOT NULL,
    amount REAL,
    unit TEXT,
    rating REAL,
    cost REAL,
    link TEXT,
    store TEXT
    )
    ''')
    cursor.execute('DELETE FROM Products_5ka')
    log.info('Таблица успешно создана')
    
    while True:
        data = queue.get()
        if data is None:
            log.info('Все данные занесены в таблицу')
            break
        cursor.execute('''INSERT INTO Products_5ka (name, amount, unit, rating, cost, link, store) 
                      VALUES (?, ?, ?, ?, ?, ?, ?)''', data)

    log.info('Все изменения в таблице сохранены')
    cursor.close()
    connection.commit()
    connection.close()


def adress_setup(adress: str, driver: webdriver.Firefox) -> None:
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
        for letter in 'Томск, Красноармейская улица, 114':
            adress_input.send_keys(letter)

        WebDriverWait(driver, 20).until(
            ec.element_to_be_clickable((By.XPATH, '//p[contains(text(), "Красноармейская улица, 114")]'))
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


def scraping(driver: webdriver.Firefox, urls: typing.List[str]) -> typing.Tuple[str, float, str, float, float, str, str]:
    time.sleep(3)
    log.info('Начинаем обрабатывать страницы')
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


def main(urls: list[str], adress: str) -> typing.Tuple[str, float, str, float, float, str, str]:
    log.info(f'Запуск процесса {multiprocessing.current_process().pid}')
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



if __name__ == "__main__":
    log = logging.getLogger('parser_logger')
    logger_initialization(log)
    number_of_processes = int(sys.argv[1])
    urls = split_list(load_urls(sys.argv[2]), number_of_processes)
    queue = multiprocessing.Queue()

    dbwriter = multiprocessing.Process(target=database_writer, args=(queue,))
    dbwriter.start()

    args = [(part_of_urls, sys.argv[3]) for part_of_urls in urls]
    with multiprocessing.Pool(processes=number_of_processes) as pool:
        for result in pool.imap(main, args):
            for value in result:
                queue.put(value)

    queue.put(None)
    dbwriter.join()
    log.info(f"Время выполнения составило {time.time() - start} секунд")
