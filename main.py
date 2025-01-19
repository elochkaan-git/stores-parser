from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup as bs
import sqlite3
import typing
import json
import time
import logging
import multiprocessing

start = time.time()
log = logging.getLogger('parser_logger')
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


def driver_initialization() -> webdriver.Firefox:
    log.info('Начинаем запускать браузер')
    driver_options = Options()
    driver_options.add_argument("--headless")
    driver_service = Service(executable_path="/usr/bin/geckodriver")
    driver = webdriver.Firefox(options=driver_options, service=driver_service)
    driver.implicitly_wait(10)
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


def adress_setup(adress: str, driver: webdriver.Firefox) -> None:
    driver.get("https://yarcheplus.ru/")
    driver.find_element(By.XPATH, "//*[@class='a8MJ8NOjn e8MJ8NOjn aXjHckwsA t8MJ8NOjn n8MJ8NOjn l8MJ8NOjn']").click()
    driver.find_element(By.XPATH, "//*[@id='receivedAddress']").send_keys("Томск, Учебная улица, 42")
    driver.find_element(By.XPATH, "//*[@id='receivedAddress']").send_keys(Keys.ENTER)
    driver.find_element(By.XPATH, "//*[@class='atLAAl6Nb gtLAAl6Nb']").click()
    log.info('Адрес установлен')


def scraping(driver: webdriver.Firefox, urls: typing.Dict[str, str]) -> typing.Tuple[str, float, str, float, float, str, str]:
    log.info('Начинаем обрабатывать страницы')
    for index, url in urls.items():
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


def main(path_to_urls: str) -> typing.Tuple[str, float, str, float, float, str, str]:
    log.info('Программа запускается')
    results = []
    try:
        logger_initialization(log)
        browser = driver_initialization()
        adress_setup('Томск, Учебная улица, 42', browser)
        urls = load_urls(path_to_urls)
        for action in scraping(browser, urls):
            results.append(action)
    except Exception as e:
        log.error('Веб-драйвер отключен, база данных отключена. Программа завершила работу с ошибкой')
        log.error(e)
        browser.quit()
    else:
        browser.quit()
        log.info('Веб-драйвер отключен, база данных отключена. Программа успешно завершила работу')
        return results



if __name__ == "__main__":
    logger_initialization(log)
    queue = multiprocessing.Queue()

    dbwriter = multiprocessing.Process(target=database_writer, args=(queue,))
    dbwriter.start()

    with multiprocessing.Pool(processes=3) as pool:
        for result in pool.imap(main, ['urls1.json', 'urls2.json', 'urls3.json']):
            for value in result:
                queue.put(value)

    queue.put(None)
    dbwriter.join()
    log.info(f"Время выполнения составило {time.time() - start - 5} секунд")