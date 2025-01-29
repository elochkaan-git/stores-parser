from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from typing import List, Tuple, Dict
from bs4 import BeautifulSoup as bs
import multiprocessing
import traceback
import os

from classes.Logger import *
logger_initialization(log)

class BaseParser:

    def __init__ (self, adress: str, urls: List[str]) -> None:

        # Все парсеры используют по умолчанию один и тот же способ
        # инициализации, поэтому он присутствует в методе __init__().
        # Единственное, что может быть переопределено -- 'домашняя' ссылка

        self.driver_options = Options()
        self.driver_options.add_argument("--headless")
        self.driver_service = Service(
            executable_path=os.system('which geckodriver')
        )
        self.driver = webdriver.Firefox(
            options=self.driver_options, 
            service=self.driver_service
        )
        log.info(f'Браузер настроен и запущен в процессе {multiprocessing.current_process().pid}')
        self.url_of_store = ''     # Переопределяется для каждого парсера
        self.adress       = adress # Переопределяется для каждого парсера
        self.urls         = urls

    def adress_setup (self) -> None:
        """
        Выставляет на сайте магазина адрес нужного магазина
        """
        
        self.driver.get(self.url_of_store)

        # Из-за разницы в классификации элементов от сайта к сайту
        # этот метод пользователь должен сам переопределить

    def scraping (self, urls: List[str]) -> Tuple[str, float, str, float, float, str, str]:
        pass

        # Здесь тоже не всё однозначно. От магазина к магазину методы
        # отличаются, но идея едина: проходимся по списку адресов,
        # находим все карточки товаров через bs().select(),
        # из этих карточек выделяем:
        # 1. name       -- название товара
        # 2. value, unit -- количество (вес, объем и т.д.) и единица
        # измерения, которые (если возможно) нужно перевести в СИ (кг, л и т.д.)
        # 3. link       -- ссылка на товар
        # 4. rating     -- оценка, если есть
        # 5. cost       -- цена
        # 
        # Далее это все возвращается ввиде кортежа:
        # yield (name, value, unit, rating, cost, link, name_of_store)
        # # name_of_store необязательно заводить отдельной переменной
        # # название магазина можно писать в самом кортеже

    def close (self):
        self.driver.quit()
        print(f'Работа {self.driver} завершена')
        
