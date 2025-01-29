from simple_term_menu import TerminalMenu
from prompt_toolkit import prompt
from rich.console import Console
from rich.table import Table
from rich.text import Text
import sqlite3
from typing import List, Tuple, Dict, Union
import json
import time
import multiprocessing
import sys
import os

from classes import YarcheplusClass,\
                    PyaterkaClass
from classes.BaseClass import log

STORES = {
    0 : YarcheplusClass.YarcheParser,
    1 : PyaterkaClass.PyaterkaParser,
}

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

def load_urls(path: str) -> Dict[str, str]:
    with open(path, 'r') as file:
        log.debug(f'Загружены ссылки из {path}')
        return json.load(file)


def split_list(dict: Dict[str, str], n: int) -> List[List[str], ...]:
    """Разбивает список lst на n примерно равных частей."""
    lst = list(dict.values())
    avg = len(lst) / float(n)
    out = []
    last = 0.0

    while last < len(lst):
        out.append(lst[int(last):int(last + avg)])
        last += avg

    return out


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


def use_parser (args: Tuple[str, List[str], int]) -> List[Tuple[str, float, str, float, float, str, str]]:
    try:
        results = []
        parser = STORES[args[2]](args[0], args[1])
        parser.adress_setup()
        for result in parser.scraping():
            results.append(result)
    finally:
        parser.close()
    return results


def main(options: Dict[str, Union[str, int, Dict[str, str]]]) -> None:
    number_of_processes = int(options['number_of_processes'])
    urls = split_list(options['selected_urls'], number_of_processes)
    if [] in urls:
        number_of_processes = len(urls) - urls.count([])
        urls = [i for i in urls if i != []]
    queue = multiprocessing.Queue()

    dbwriter = multiprocessing.Process(target=database_writer, args=(queue, options['name_of_table'],))
    dbwriter.start()

    args = [(options['adress'], part_of_urls, options['store_id']) for part_of_urls in urls]
    with multiprocessing.Pool(processes=number_of_processes) as pool:
        for result in pool.imap(use_parser, args):
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
                'store_id' : 0,
                'selected_urls' : {}
            }
        elif int(store) == 2:
            options = {
                'number_of_processes' : '3',
                'path_to_urls' : './test_urls1.json',
                'adress' : '',
                'name_of_table' : 'Products_5ka',
                'store_id' : 1,
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
    main(menu())