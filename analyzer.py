import sqlite3
from rapidfuzz import fuzz
import tabulate
from simple_term_menu import TerminalMenu
import pymorphy3

morph = pymorphy3.MorphAnalyzer()
def normalize_string(string: str):
    words = string.split(' ')
    normalized_words = []
    for word in words:
        try:
            normalized_words.append(morph.parse(word)[0].inflect({'sing', 'nomn'}).word)
        except:
            normalized_words.append(word)
    
    return ' '.join(normalized_words)


connection = sqlite3.connect('products.db')
cursor = connection.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

product = input("Введите продукт для поиска: ")
result = []

for table in tables:
    table_name = table[0]

    cursor.execute(f'SELECT * FROM {table_name}')
    all_items = cursor.fetchall()
    table = []
    for item in all_items:
        score = fuzz.token_ratio(normalize_string(product.lower()), normalize_string(item[0].lower()))
        if score >= 90:
            table.append(item)

    names = [i[0] for i in table]

    menu = TerminalMenu(
        menu_entries=names,
        multi_select=True,
        title='Вот, что мы нашли по вашему запросу. Выберите нужные варианты'
    )

    menu.show()
    for item in all_items:
        if item[0] in menu.chosen_menu_entries:
            temp = (item[0], item[4]/item[1], item[6])
            result.append(temp)

result.sort(key=lambda x: x[1])
print(tabulate.tabulate(result, tablefmt='github', headers=('Название товара', 'Цена за кг/л/шт', 'Магазин')))

cursor.close()
connection.close()