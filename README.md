Перед вами находится небольшая программа, которая собирает информацию о продуктах в магазинах. Пока доступны только **Пятерочка** и **Ярче!**.

### Быстрый старт
```shell
git clone https://github.com/elochkaan-git/stores-parser.git
cd stores-parser
python -m venv .
source bin/activate
# .\Scripts\activate.bat if you using cmd.exe
# .\Scripts\Activate.ps1 if you using PowerShell
pip install -r requirements.txt
python merged_parser.py
```

### Объяснение работы
Данная программа использует Selenium для загрузки веб-страниц и их обработки. Имитируя действия пользователя изначально выставляется адрес магазина, цены на товары в котором вы хотите узнать. Затем задача разбивается на процессы, и они обрабатывают карточки товаров. Результат работы записывается в БД SQLite.