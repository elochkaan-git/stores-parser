from classes.BaseClass import *

class YarcheParser (BaseParser):

    def __init__ (self, adress: str, urls: List[str]) -> None:
        super().__init__(adress, urls)
        self.url_of_store = 'https://yarcheplus.ru'

    def adress_setup (self) -> None:
        self.driver.get(self.url_of_store)

        WebDriverWait(self.driver, 20).until(
            ec.element_to_be_clickable((By.XPATH, '//*[@class="a8MJ8NOjn e8MJ8NOjn aXjHckwsA t8MJ8NOjn n8MJ8NOjn l8MJ8NOjn"]'))
        ).click()

        address_input = WebDriverWait(self.driver, 20).until(
            ec.presence_of_element_located((By.XPATH, '//*[@id="receivedAddress"]'))
        )
        address_input.send_keys(self.adress)
        address_input.send_keys(Keys.ENTER)

        WebDriverWait(self.driver, 20).until(
            ec.element_to_be_clickable((By.XPATH, '//button[@class="atLAAl6Nb gtLAAl6Nb"]'))
        ).click()
        log.info(f'В процессе {multiprocessing.current_process().pid} установлен адрес')

    def scraping (self) -> Tuple[str, float, str, float, float, str, str]:
        log.info('Начинаем обрабатывать страницы')
        WebDriverWait(self.driver, 20).until(
            ec.presence_of_element_located((By.XPATH, '//*[@class="loFy5xub4 WoFy5xub4 coFy5xub4"]'))
        )
        log.debug(f'Текущий адрес: {self.driver.find_element(By.XPATH, '//*[@class="loFy5xub4 WoFy5xub4 coFy5xub4"]').text + \
                                    self.driver.find_element(By.XPATH, '//*[@class="loFy5xub4 dXjHckwsA WoFy5xub4"]').text}')
        for url in self.urls:
            try:
                log.debug(f'Процесс {multiprocessing.current_process().pid} обрабатывает {url}')
                self.driver.get(url)
                soup = bs(self.driver.page_source, 'html.parser')

                for item in soup.select("[class*='akn2Ylc1S bkn2Ylc1S']"):
                    name = item.find("div", class_="doFy5xub4 jkn2Ylc1S ToFy5xub4 bBoFy5xub4 coFy5xub4").text
                    value, unit = float(item.find("div", class_="eoFy5xub4 rkn2Ylc1S RoFy5xub4 bBoFy5xub4 aoFy5xub4").text.replace('\xa0', ' ').rsplit(' ', 1)[0]),\
                                item.find("div", class_="eoFy5xub4 rkn2Ylc1S RoFy5xub4 bBoFy5xub4 aoFy5xub4").text.replace('\xa0', ' ').rsplit(' ', 1)[1]

                    if unit in ['г', 'мл']:
                        value, unit = value / 1000, 'кг' if unit == 'г' else 'л'

                    link = self.url_of_store + item.find("a", class_="lkn2Ylc1S").get('href')
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