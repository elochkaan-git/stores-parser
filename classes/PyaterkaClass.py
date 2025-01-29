from classes.BaseClass import *
import re
import sys
import time

class PyaterkaParser (BaseParser):

    def __init__ (self, adress: str, urls: List[str]) -> None:
        super().__init__(adress, urls)
        self.url_of_store = 'https://5ka.ru/catalog'

    def adress_setup (self) -> None:
        self.driver.get(self.url_of_store)

        try:
            WebDriverWait(self.driver, 20).until(
                ec.element_to_be_clickable((By.XPATH, "//*[@class='chakra-button k6J7twGM- css-12yf9td']"))
            )

            WebDriverWait(self.driver, 20).until(
                ec.element_to_be_clickable((By.XPATH, "//*[@class='chakra-button k6J7twGM- css-12yf9td']"))
            ).click()

            adress_input = WebDriverWait(self.driver, 20).until(
                ec.element_to_be_clickable((By.XPATH, '/html/body/div[3]/div[3]/div/section/div/div[2]/div/div[2]/div/input'))
            )
            for letter in self.adress:
                adress_input.send_keys(letter)

            self.adress = self.adress.split(' ', 1)[1]
            WebDriverWait(self.driver, 20).until(
                ec.element_to_be_clickable((By.XPATH, f'//p[contains(text(), "{self.adress}")]'))
            ).click()

            button_accept = WebDriverWait(self.driver, 20).until(
                ec.element_to_be_clickable((By.XPATH, '//button[@class="chakra-button nRbDkUwL- css-j9bhfa"]'))
            )
            button_accept.click()
        except:
            self.driver.quit()
            log.error(f'В процессе {multiprocessing.current_process().pid} произошла ошибка - адресс не установлен. Пожалуйста, перезапустите программу')
            log.error(traceback.format_exc())
            sys.exit()

        log.info(f'Адрес установлен для браузера в процессе {multiprocessing.current_process().pid}')

    def scraping (self) -> Tuple[str, float, str, float, float, str, str]:
        time.sleep(3)
        log.info('Начинаем обрабатывать страницы')
        log.debug(f'Текущий адрес: {self.driver.find_element(By.XPATH, '//p[@class="chakra-text fyOFNehN- SdFzIDV1- css-15mul6p"]').text}')
        for url in self.urls:
            log.debug(f'Процесс {multiprocessing.current_process().pid} обрабатывает {url}')
            self.driver.get(url)
            try:
                WebDriverWait(self.driver, 20).until(
                    ec.presence_of_element_located((By.XPATH, '//div[@class="chakra-stack KnkuqE3h- fLmfW7LE- css-8g8ihq"]'))
                )
            except:
                log.warning(f'В данный момент сайт {url} недоступен или на нем отсутствуют товары')
            soup = bs(self.driver.page_source, 'html.parser')

            for item in soup.select("[class='chakra-stack KnkuqE3h- fLmfW7LE- css-8g8ihq']"):
                name = item.find("p", class_="chakra-text SdLEFc2B- css-1jdqp4k").text
                value, unit = float(re.sub(r'\D', '', item.find("p", class_="chakra-text hPKYUDdM- css-15thl77").text.rsplit(' ', 1)[0])),\
                            item.find("p", class_="chakra-text hPKYUDdM- css-15thl77").text.rsplit(' ', 1)[1]

                if unit in ['г', 'мл']:
                    value, unit = value / 1000, 'кг' if unit == 'г' else 'л'

                link = self.url_of_store + item.find("a", class_="chakra-link xlSVIYdp- css-13jvj27").get('href')
                try:
                    rating = item.find("p", class_="chakra-text o1tGK2uB- css-1jdqp4k").text
                except:
                    rating = None
                cost = float(re.sub(r'\D', '', item.select("div.j_IdgaDq-.css-k008qs p")[0].text) + '.' + re.sub(r'\D', '', item.select("div.j_IdgaDq-.css-k008qs p")[1].text))

                log.debug(f'{(name, value, unit, rating, cost, link, "Пятерочка")} переданы в базу данных')
                yield (name, value, unit, rating, cost, link, "Пятерочка")