from random import randint
from time import sleep
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import pandas as pd
from datetime import datetime

def nairaland_sections():
    try:
        driver.get('https://www.nairaland.com')
        driver.maximize_window()
        title = driver.find_element(By.XPATH, '/html/body/div/table[2]/tbody')
        title = title.find_elements(By.TAG_NAME, 'a')
        title_text = [titles.text.strip() for titles in title]
        title_url = [titles.get_attribute('href') for titles in title]
        print('Reconstructing data into columnar format...')
        data = {
            'SECTION': title_text,
            'SECTION_URL': title_url
        }
        path = r'C:\Users\Kola PC\Desktop\examples\selenium_tests\NAIRALAND_SCRAPE\naira_sections.parquet'
        print(f'Saving data as parquet at {path}')
        df = pd.DataFrame(data)
        df.to_parquet(path)
        rows, columns = df.shape
        print('DATA INFO:\n')
        print(f'There are {rows} rows and {columns} columns in saved data.')
        return
    except:
        print('There is something wrong...')
        return

def naira_news():
    try:
        driver.get('https://www.nairaland.com/news')
        driver.maximize_window()
        TITLE = []
        URL = []
        x = 0
        while x <= 20:
            title = driver.find_element(By.XPATH, '/html/body/div/table[2]/tbody/tr/td')
            titles = title.find_elements(By.TAG_NAME, 'a')
            title_text = [title.text.strip() for title in titles]
            title_url = [title.get_attribute('href') for title in titles]
            TITLE.extend(title_text)
            URL.extend(title_url)

            x+=1

            driver.execute_script('arguments[0].scrollIntoView(true);',driver.find_element(By.XPATH, '/html/body/div/p[5]'))
            next = driver.find_element(By.CSS_SELECTOR, 'body > div > p:nth-child(9)')
            next.find_element(By.LINK_TEXT, f"({x+1})").click()
            sleeping = randint(3,7)
            sleep(sleeping)
        
        df = pd.DataFrame({
            'TITLE':TITLE,
            'URL':URL,
            'TIME':datetime.now().strftime('%H:%M:%S'),
            'DATE':datetime.now().strftime('%Y-%m-%d')
        })
        path = r'C:\Users\Kola PC\Desktop\examples\selenium_tests\NAIRALAND_SCRAPE\nairanews.parquet'
        print(f'Saving data as parquet in {path}')
        df.to_parquet(path)
        rows, columns = df.shape
        print(f'We have scraped {len(TITLE)} news from Nairaland,\nYour data contains: {rows} rows and {columns} columns.\nSaved at {path},')
        return
    except:
        print('There is error somewhere...')
        return

def request():
    try:
        print('Welcome! We can scrape the categories and titles for you.\nJust enter your request below as "category" or "news".')
        request = input('Enter your request here as NEWS or CATEGORIES\n')
        if request.strip().lower() == 'news':
            naira_news()
        elif request.strip().lower() == 'categories':
            nairaland_sections()
    except:
        print('Error...')

if __name__ == '__main__':
    try:
        service = Service()
        options = Options() 
        options.add_experimental_option('detach', True)
        options.add_argument('--headless')
        driver = webdriver.Chrome(service = service, options = options)
        request()
        sleep(5)
    except:
        'Something is wrong... \nConnect to the internet or check your code!'
    finally:
        driver.close()
