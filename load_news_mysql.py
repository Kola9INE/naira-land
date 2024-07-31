"""
Users of this code should include their
connection parameters for their MySQL
database as a 'details.toml' file.
"""

import mysql.connector, toml, pandas as pd

# Creating session to connect to mysql database using connection params.
def mysql_connect():
    with open(r'details.toml', 'r') as f:
        file = toml.load(f)
    
    host, username, password = (
        file['connection']['host'],
        file['connection']['username'],
        file['connection']['password']
    )

    mydb = mysql.connector.connect(
        host = host,
        user = username, 
        password = password,
    )
    return mydb

# Loading extracted and transformed data from location to mysql using the mysql session created above.
def load_news_category():
    session = mysql_connect()
    cursor = session.cursor()
    cursor.execute(
        'CREATE DATABASE IF NOT EXISTS NAIRALAND'
    )
    cursor.execute('USE NAIRALAND')
    cursor.execute('CREATE TABLE IF NOT EXISTS nairanews (ID INT PRIMARY KEY AUTO_INCREMENT, TITLE VARCHAR(300) NOT NULL, TITLE_URL VARCHAR(400) NOT NULL, DATE VARCHAR(10) NOT NULL, TIME VARCHAR(10) NOT NULL, UNIQUE(title, title_url))')
    df = pd.read_parquet(r'selenium_tests\NAIRALAND_SCRAPE\nairanews.parquet')
    try:
        query = 'INSERT IGNORE INTO nairanews (TITLE, TITLE_URL, DATE, TIME) VALUES (%s, %s, %s, %s)'
        for index, values in df.iterrows():
            cursor.execute(query, (values.TITLE, values.URL, values.DATE, values.TIME))
        print(f'Successfully inputed {df.shape[0]} rows into database...')
        return
    except mysql.connector.Error as err:
        print('Insert duplicate records error', err)
    finally:
        session.commit()
        session.close()

# Main program
if __name__ == '__main__':
    load_news_category()