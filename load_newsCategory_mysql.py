import mysql.connector, toml, pandas as pd

# Creating session to connect to mysql database using connection params.
def mysql_connect():
    with open(r'selenium_tests\NAIRALAND_SCRAPE\details.toml', 'r') as f:
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
    cursor.execute('CREATE TABLE IF NOT EXISTS news_category (id INT PRIMARY KEY AUTO_INCREMENT, category_name VARCHAR(100), category_url VARCHAR(500))')
    df = pd.read_parquet(r'selenium_tests\NAIRALAND_SCRAPE\naira_sections.parquet')
    query = 'INSERT INTO news_category (category_name, category_url) VALUES (%s, %s)'
    for index, values in df.iterrows():
        cursor.execute(query, (values.SECTION, values.SECTION_URL))
    print(f'Successfully inputed {df.shape[0]} rows into database...')
    session.commit()
    session.close()
    return

# Main program
if __name__ == '__main__':
    load_news_category()