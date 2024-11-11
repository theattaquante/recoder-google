import logging
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import mysql.connector
import datetime
import time 

logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s',
    level=logging.INFO)

class Discoverer:

    def __init__(self, urls=[]):
        self.visited_urls = []
        self.urls_to_visit = urls

    def download_url(self, url):
        logging.info(f'content: {requests.get(url).text}')
        return requests.get(url).text

    def get_linked_urls(self, url, html):
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a'):
            path = link.get('href')
            if path and path.startswith('/'):
                path = urljoin(url, path)
            yield path

    def url_to_database(self, url):
        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="test"
        )
        mycursor = mydb.cursor()
        sql = "INSERT INTO url (url, discoveredAt) VALUES (%s, %s)"
        val = (url, datetime.datetime.now())
        mycursor.execute(sql, val)
        mydb.commit()
        print(mycursor.rowcount, "record inserted.")
        mydb.close()

    def add_url_to_visit(self, url):
        
        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="test"
        )
        mycursor = mydb.cursor()
        sql = "SELECT ID FROM url WHERE url = \""+ url +"\""
        mycursor.execute(sql)
        
        myresult = mycursor.fetchall()
        if len(myresult) == 0:
            self.url_to_database(url)
            self.add_url_to_visit(url)
        
        mydb.close()
       

    def crawl(self, url):
        time.sleep(1)

        html = self.download_url(url)
        for url in self.get_linked_urls(url, html):
            self.add_url_to_visit(url)

    def run(self):
        while self.urls_to_visit:
            url = self.urls_to_visit.pop(0)
            logging.info(f'Crawling: {url}')
            try:
                self.crawl(url)
            except Exception:
                logging.exception(f'Failed to crawl: {url}')
            finally:
                self.visited_urls.append(url)

if __name__ == '__main__':
    Discoverer(urls=['https://www.pecheur.com']).run()