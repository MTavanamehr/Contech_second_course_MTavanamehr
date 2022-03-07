from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait

WEBDriver = "C:\Program Files\Google\Chrome\Application\chromedriver.exe"
driver = webdriver.Chrome(WEBDriver)

products = []
prices = []
ratings = []
page = 0

while True:
  page += 1
  page_ = str(page)
  url = 'https://www.amazon.com/s?k=laptop&crid=1JVMEFDL81Q5D&qid=1646545405&sprefix=lap%2Caps%2C502&page='+page_+''
  WebDriverWait(driver, 5)
  driver.get(url)
  content = driver.page_source
  soup = BeautifulSoup(content,'html.parser')

  for a in soup.findAll('div', attrs={'class':'a-section a-spacing-small a-spacing-top-small'}):
    if a.find('span', attrs={'class':'a-offscreen'}) is not None:
      name=a.find('span', attrs={'class':'a-size-medium a-color-base a-text-normal'})
      price=a.find('span', attrs={'class':'a-offscreen'})
      rating=a.find('span', attrs={'class':'a-icon-alt'})
      products.append(name.text)
      prices.append(price.text)
      if a.find('span', attrs={'class':'a-icon-alt'}) is not None:
        ratings.append(rating.text)
      else:
        ratings.append('No rate exist')
  if soup.find('span', {'class':'s-pagination-item s-pagination-next s-pagination-disabled'}) is not None:
    break

df = pd.DataFrame({'Product Name':products,'Price':prices,'Rating':ratings})
df.to_csv('amazon_laptop.csv', index=False, encoding='utf-8')