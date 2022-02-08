from bs4 import BeautifulSoup
from urllib.request import Request, urlopen

site= "https://www.world-airport-codes.com/alphabetical/airport-code/a.html?page=1"
hdr = {'User-Agent': 'Mozilla/5.0'}
req = Request(site,headers=hdr)
page = urlopen(req).read()
soup = BeautifulSoup(page)
table = soup.find(lambda tag: tag.name=='table' and tag.has_attr('class') and tag['class']=="stack2") 
rows = table.findAll(lambda tag: tag.name=='tr')

# print(soup)
# print(soup.find(id="eu-cookie-bar" ))