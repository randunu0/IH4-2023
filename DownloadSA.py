import requests
from bs4 import BeautifulSoup

def DownloadSA(ERCOT_Website):
    url = ERCOT_Website
    r = requests.get(url, allow_redirects=True)
    base = 'https://sa.ercot.com'
    
    soup = BeautifulSoup(r.content, "html.parser")
    
    files = soup.find_all('a')
        
    links = []
    for file in files:
        link = file["href"]
        links.append(link)
    
    for i in range(len(links)):
        links[i] = base + links[i]
    
    for download in links:
        name = download.split('/')[-1]
        name = name.split('=')[-1] + ".zip"
        r = requests.get(download, allow_redirects=True)
        open(name, 'wb').write(r.content)
