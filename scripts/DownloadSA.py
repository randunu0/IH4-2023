import requests
import os
from bs4 import BeautifulSoup


def DownloadSA(ERCOT_Website, FolderName):
    url = ERCOT_Website
    r = requests.get(url, allow_redirects=True)
    base = 'https://sa.ercot.com'
    
    soup = BeautifulSoup(r.content, "html.parser")
    
    files = soup.find_all('a')
    
    filetype = soup.find_all('td', class_='labelOptional_ind')
        
    links = []
    i = 0
    for file in files:
        if filetype[i].text[-7:] == 'csv.zip':    
            link = file["href"]
            links.append(link)
        i = i + 1
    
    for i in range(len(links)):
        links[i] = base + links[i]
    
    if not os.path.isdir(FolderName):
        os.makedirs(FolderName)
    FolderPath = os.getcwd() + '\\' + FolderName
    
    for download in links:
        name = download.split('/')[-1]
        name = name.split('=')[-1] + ".zip"
        r = requests.get(download, allow_redirects=True)
        open(FolderPath + '\\' + name, 'wb').write(r.content)
