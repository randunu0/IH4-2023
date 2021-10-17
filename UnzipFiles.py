import os
from zipfile import ZipFile


def Unzip(folder):
    files = os.listdir(folder)
    
    for file in files:
        with ZipFile(folder + '\\' + file) as ZipObj:
            ZipObj.extractall(folder)
        os.remove(folder + '\\' + file)
        