import requests as req
from bs4 import BeautifulSoup
import threading
import os
from zipfile import ZipFile

url = 'https://s3.amazonaws.com/tripdata/'


def get_lsit_of_zips():
    res = req.get(url)
    soup = BeautifulSoup(res.text, 'xml')
    data_files = soup.find_all('Key')
    a = []

    for file in range(len(data_files) - 1):
        a.append(data_files[file].get_text())

    return a


def d(a, n):
    k, m = divmod(len(a), n)
    return [a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]


def f1(a):
    for z in zip_files:
        tgt_zip_file = 'C:/citibike_tripdata/' + z
        f = open(tgt_zip_file, 'wb')
        response = req.get(url + z)
        f.write(response.content)
        f.close()
        # zip_ref = ZipFile(tgt_zip_file, 'r')
        # zip_ref.extractall('C:/citibike_tripdata/')
        # zip_ref.close()
        # os.remove(tgt_zip_file)


zip_files = get_lsit_of_zips()
divided_arr = d(zip_files, 4)

if __name__ == '__main__':
    for i in range(4):
        print("Thread: " + str(i), "arr len: " + str(len(divided_arr[i])))
        my_thread = threading.Thread(target=f1, args=(divided_arr[i],))
        my_thread.start()

