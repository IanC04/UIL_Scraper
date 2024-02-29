import requests
from bs4 import BeautifulSoup
import csv

URL = "https://utdirect.utexas.edu/nlogon/uil/vlcp_pub_arch.WBX"
r = requests.get(URL)

soup = BeautifulSoup(r.content, 'html.parser')