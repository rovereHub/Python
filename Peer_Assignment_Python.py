#************** WebScraping *************


from bs4 import BeautifulSoup
import html5lib
import requests
import pandas as pd
import lxml

html_data = requests.get('https://en.wikipedia.org/wiki/List_of_largest_banks').text
#print(html_data)
print(html_data[483:506])

soup = BeautifulSoup(html_data, 'lxml')
data = pd.DataFrame(columns=["Name", "Market Cap (US$ Billion)"])

for row in soup.find_all('tbody')[2].find_all('tr'):
    cols = row.find_all('td')
    #Write your code here
    if len(cols) == 0:
        continue
    else: 
           data = data.append({#'Rank': col[0].text.strip(),
                         'Name': cols[1].text.strip(),
                         'Market Cap (US$ Billion)': cols[2].text.strip()}, ignore_index=True)
data.head()

#data.to_json("bank_market_cap.json")




#************** Extract API Data ************


import requests
import pandas as pd
import json

url = "https://api.apilayer.com/exchangerates_data/latest?base=EUR&apikey=GtqnvvLuz7sFX6NcIPIk4IhYh6oD4MSR"

html_data = requests.get(url).text
data_text = str(html_data).splitlines()[6:-2]

data = pd.DataFrame(columns = ["Currency","Rates"])
for row in data_text:
    col = row.split(":")
    Currency = col[0].strip()[1:-1]
    Rates = col[1].replace(",","")
    
    
    data = data.append({"Currency":Currency,"Rates":Rates},ignore_index=True)
data.set_index('Currency',inplace=True)
data.head(5)


data.to_csv("exchange_rates_1.csv")


#************** ETL *************


import glob
import pandas as pd
from datetime import datetime
import wget

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe

columns=['Name','Market Cap (US$ Billion)']

def extract():
    df = extract_from_json('bank_market_cap_1.json')
    df.columns = columns
    return df

exc_rate = pd.read_csv('exchange_rates.csv',index_col=0)
exc_rate = exc_rate.loc['GBP',:]
print(exc_rate)



def transform(market, exc_rate):
    # Write your code here
    #market = extract()
    market['Market Cap (GBP$ Billion)'] = market['Market Cap (US$ Billion)']
    market['Market Cap (GBP$ Billion)']*=exc_rate['Rates'].tolist()
    market.drop('Market Cap (US$ Billion)',axis=1, inplace=True)
    return market

def load(data):
    data.to_csv('bank_market_cap_gbp.csv')

def log(log):
    with open('log.txt', 'a') as f:
        f.write('{}\n'.format(log))
    # Write your code here


# Write your code here
log('ETL Job Started')

log('Extract phase Started')

df = extract()
# Print the rows here
df.head()

log("Extract phase Ended")

log('Transform phase Started')

# Call the function here
df = transform(extract(),exc_rate)
# Print the first 5 rows here
df.head()

# Write your code here
log('Transform phase Ended')

# Write your code here
log('Load phase Started')

# Write your code here
load(df)

# Write your code here
log('Load phase Ended')