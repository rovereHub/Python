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
