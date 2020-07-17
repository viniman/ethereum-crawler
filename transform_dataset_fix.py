import pandas as pd


# Data read
dfRead = pd.read_csv('../DataScience/ethereum/datasets/dataset_20190812.csv')
fix_data = pd.read_csv('text.txt', skiprows=range(1, 100000, 2), sep=' ')


# ---------------------------------------------------
# Data preprocessing

# pass to datetime type
dfRead.joined_pool = pd.to_datetime(dfRead.joined_pool)
dfRead.joined_chain = pd.to_datetime(dfRead.joined_chain)

# sort values by [joined_chain, joined_pool]
dfRead = dfRead.sort_values(by=['joined_chain', 'joined_pool'])
dfRead = dfRead.reset_index(drop=True)

# create time_pending column with data type
dfRead['time_pending_date'] = dfRead.joined_chain - dfRead.joined_pool

# time_pending column date type (datetime64) => int
dfRead['time_pending'] = pd.to_timedelta(dfRead.time_pending_date).dt.total_seconds().astype(int)

# Take off the negatives
dfRead = dfRead[dfRead.time_pending > 0]

# Inverting classes
dfRead.receipt_status = 1 - dfRead.receipt_status

# Sampling the dataset
dfRead = dfRead.sample(50000, random_state=2)
# ---------------------------------------------------


# Confere os dados
fix_data = pd.read_csv('text.txt', skiprows=range(1, 100000, 2), sep=' ')
cont=0
fix_data = fix_data.sort_values(by=['id'])
dfRead = dfRead.sort_index()
dfRead['id2'] = fix_data.iloc[:,0].values
print(dfRead)
for index, row in dfRead.iterrows():
    if index == int(row['id2']):
        cont +=1
dfRead = dfRead.drop(columns='id2')
print('Valor iguais de ID\'s:', cont)


# Fix the data
dfRead['gas'] = fix_data.iloc[:,1].values#['gas']
dfRead['value'] = fix_data.iloc[:,2].values#['value']


print(dfRead)
print(fix_data)

# Save data

dfRead.to_csv('../DataScience/ethereum/datasets/dataset_with_gas.csv', index=False)