import pandas as pd
from itertools import chain

nrows = 97722


read_path = '../DataScience/ethereum/datasets/dataset_hash_to_fix.csv'


nrows = 97722
dataset_size = 6840554


for instance in range(0, 70):
    print('instance', instance)
    
    if(instance != 69):
        skiprows = chain(range(1,instance*nrows+1), range((instance+1)*nrows+1, dataset_size))
        hash_transactions = pd.read_csv(read_path, nrows=nrows, skiprows=skiprows)
    else:
        print('entra sim cara')
        skiprows = range(1,instance*nrows+1)#, range((instance+1)*nrows+1, dataset_size-14))
        hash_transactions = pd.read_csv(read_path, skiprows=skiprows)

    
    
    print('Qtd:', hash_transactions.count())
    
    write_path = 'dataset_hash/dataset_hash_to_fix_' + str(instance) + '.csv'
    hash_transactions.to_csv(write_path, index=False)



