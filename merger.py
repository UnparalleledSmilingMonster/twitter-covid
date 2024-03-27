import csv

from pathlib import Path
from glob import glob

import pandas as pd
import os


ignore = []
    

p = Path('./data/')

directories =[str(f) for f in p.iterdir() if f.is_dir()]

for directory in directories :
    filenames = [i for i in glob(directory+"/*.csv")]  
    for f in filenames : 
        try :
            csv_data= pd.read_csv(f, delimiter='|',lineterminator="\n")
        
        except Exception :
            ignore.append(f)
            pass
            
            
            
print("Here are the corrupted .csv files : \n", ignore)
            


for directory in directories :     

        filenames = [i for i in glob(directory+"/*.csv")]      

        combined_csv_data= pd.concat([pd.read_csv(f, delimiter='|',lineterminator="\n") if f not in ignore else None for f in filenames ])
        combined_csv_data.to_csv("./data/"+directory[5:-4]+"_merged.csv", sep='|',index=False)
    


print("The csv file for each file was created.")

filenames_merged= [i for i in glob( "data/*merged.csv")]      
csv_data= pd.concat([pd.read_csv(f, delimiter='|',lineterminator="\n") for f in filenames_merged ])
csv_data.to_csv("./data/dataset.csv",sep='|',index=False)


print("Merged the dataset in a unique csv file")
