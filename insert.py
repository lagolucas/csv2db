from json import load
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import glob
import os

import pandas as pd

with open("config.json") as jsonfile: # pode ser necessário corrigir o caminho
    db_config = load(jsonfile)['database']

engine = create_engine(URL.create(db_config['drivername'], db_config['username'], db_config['password'], db_config['host'],
                                  db_config['port'], db_config['database']))


for file in glob.glob("*.csv"): # substituir o * pelo nome do arquivo
    df = pd.read_csv(file, delimiter=',', quotechar='"')
    df = df.assign(query=file.split('_')[0], captured_in=int(file.split('_')[1].replace(".csv", "")))
    df.to_sql('tabela', engine, if_exists='append') # nome da tabela que vai ser feito o upload
    os.rename(file, file+'.old') # eu gosto de renomear o arquivo que já usei como .old 
