import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os

@task(retries=3)
def download_from_url(url: str, years: list):
    # 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-2004-01.csv'    
    
    for year in years:
        url_clean = url + f'ca-{year}-01.csv'
        
        os.system(f"wget {url_clean} -O 'ca-{year}-01.csv'")
        
        url_clean_2 = url + f'ca-{year}-02.csv'
        
        os.system(f"wget {url_clean_2} -O 'ca-{year}-02.csv'")
    
    return

@task(retries=3)
def clean_dfs():
    
    for file in os.listdir():
        if file[0:2] == 'ca':
            
            print('este arquivo está fodendo o role', file)
            
            df = pd.read_csv(file,
                 header=0,
                 sep=';',
                 decimal=',',
                 dtype={
                     'Valor de Venda': float,
                     'Valor de Compra': float
                 },
                 parse_dates=['Data da Coleta'],
                 dayfirst=True,
                 encoding='latin-1',
                 quotechar='"'
                 )
            
            columns_to_rename = {
            df.columns[0]: 'regiao_sigla', 
            df.columns[1]: 'estado_sigla', 
            df.columns[2]: 'municipio', 
            df.columns[3]: 'revenda',
            df.columns[4]: 'cnpj_revenda',
            df.columns[5]: 'endereço', 
            df.columns[6]: 'numero_rua', 
            df.columns[7]: 'complemento', 
            df.columns[8]: 'bairro',
            df.columns[9]: 'cep', 
            df.columns[10]: 'produto',
            df.columns[11]: 'data_da_coleta',
            df.columns[12]: 'valor_venda',
            df.columns[13]: 'valor_compra',
            df.columns[14]: 'unidade_medida',
            df.columns[15]: 'bandeira'
            }
            
            df.rename(columns=columns_to_rename)
            
            df.to_parquet(f'data/{file[:-4]}.parquet')
            
            os.remove(file)
            
    
    
    return
            
@task(retries=3)
def local_to_gcs():
    gcs_block = GcsBucket.load("project-zoomcamp")
        
    for file in os.listdir('data/'):
        
        path = 'data/' + file
        
        gcs_block.upload_from_path(
            from_path = f'{path}',
            to_path = f'parquet_files/{file}'
        )

        os.remove(path)
        
    return print('local_to_gcs succesful')   

@flow()
def elt_web_to_gcs() -> None:
    url = 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/'
    years = [2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021]
    
    #download_from_url(url, years)    
    clean_dfs()
    local_to_gcs()
        
if __name__ == "__main__":   
    elt_web_to_gcs()