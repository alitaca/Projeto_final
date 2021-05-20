import logging
from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
import time
import re

logger = logging.getLogger('nodes.data_gathering')


def update(client, params, list_gather):
    ''' Esta funcao faz a busca de dados nas urls de interesse,
        e salva os arquivos com os dados na pasta 'raw'.    

    Parameters
    ----------
    client : class
        parametros de conexao da pipeline
    params : class
        parametros da pipeline
    list_gather : list
        lista de strings dos dados que devem ser atualizados

    Returns
    -------
    None.

    '''
    
    def get_onibus():
        ''' Esta funcao faz uma busca de todos os arquivos de passageiros no
            ano de 2020 e 2021 no site da SPTrans, carrega os dados e salva
            em arquivo csv.
        

        Returns
        -------
        None.

        '''
        
        # data url's
        try:
            url_2021 = requests.get("https://www.prefeitura.sp.gov.br/cidade/secretarias/transportes/institucional/sptrans/acesso_a_informacao/index.php?p=306932").text
            url_2020 = requests.get("https://www.prefeitura.sp.gov.br/cidade/secretarias/transportes/institucional/sptrans/acesso_a_informacao/index.php?p=292723").text
            
            logger.info('url conection loaded')
        except:
            logger.warning('url connection failed')
        else:
            # 2021
            soup_2021 = BeautifulSoup(url_2021, "lxml")
            links_2021 = [i["href"] for i in soup_2021.findAll("a", href=True) if i["href"].endswith(".xls")
                          and "Consolidad" not in i["href"] and "Pass_Transp" not in i["href"]]
            
            # 2020
            soup_2020 = BeautifulSoup(url_2020, "lxml")
            links_2020 = [i["href"] for i in soup_2020.findAll("a", href=True) if i["href"].endswith(".xls")
                          and "Consolidad" not in i["href"] and "Pass_Transp" not in i["href"]]
            
            # Identificando arquivos novos
            arq_links = os.listdir(params.raw_data+r'\\onibus')
            all_links = [url for url in (links_2020 + links_2021) if re.sub('(\(\w\))?(xls)?', '', url.split('upload/')[1])+'csv' not in arq_links]
            
            missing = []
            
            for url in all_links:
                try:
                    onibus_dia = pd.read_excel(url, skiprows=2)
                except:
                    missing.append(re.findall('upload/(\w*)', url)[0])
                else:
                    dia = re.findall('upload/(\w*)', url)[0]+'.csv'
                    onibus_dia.to_csv(params.raw_data+r'\\onibus\\'+dia, index=False)
                
                time.sleep(1)
            
            if len(missing)>0:
                logger.warning(f'the following files were missing: {missing}')
            else:
                logger.info('files loaded succefully')
            
            
    def get_metro():
        ''' Esta funcao faz uma busca de todos os arquivos de entrada de passageiros
            por linha no site do metro, carrega os dados e salva em arquivo csv.        

        Returns
        -------
        None.

        '''
        
        # extraindo dados
        try:
            url_metro = requests.get("https://transparencia.metrosp.com.br/dataset/demanda", verify=False).text
            
            logger.info('metro url conection loaded')
        except:
            logger.warning('metro url connection failed')
            all_links = []
        else:
            soup_metro = BeautifulSoup(url_metro, features='lxml')
            links_metro = [i["href"] for i in soup_metro.findAll("a", href=True) if i["href"].endswith(".csv")
                           and "Entrada" in i["href"] and "Linha" in i["href"]]
            
            # Identificando arquivos novos
            arq_links = os.listdir(params.raw_data+r'\\metro')
            all_links = [url for url in links_metro if re.sub('(%20)?-?', '', re.sub('%C3%A7', 'c', url)).split('Linha')[1] not in arq_links]
            
            missing = []
            
            for url in all_links:
                try:
                    metro_mes = pd.read_csv(url, sep=';', encoding="ANSI", skiprows=4, usecols=range(6))
                except:
                    missing.append(re.findall('upload/(\w*)', url)[0])
                else:
                    mes = re.sub('(%20)?-?', '', re.sub('%C3%A7', 'c', url)).split('Linha')[1]
                    metro_mes.to_csv(params.raw_data+r'\\metro\\'+mes, index=False)
                
                time.sleep(1)
            
            if len(missing)>0:
                logger.warning(f'the following files were missing: {missing}')
            else:
                logger.info('metro files loaded succefully')

    # carregando dados de passageiros de onibus
    if 'onibus' in list_gather:
        get_onibus()
    
    # carregando dados de passageiros de metro
    if 'metro' in list_gather:
        get_metro()

    return None
    
def done(client, params):
    ''' Esta funcao verifica se os arquivos de passageiros diarios de onibus
        estao atualizados.

    Parameters
    ----------
    client : class
        parametros de conexao da pipeline
    params : class
        parametros da pipeline

    Returns
    -------
    obsoleto : list
        lista de strings que representa os dados que estao obsoletos

    '''
    
    # verificando comando forcado
    if params.gathering == False:
        return []
    
    def verifica_onibus():
        ''' Esta funcao verifica se ha dados novos no site da SPTrans sobre passageiros
            de onibus.

        Returns
        -------
        str
            string 'onibus', caso haja dados novos para atualizar, e 'None' caso contrario

        '''
        
        # Extraindo informacoes da internet
        try:
            url_2021 = requests.get("https://www.prefeitura.sp.gov.br/cidade/secretarias/transportes/institucional/sptrans/acesso_a_informacao/index.php?p=306932").text
            url_2020 = requests.get("https://www.prefeitura.sp.gov.br/cidade/secretarias/transportes/institucional/sptrans/acesso_a_informacao/index.php?p=292723").text
            
            logger.info('url conection loaded')
        except:
            logger.warning('url connection failed')
            all_links = []
        else:
            # 2021
            soup_2021 = BeautifulSoup(url_2021, "lxml")
            links_2021 = [i["href"] for i in soup_2021.findAll("a", href=True) if i["href"].endswith(".xls")
                          and "Consolidad" not in i["href"] and "Pass_Transp" not in i["href"]]
            
            # 2020
            soup_2020 = BeautifulSoup(url_2020, "lxml")
            links_2020 = [i["href"] for i in soup_2020.findAll("a", href=True) if i["href"].endswith(".xls")
                          and "Consolidad" not in i["href"] and "Pass_Transp" not in i["href"]]
            
            # Identificando arquivos novos
            arq_links = os.listdir(params.raw_data+r'\\onibus')
            all_links = [url for url in (links_2020 + links_2021) if re.sub('(\(\w\))?(xls)?', '', url.split('upload/')[1])+'csv' not in arq_links]
        
        # verificando se ha dados novos
        if len(all_links)>0:
            logger.info('new onibus data is available')
            return 'onibus'
        else:
            logger.info('onibus data is up to date')
            return None
        
    def verifica_metro():
        ''' Esta funcao verifica se ha dados novos no site do metro sobre passageiros
            por linha.

        Returns
        -------
        str
            string 'metro', caso haja dados novos para atualizar, e 'None' caso contrario

        '''
        
        # extraindo dados
        try:
            url_metro = requests.get("https://transparencia.metrosp.com.br/dataset/demanda", verify=False).text
            
            logger.info('metro url conection loaded')
        except:
            logger.warning('metro url connection failed')
            all_links = []
        else:
            soup_metro = BeautifulSoup(url_metro, features='lxml')
            links_metro = [i["href"] for i in soup_metro.findAll("a", href=True) if i["href"].endswith(".csv")
                           and "Entrada" in i["href"] and "Linha" in i["href"]]
            
            # Identificando arquivos novos
            arq_links = os.listdir(params.raw_data+r'\\metro')
            all_links = [url for url in links_metro if re.sub('(%20)?-?', '', re.sub('%C3%A7', 'c', url)).split('Linha')[1] not in arq_links]
            
            if len(all_links)>0:
                logger.info('new metro data is available')
                return 'metro'
            else:
                logger.info('metro data is up to date')
                return None
                
    # inicializando variaveis
    obsoleto = []
    obsoleto.append(verifica_onibus())
    obsoleto.append(verifica_metro())
    
    return obsoleto