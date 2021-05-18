import pandas as pd

import os
import logging

logger = logging.getLogger('nodes.data_transform')


def update_onibus(client, params):
    ''' Esta funcao faz a extracao e tratamento de dados de passageiros e de 
        linhas de onibus, e retorna dois dataframes.

    Parameters
    ----------
    client : class
        parametros de conexao da pipeline.
    params : class
        paramatros da pipeline.

    Returns
    -------
    passageiro : DataFrame
        dataframe de dados de passageiros de onibus.
    linha : DataFrame
        dataframe de dados de linhas de onibus.


    '''
    
    def update_passageiros():
        ''' Esta funcao busca os arquivos relacionados as viagens de onibus,
            faz o tratamento das colunas, o tratamento de vazios e retorna
            um dataframe com os dados de passageiros de onibus.        

        Returns
        -------
        passageiros : DataFrame
            dataframe de dados de passageiros de onibus.

        '''
        
        # Importando dados de onibus
        passageiros = pd.read_csv(f'{params.raw_data}/archive.zip', compression='zip')
        logger.info('passageiros csv loaded succefully')
    
        # Tratamento de valores nulos
        passageiros = passageiros.dropna(thresh=10).reset_index(drop=True)
        passageiros = passageiros.fillna(0)

        # Tratamento de nomes de colunas
        passageiros.columns = list(map(lambda x: x.lower(), passageiros.columns))
        
        # Tratamento de colunas 
        passageiros['area'] = passageiros['area'].apply(lambda x: int(x.split('AREA ')[1]))
	
        passageiros['n_linha'] = passageiros['linha'].apply(lambda x: x.split(' - ')[0][:4]+'-'+x.split(' - ')[0][4:])
        passageiros['desc_linha'] = passageiros['linha'].apply(lambda x: x.split(' - ')[1])
    
        passageiros['data'] = pd.to_datetime(passageiros['data'])

        passageiros['fim_de_semana'] = passageiros['data'].apply(lambda x: 0 if x.weekday()<5 else 1)

        passageiros['semana'] = passageiros['data'].apply(lambda x: x.isocalendar()[1])
        passageiros['mes'] = passageiros['data'].apply(lambda x: x.month)
        passageiros['ano'] = passageiros['data'].apply(lambda x: x.year)
        
        logger.info('passageiros data treatment complete')
   
        return passageiros
    
    def update_linha():
        ''' Esta funcao busca as planilhas excel de custos de operacao, onde estao armazenadas
            informacoes de kilometragem das linhas de onibus, faz a leitura dos dados de
            linhas de onibus, faz o tratamento dos dados, e retorna um dataframe com
            dados de linhas de onibus.

        Returns
        -------
        linha : DataFrame
            dataframe de dados de linhas de onibus

        '''
        
        # Importando dados de linhas de onibus
        linha_semana = pd.read_excel(params.raw_data+r'\\planilha_de_custos_1577365324.xlsx', sheet_name='km dia útil', skiprows=6, usecols='A:E')
        linha_fds = pd.read_excel(params.raw_data+r'\\planilha_de_custos_1577365324.xlsx', sheet_name='km dia sábado', skiprows=6, usecols='A:E')
        
        logger.info('linha excel file loaded succefully')
        
        # Tratamento de nome de colunas
        linha_semana.columns = ['n_linha', 'extensao_ida', 'extensao_volta', 'partidas_ida', 'partidas_volta']
        linha_fds.columns = ['n_linha', 'extensao_ida', 'extensao_volta', 'partidas_ida', 'partidas_volta']
        
        # Tratamento de valores nulos
        linha_semana = linha_semana.drop(0)
        linha_semana = linha_semana.dropna(thresh=4).reset_index(drop=True)
        linha_semana = linha_semana.fillna(0)
        
        linha_fds = linha_fds.drop(0)
        linha_fds = linha_fds.dropna(thresh=4).reset_index(drop=True)
        linha_fds = linha_fds.fillna(0)
        
        # Acrescentando coluna de flag de fim de semana
        linha_semana['fim_de_semana']=0
        linha_fds['fim_de_semana']=1
        
        # Alterando tipo das colunas
        linha_semana = linha_semana.apply(pd.to_numeric, errors='ignore')
        
        logger.info('linhas data treatment complete')
        
        return pd.concat([linha_semana, linha_fds])
    
    # Executando funcoes de extracao e tratamento de dados
    passageiro = update_passageiros()
    linha = update_linha()
    
    # Uniao das tabelas de passageiros e linhas de onibus
    return passageiro, linha
    
def done_onibus(client, params):
	pass