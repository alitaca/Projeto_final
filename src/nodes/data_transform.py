import pandas as pd
import numpy as np
import re

import os
import logging

logger = logging.getLogger('nodes.data_transform')

class Onibus:
    
    def __init__(self, client, params):
        self.client = client
        self.params = params
        self.path_2019 = params.raw_data+r'\\linha_onibus\\estudo_planilha_tarifa_2019_com_detalhamento_1546524534.xlsx'
        self.path_2020 = params.raw_data+r'\\linha_onibus\\planilha_de_custos_1577365324.xlsx'
        self.path_arq_pass = params.raw_data+r'\\onibus\\archive.zip'
        self.path_new_pass = [f'{params.raw_data}\\onibus\\{name}' for name in os.listdir(params.raw_data+r'\\onibus') if name != 'archive.zip']
    
    def pre_tratamento(self, df):
        ''' Esta funcao recebe um dataframe com dados nao tratados
            e retorna o dataframe tratado.

        Parameters
        ----------
        df : DataFrame
            dataframe com dados nao-tratados

        Returns
        -------
        df : DataFrame
            dataframe apos pre-tratamento

        '''
        
        # tratamento de colunas
        df['PAGANTES_DINHEIRO'] = df['Passageiros Pagtes Em Dinheiro']
        df['PAGANTES_BU_E_VT'] = df['Passageiros Comum e VT']+df['Passageiros Pgts Bu Comum M']+df['Passageiros Pgts Bu Vt Mensal']
        df['PAGANTES_ESTUDANTES'] = df['Passageiros Pagtes Estudante']+df['Passageiros Pgts Bu Est Mensal']
        df['TOTAL_ESTUDANTES'] = df['Passageiros Pagtes Estudante']+df['Passageiros Pgts Bu Est Mensal']+df['Passageiros Com Gratuidade Est']
        df['PASSAGEIROS_PAGANTES'] = df['Passageiros Pagantes']
        df['GRATUIDADES_OUTRAS'] = df['Passageiros Com Gratuidade']
        df['INTEGRACAO_ONIBUS'] = df['Passageiros Int Ônibus->Ônibus']
        df['TOTAL_PASSAGEIROS'] = df['Tot Passageiros Transportados']
        
        # excluindo colunas obsoletas
        df = df.drop(['Passageiros Pagtes Em Dinheiro','Passageiros Comum e VT','Passageiros Pgts Bu Comum M','Passageiros Pagtes Estudante',
                      'Passageiros Pgts Bu Est Mensal','Passageiros Pgts Bu Vt Mensal','Passageiros Pagantes','Passageiros Int Ônibus->Ônibus',
                      'Passageiros Com Gratuidade','Passageiros Com Gratuidade Est','Tot Passageiros Transportados', 'Empresa'], axis=1)
        
        return df
    
    def tratamento_passageiros(self, df):
        ''' Esta funcao recebe uma tabela com dados relativos as viagens de
            onibus, faz o tratamento das colunas, o tratamento de vazios e
            retorna um dataframe com os dados tratados.        

        Parameters
        ----------
        df : DataFrame
            dataframe com dados de passageiros de onibus.

        Returns
        -------
        df : DataFrame
            dataframe com dados tratados.

        '''
        
        # Tratamento de valores nulos
        df = df.dropna(thresh=10).reset_index(drop=True)
        df = df.fillna(0)

        # Tratamento de nomes de colunas
        df.columns = list(map(lambda x: x.lower(), df.columns))
        
        # Tratamento de colunas 
        df['area'] = df['area'].apply(lambda x: int(x.split('AREA ')[1]))
	
        df['n_linha'] = df['linha'].apply(lambda x: x.split(' - ')[0][:4]+'-'+x.split(' - ')[0][4:])
        df['desc_linha'] = df['linha'].apply(lambda x: x.split(' - ')[1])
        df = df.drop('linha', axis=1)
    
        df['data'] = pd.to_datetime(df['data'])

        df['fim_de_semana'] = df['data'].apply(lambda x: 0 if x.weekday()<5 else 1)

        df['semana'] = df['data'].apply(lambda x: x.isocalendar()[1])
        df['mes'] = df['data'].apply(lambda x: x.month)
        df['ano'] = df['data'].apply(lambda x: x.year)
        
        # Removendo dados duplicados
        df = df.drop_duplicates()
        
        # Tratamento de linhas de onibus desconhecidas
        df.drop(df.loc[df['n_linha'].apply(lambda x: True if len(re.findall('-\D', x))>0 else False)].index)
        
        logger.info('passageiros data treatment complete')
   
        return df
    
    def update_arq_onibus(self):
        ''' Esta funcao busca os arquivos historicos ate 2019 relacionados as 
            viagens de onibus, faz o tratamento das colunas, o tratamento de
            vazios e retorna um dataframe com os dados de passageiros de onibus.        

        Returns
        -------
        passageiros : DataFrame
            dataframe de dados de passageiros de onibus.

        '''
        
        # Importando dados de onibus
        passageiros = pd.read_csv(self.path_arq_pass, compression='zip')
        logger.info('archive passageiros loaded succefully')
        
        passageiros = self.tratamento_passageiros(passageiros)
        
        return passageiros
    
    def update_passageiros(self):
        ''' Esta funcao busca os arquivos relacionados as viagens de onibus,
            faz o tratamento das colunas, o tratamento de vazios e retorna
            um dataframe com os dados de passageiros de onibus.        

        Returns
        -------
        passageiros : DataFrame
            dataframe de dados de passageiros de onibus.

        '''
        
        # Importando dados de onibus
        passageiros = pd.read_csv(self.path_new_pass[0])
        
        for i in range(1, len(self.path_new_pass)):
            new_pass = pd.read_csv(self.path_new_pass[i])
            new_pass.columns = passageiros.columns
            passageiros = pd.concat([passageiros, new_pass])
            
        logger.info('passageiros csvs loaded succefully')
    
        passageiros = self.pre_tratamento(passageiros)
        passageiros = self.tratamento_passageiros(passageiros)
        
        logger.info('passageiros data treatment complete')
   
        return passageiros
    
    def update_linha(self, path_linha):
        ''' Esta funcao recebe um caminho de um arquivo, le as planilhas excel de custos
            de operacao, onde estao armazenadas informacoes de kilometragem das linhas
            de onibus, faz a leitura dos dados de linhas de onibus, faz o tratamento dos
            dados, e retorna um dataframe com dados de linhas de onibus.
            
        Input
        -------
        path_linha : str
            string de endereco do diretorio do arquivo
            
        Returns
        -------
        linha : DataFrame
            dataframe de dados de linhas de onibus

        '''
        
        # Importando dados de linhas de onibus
        linha_semana = pd.read_excel(path_linha, sheet_name='km dia útil', skiprows=6, usecols='A:E')
        linha_fds = pd.read_excel(path_linha, sheet_name='km dia sábado', skiprows=6, usecols='A:E')
        
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
        
        # Tratamento da coluna 'n_linha'
        linha_semana['n_linha'] = linha_semana['n_linha'].apply(lambda x: x+'0' if x.endswith('-1') else x)
        linha_fds['n_linha'] = linha_fds['n_linha'].apply(lambda x: x+'0' if x.endswith('-1') else x)
        
        logger.info('linhas data treatment complete')
        
        return pd.concat([linha_semana, linha_fds])
    
    def update_onibus(self):
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
        
        # Executando funcoes de extracao e tratamento de dados de passageiros
        query = '''SELECT ano FROM passageiros_onibus GROUP BY ano'''
        
        try:
            arq_ano =  np.array(pd.read_sql(query, con=self.client.conn))
            miss_arq = sum([0 if i in arq_ano else 1 for i in range(2015, 2020)])
        except:
            miss_arq = 5

        if miss_arq == 5:
            arq_passageiro = self.update_arq_onibus()
            novo_passageiro = self.update_passageiros()
            passageiro = pd.concat([arq_passageiro, novo_passageiro])
        else:
            passageiro = self.update_passageiros()
        
        # Executando funcoes de extracao e tratamento de dados de linhas de onibus
        linha_2019 = self.update_linha(self.path_2019)
        linha_2019['ano'] = 2019
        
        linha_2020 = self.update_linha(self.path_2020)
        linha_2020['ano'] = 2020
        
        linha = pd.concat([linha_2019, linha_2020])
        
        logger.info('treatment of onibus data is complete')
        
        # Uniao das tabelas de passageiros e linhas de onibus
        return passageiro, linha

class Metro:
    
    def __init__(self, client, params):
        self.client = client
        self.params = params
        self.path_metro = [f'{params.raw_data}\\metro\\{name}' for name in os.listdir(params.raw_data+r'\\metro') if name.endswith('csv')]

    def tratamento_metro(self, path_df):
        ''' Esta funcao recebe uma string de caminho de um csv, faz a extracao
            dos dados, faz o tratamento, e retorna um dataframe

        Parameters
        ----------
        path_df : str
            string de caminho de um csv

        Returns
        -------
        df : DataFrame
            dataframe tratado

        '''
        
        # importando dados de metro
        try:
            df = pd.read_csv(path_df, sep=';', encoding="ANSI", skiprows=3, usecols=range(6))
        except:
            df = pd.read_csv(path_df, header=None)
        
        # tratamento de dados
        df = df.dropna().transpose().reset_index().drop('index', axis=1)
        df.columns = ['linha', 'total', 'media_dia_util', 'media_sabado', 'media_domingo', 'maxima_diaria']
        df = df.drop(0)
        
        # tratamento de colunas numericas
        for col in df.columns:
            df[col] = df[col].apply(lambda x: 0 if x=='-' else x)
            
        df = df.apply(pd.to_numeric, errors='ignore')
        
        # inserindo colunas de mes e ano
        df['mes'] = re.findall('metro\\\\([A-Za-z]+)', path_df)[0].lower()
        df['ano'] = int(re.findall('metro\\\\\D*([0-9]+)', path_df)[0])
        
        return df
        
    def update_metro(self):
        ''' Esta funcao faz a leitura e tratamento dos dados de passageiros
            do metro, e retorna um dataframe

        Returns
        -------
        metro : DataFrame
            dataframe de dados de passageiros do metro

        '''
        
        # Importando dados de metro
        metro = self.tratamento_metro(self.path_metro[0])
        
        for i in range(1, len(self.path_metro)):
            new_metro = self.tratamento_metro(self.path_metro[i])
            metro = pd.concat([metro, new_metro])
        
        logger.info('metro data treatment complete')
   
        return metro
    
def update(client, params, update_list):
    ''' Esta funcao faz o carregamento de dados de passageiros e linhas de onibus,
        faz o tratamento dos dados, e retorna dois dataframes tratados.

    Parameters
    ----------
    client : class
        parametros de conexao da pipeline
    params : class
        parametros da pipeline

    Returns
    -------
    passageiro : DataFrame
        dataframe de dados de passageiros de onibus.
    linha : DataFrame
        dataframe de dados de linhas de onibus.

    '''
    
    if 'onibus' in update_list:
        onibus = Onibus(client, params)
        passag, lin = onibus.update_onibus()
    else:
        passag = 0
        lin = 0
    
    if 'metro' in update_list:
        metro = Metro(client, params)
        metr = metro.update_metro()
    else:
        metr = 0
    
    return passag, lin, metr

def done(client, params, update_list):
    if params.transform == True:
        return False
    else:
        return True