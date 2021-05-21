import logging
import pandas as pd

logger = logging.getLogger('nodes.data_storage')


def update(client, params, df, nome):
    ''' Esta funcao recebe um dataframe e faz o carregamento no database do SQL
        na tabela de nome 'nome'.

    Parameters
    ----------
    client : class
        parametros de conexao da pipeline.
    params : class
        paramatros da pipeline.
    df : DataFrame
        dataframe de dados a serem carregados no database.
    nome : str
        string com o nome da tabela

    Returns
    -------
    None.

    '''
    
    try:
        df.to_sql(name=nome, con=client.conn, if_exists='append', index=False)
        logger.info(f'The data was loaded in {params.database} Database')
    except ValueError:
        logger.warning('Data not loaded in database')
        
        
def done(client, params):
    if params.storage == True:
        return False
    else:
        return True