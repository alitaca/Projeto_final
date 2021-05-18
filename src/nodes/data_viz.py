import logging
import pandas as pd

logger = logging.getLogger('nodes.data_viz')

def update(client, params):
    ''' Esta funcao faz extracao de dados no database e salva os dados extraidos
        em arquivo csv.
    
    Parameters
    ----------
    client : class
        parametros de conexao da pipeline.
    params : class
        parametros da pipeline.

    Returns
    -------
    None.

    '''
    
    # Construindo query
    query = '''
    SELECT MIN(data) AS data, ano, mes, semana, pass_bus.fim_de_semana, tipo, area, pass_bus.n_linha, desc_linha,
        SUM(pagantes_dinheiro) AS pagantes_dinheiro, SUM(pagantes_bu_e_vt) AS pagantes_bu_e_vt,
        SUM(pagantes_estudantes) AS pagantes_estudantes, SUM(total_estudantes) AS total_estudantes,
        SUM(passageiros_pagantes) AS passageiros_pagantes, SUM(gratuidades_outras) AS gratuidades_outras,
        SUM(integracao_onibus) AS integracao_onibus, SUM(total_passageiros) AS total_passageiros,
        AVG(extensao_ida) AS extensao_ida, AVG(extensao_volta) AS extensao_volta,
        SUM(partidas_ida) AS partidas_ida, SUM(partidas_volta) AS partidas_volta
    FROM passageiros_onibus AS pass_bus
    LEFT JOIN linha ON linha.n_linha = pass_bus.n_linha
        AND linha.fim_de_semana = pass_bus.fim_de_semana
    GROUP BY ano, mes, semana, pass_bus.fim_de_semana, tipo, area, pass_bus.n_linha, desc_linha'''
    
    # Extraindo tabela do SQL
    onibus = pd.read_sql(query, con=client.conn)
    
    # Salvando resultado em csv
    onibus.to_csv(params.processed_data+r'\\onibus.csv', index=False)
    
    
def done(client, params):
	pass