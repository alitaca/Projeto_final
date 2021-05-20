from nodes import data_gathering
from nodes import data_transform
from nodes import data_storage
from nodes import data_viz
from nodes import data_preparation

from params import Params 
from client import Client
import logging

def process(client, params):  
	"""
	The ETL pipeline.

	It contains the main nodes of the extract-transform-load 
	pipeline from the process. 
	"""
	data_preparation.run(client, params)
    
	gather_done = data_gathering.done(client, params)
	if len(gather_done)>0:
		data_gathering.update(client, params, gather_done)
        
	gather_done = ['onibus']


	if not data_transform.done(client, params, gather_done):
		df_pass, df_linha, df_metro = data_transform.update(client, params, gather_done)
		nome_pass = 'passageiros_onibus'
		nome_linha = 'linha'
		nome_metro = 'metro'

	if not data_storage.done(client, params): 
		data_storage.update(client, params, df_pass, nome_pass)
		data_storage.update(client, params, df_linha, nome_linha)
		data_storage.update(client, params, df_metro, nome_metro)

	if not data_viz.done(client, params):
		data_viz.update(client, params)

if __name__ == '__main__': 

	params = Params()
	client = Client(params)

	logging.basicConfig(filename=params.log_name,
						level=logging.INFO,
						format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    					datefmt='%Y-%m-%d %H:%M:%S')
	
	process(client, params)