from dotenv import load_dotenv, find_dotenv
from pandas import DataFrame
import os 
import logging

logger = logging.getLogger('pipeline.py')


class Params:
    """
    Parameters class.

    This file centralizes anything that can be 
    parametrized in the code.

    If you want to use a parameter later in the code, you
    should instantiate params as:
    `params = Params()`, 
    send the object `params` within functions and, inside the 
    function, call the parameter as an attribute of the `params` object.

    For instance, if you have a parameter called `url` created here, you'd 
    call it in a function called `func` as:

    ```
    def func(params):
    		...

    		url = params.url

    	...
    ```
    """

    # pre-requeqs

    # controles pseudo-manuais para forçar os steps
    gathering = True
    transform = True
    storage = True
    viz = False

    # magically load environment variables from any .env files
    load_dotenv(os.path.abspath('../.env'))

    # parameters
    raw_data = os.path.abspath('../data/raw/')
    external_data = os.path.abspath('../data/external/')
    processed_data = os.path.abspath('../data/processed/')
    intermediate_data = os.path.abspath('../data/intermediate/')

    log_name = os.path.abspath('../log/transportesp.log')

    # if this is set to True, then all the nodes will be automatically 
    # considered not up-to-date and will be rerun.
    force_execution = True 

    ## Database connection params
    db_server = 'postgresql'
    user = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    host = 'localhost'
    database = "transportesp"
    logger.info(f'The params are {raw_data}')