import os

def get_api_key(filename):
    '''
    loads in API key from file 

    ARGUMENTS:  
        file: file that contains your API key
    
    RETURNS:
        your API key
    '''
    base_path = os.path.abspath(os.path.dirname(__file__))
    full_path = os.path.join(base_path, filename)
    with open(full_path) as f:
        api_key = f.read()
    return api_key

API_KEY = get_api_key('apikey.txt')