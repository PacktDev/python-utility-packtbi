import requests, adal
from airflow.hooks.base_hook import BaseHook

def refresh_bi(workspace_id:str, dataset_id:str):
    '''
    
    Used to refresh power bi datasets.
    
    Warning: The dataset in power bi must have the gateway configured
    correctly. The response may still be successful even if the refresh
    does not work.
    
    Parameters
    ---------
    workspace_id: str
        Workspace id, can be found within Power BI
    dataset_id: str
        dataset id, can be found within Power BI
    
    '''
    resource_url = 'https://analysis.windows.net/powerbi/api'
    client_secret = BaseHook.get_connection('POWER_BI_API_CLIENT_SECRET').password
    client_id = BaseHook.get_connection('POWER_BI_API_CLIENT_ID').password
    tenant_id = BaseHook.get_connection('POWER_BI_TENANT_ID').password
    authority_url = 'https://login.microsoftonline.com/'+tenant_id+'/'
    
    context = adal.AuthenticationContext(authority=authority_url,
                                     validate_authority=True,
                                     api_version=None)
    
    token = context.acquire_token_with_client_credentials(resource_url, client_id, client_secret)
    
    access_token = token.get('accessToken')
    
    header = {'Authorization': f'Bearer {access_token}','Content-Type':'application/json'}
    
    url = 'https://api.powerbi.com/v1.0/myorg/groups/'+workspace_id+'/datasets/'+dataset_id+'/refreshes'
    
    print (f'Request url = {url}')
    
    response = requests.post(url=url, headers=header)
    
    print(f'Response = {response}')
    
def refresh_bi_dataflow(workspace_id:str, dataflow_id:str):
    '''
    
    Used to refresh power bi dataflows.
    
    Warning: The dataset in power bi must have the gateway configured
    correctly. The response may still be successful even if the refresh
    does not work.
    
    Parameters
    ---------
    workspace_id: str
        Workspace id, can be found within Power BI
    dataflow_id: str
        dataflow id, can be found within Power BI
    
    '''
    resource_url = 'https://analysis.windows.net/powerbi/api'
    client_secret = BaseHook.get_connection('POWER_BI_API_CLIENT_SECRET').password
    client_id = BaseHook.get_connection('POWER_BI_API_CLIENT_ID').password
    tenant_id = BaseHook.get_connection('POWER_BI_TENANT_ID').password
    authority_url = 'https://login.microsoftonline.com/'+tenant_id+'/'
    
    context = adal.AuthenticationContext(authority=authority_url,
                                     validate_authority=True,
                                     api_version=None)
    
    token = context.acquire_token_with_client_credentials(resource_url, client_id, client_secret)
    
    access_token = token.get('accessToken')
    
    header = {'Authorization': f'Bearer {access_token}','Content-Type':'application/json'}
    
    url = 'https://api.powerbi.com/v1.0/myorg/groups/'+workspace_id+'/dataflows/'+dataflow_id+'/refreshes'
    
    print (f'Request url = {url}')
    
    response = requests.post(url=url, headers=header, data=json.dumps({'refreshRequest': 'y'}))
    
    print(f'Response = {response}')