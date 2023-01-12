import requests, adal
from airflow.hooks.base_hook import BaseHook


class packt_bi:
    '''
        Class used to connect to powerbi API

    Attributes
    ----------
    client_id: str
        Client id, can be found within Power BI, used for authentication
    client_secret: str
        Client secret, can be found within Power BI, used for authentication
    tenant_id: str
        Tenant id, can be found within Power BI, used for authentication
    
    Methods
    --------
    refresh_bi(workspace_id:str, dataset_id:str):
        Used to refresh power bi datasets.
    refresh_bi_dataflow(workspace_id:str, dataflow_id:str):
        Used to refresh power bi dataflows.
    '''

    def __init__(self, client_id:str, client_secret:str, tenant_id:str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.resource_url = 'https://analysis.windows.net/powerbi/api'
        self.authority_url = 'https://login.microsoftonline.com/'+self.tenant_id+'/'

    def refresh_bi(self, workspace_id:str, dataset_id:str):
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
        self.context = adal.AuthenticationContext(authority=self.authority_url,
                                     validate_authority=True,
                                     api_version=None)
        self.access_token = context.acquire_token_with_client_credentials(self.resource_url, self.client_id, self.client_secret).get('accessToken')
        self.header = {'Authorization': f'Bearer {self.access_token}','Content-Type':'application/json'}
        self.url = 'https://api.powerbi.com/v1.0/myorg/groups/'+workspace_id+'/datasets/'+dataset_id+'/refreshes'
        print (f'Request url = {self.url}')
        self.response = requests.post(url=self.url, headers=self.header)
        print(f'Response = {self.response}')

    def refresh_bi_dataflow(self, workspace_id:str, dataflow_id:str):
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
        self.context = adal.AuthenticationContext(authority=self.authority_url,
                                     validate_authority=True,
                                     api_version=None)
        self.access_token = context.acquire_token_with_client_credentials(self.resource_url, self.client_id, self.client_secret).get('accessToken')
        self.header = {'Authorization': f'Bearer {self.access_token}','Content-Type':'application/json'}
        self.url = 'https://api.powerbi.com/v1.0/myorg/groups/'+workspace_id+'/dataflows/'+dataflow_id+'/refreshes'
        print (f'Request url = {self.url}')
        self.response = requests.post(url=self.url, headers=self.header)
        print(f'Response = {self.response}')
