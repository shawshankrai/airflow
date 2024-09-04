from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base import BaseHook

from elasticsearch import Elasticsearch

class ElasticHook(BaseHook):
    
    def __init__(self, conn_id='elastic_default', *args, **kwargs):
        super().__init__(*args, **kwargs) # init base hook
        conn = self.get_connection(conn_id)
        
        conn_config = {}
        host = []
        
        if conn.host:
            hosts = conn.host.split(',')
        if conn.port:
            conn_config['port'] = (conn.port)
        if conn.login:
            conn_config['http_auth'] = (conn.login, conn.password)
            
        e_url = f"http://{hosts[0]}:{conn_config.get('port')}"
        print("elastic_url =",e_url)
        
        self.es = Elasticsearch(e_url)
        self.index = conn.schema
        
    def info(self):
        return self.es.info()
    
    def add_doc(self, index, doc_type, doc):
        self.set_index(index)
        res = self.es.index(index=index, doc_type=doc_type, doc=doc)
        return res
        
class AirflowElasticPlugin(AirflowPlugin):
    name = 'elastic'
    hooks = [ElasticHook]
    
    # docker exec -it airflow-airflow-scheduler-1 /bin/bash
    # airflow@9c3af0641505:/opt/airflow$ airflow plugins