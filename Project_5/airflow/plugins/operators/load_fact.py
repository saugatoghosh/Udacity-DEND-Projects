from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#Loads the fact table from staging tables

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    sql_template = "{}"

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query

    def execute(self, context):
        '''
        Appends data to the fact table
        '''
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Implementing LoadFactOperator")
        fact_query = LoadFactOperator.sql_template.format(self.sql_query)
        redshift.run(fact_query)
