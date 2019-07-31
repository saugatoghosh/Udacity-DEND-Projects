from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Custom operator for loading dimension tables

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql_template = "{}"

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 truncate = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query
        self.table=table
        self.truncate=truncate

    def execute(self, context):
        '''
        Either empties and then loads dimension table or
        simply loads dimension table
        '''
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if(self.truncate == True):
            self.log.info('Clearing data from target dimension table')
            redshift.run("TRUNCATE TABLE {}".format(self.table))
            self.log.info("Implementing LoadDimensionOperator")
            dim_query = LoadDimensionOperator.sql_template.format(self.sql_query)
            redshift.run(dim_query)
            
        else:
            self.log.info("Implementing LoadDimensionOperator without truncating")
            dim_query = LoadDimensionOperator.sql_template.format(self.sql_query)
            redshift.run(dim_query)
        
