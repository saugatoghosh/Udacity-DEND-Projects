from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#Custom operator for performing data quality check on dimension table

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 pkey_col="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.pkey_col = pkey_col

    def execute(self, context):
        '''
        Checks if the primary key column has any null value. If so throws an exception
        '''
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table} WHERE {self.pkey_col} IS NULL")   
        num_records = records[0][0]
        if num_records > 0:
            raise ValueError(f"Data quality check failed. Primary key column {self.pkey_col} in table {self.table} contained {records[0][0]} null values")
        self.log.info(f"Data quality check on primary key column {self.pkey_col} in table {self.table} passed with 0 null values")
        
        
