from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    DAG operator used to populate fact tables.
    :param string  redshift_conn_id: reference to a specific redshift database
    :param string  table_name: redshift table to load
    :param string  sql_query: statement used to extract songplays data
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 insert_sql,
                 *args,
                 **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.insert_sql = insert_sql

    def execute(self, context):
        self.log.info('LoadFactOperator execution starting')
        
        # connect to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"connected with {self.redshift_conn_id}")

        # build insert statement
        insert_sqlf = f"INSERT INTO {self.table_name} {self.insert_sql}"
        self.log.info(f"insert sql {insert_sqlf}")

        redshift_hook.run(insert_sqlf)
        self.log.info(f"StageToRedshiftOperator {self.table_name} complete")