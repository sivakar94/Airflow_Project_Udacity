from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
     DAG operator used to operate DataQuality
    :param string  redshift_conn_id: reference to a specific redshift database
    :param list  tables: list of tables to check
    :param list checks: Dictionary of data checks
     """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id,
                 tables,
                 checks
                 ,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.checks= checks

    def execute(self, context):
        
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"Process of Checking table {table}")
            records = self.hook.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
            
        failed_tests = []
        for check in self.checks:
            sql = check.get('check_sql')
            if sql:
                exp_result = check.get('expected_result')
                descr = check.get('descr')
                self.log.info(f"[{exp_result}/{descr}] {sql}")

                result = self.hook.get_records(sql)[0]

                if exp_result != result[0]:
                    failed_tests.append(
                        f"{descr}, expected {exp_result} got {result[0]}\n  "
                        "{sql}")
            
        if len(failed_tests) > 0:
            self.log.info('Tests failed')
            self.log.info(failed_tests)
            raise ValueError('Data quality failed')
            
        self.log.info("DataQualityOperator complete")