#import libraries and modules
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


#Create LoadFactOperator
class TestOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="postgres_default",
                 value=[],
                 *args, **kwargs):

        super(TestOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.value = value
        
    def execute(self, context):
        """
        Execution function of LoadFactOperator
        """
        #Create PostgresHook to connect to redshift
        pgs_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        #Run sql command in redshift
        pgs_hook.run("""
        INSERT INTO test 
	(id, height, weight)
        VALUES ({},{},{})
        """.format(self.value[0], self.value[1], self.value[2]))

        self.log.info("Finish inserting data to PostgreSQL")
        context['task_instance'].xcom_push(key='key', value = 'Burger is so nice')
