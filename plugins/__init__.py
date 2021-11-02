from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin

import operators
import helpers
import db_connections

#Define the plugin class
class MyPlugins(AirflowPlugin):
    name = "my_plugin"
    operators = [operators.LoadVaccOperator, operators.LoadCasesOperator, operators.LoadSuspectsOperator, operators.CheckMySqlRecordOperator, operators.UpdateCasesTableOperator, operators.UpdateSuspectsTableOperator, operators.UpdateVaccTableOperator,operators.DataQualityCheckOperator]
    
    db_connections = [db_connections.mysql_connect]

    helpers = [helpers.LoadTableFunctions]
