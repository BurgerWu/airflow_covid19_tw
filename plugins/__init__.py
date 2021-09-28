from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

#Define the plugin class
class MyPlugins(AirflowPlugin):
    name = "my_plugin"
    operators = [operators.TestOperator, operators.LoadVaccOperator, operators.LoadCasesOperator, operators.LoadSuspectsOperator, operators.CheckMySqlRecordOperator, operators.UpdateCasesTableOperator, operators,UpdateSuspectsTableOperator, operators.UpdateVaccTableOperator]
    helpers = [helpers.LoadTableFunctions]
