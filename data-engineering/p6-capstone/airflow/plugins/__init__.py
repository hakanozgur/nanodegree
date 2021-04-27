from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import helpers
import operators


class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.DataQualityOperator,
        operators.LoadDimensionOperator,
        operators.LoadFactOperator,
    ]
    helpers = [
        helpers.SqlQueries
    ]
