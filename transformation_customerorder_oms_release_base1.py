"""
File Name  : transformation_customerorder_oms_release_base1.py
Class Name : DataTransformationBasePlusOneOmsRelease
Description : This file takes the source table present at base layer and applies the transformation logic
             to create oms_release table at base+1 layer.
"""

from lib.gap.core.common_utility.common_functions import CommonFunctions
from lib.gap.core.common_utility.constant import Constant
from lib.gap.core.common_utility.exception.ex_handler import basic_error_handling, PolicyLogAbort
from dataengglibs.gap.curation.common_utils.generic_functions import CurationCommonFunctions
from dataengglibs.gap.curation.common_utils.curation_trigger_base import ParameterBlock
from dataengglibs.gap.curation.common_utils.curation_constant import CurationCommonConstant
from pyspark.sql.functions import col, explode


# noinspection PyMethodMayBeStatic


class DataTransformationBasePlusOneOmsRelease:
    def __init__(self):
        self.common_functions = CommonFunctions()
        self.curation_common_functions = CurationCommonFunctions()
        self.spark = self.common_functions.gdp_spark_session()
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        self.spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

    def transformation_query(self, dq_checked_dataframe):
        """
        Error Message: transformation query stream failed! {0}
        """

        transformed_dataframe = dq_checked_dataframe.select(
                                                *CurationCommonConstant.AUDIT_COLUMNS,
                                                col("data.releaseEventDetails.id").alias("release_id"),
                                                col("data.versionId").alias("event_version_id"),
                                                col("data.eventName").alias("event_name"),
                                                col("data.publishedTime").alias("event_published_time"),
                                                col("data.createdOn").alias("event_created_on"),
                                                col("data.createdBy").alias("event_created_by"),
                                                col("data.releaseEventDetails.createdOn").alias("release_created_on"),
                                                col("data.releaseEventDetails.orderId").alias("order_id"),
                                                col("data.releaseEventDetails.orderDate").alias("order_date"),
                                                col("data.releaseEventDetails.invoiceId").alias("invoice_id"),
                                                col("data.releaseEventDetails.market").alias("market"),
                                                col("data.releaseEventDetails.countryCode").alias("country_code"),
                                                col("data.releaseEventDetails.parentShipNode").alias("parent_ship_node"),
                                                col("data.releaseEventDetails.inHomeDeliveryDate").alias("in_home_delivery_date"),
                                                col("data.releaseEventDetails.stockOrderDownloadVersion").alias("stock_order_download_version"),
                                                col("data.releaseEventDetails.releaseStatus").alias("release_status"),
                                                *CurationCommonConstant.DQ_COLUMNS
                                                )

        return transformed_dataframe

    def baseplus_write_stream(self, baseplus_configs_dictionary, output_stream_dataframe, trigger_once):
        """
        Error Message: Baseplus write stream failed! {0}
        """
        checkpoint_location = baseplus_configs_dictionary["checkpoint_location"]
        bad_records_location = baseplus_configs_dictionary["bad_records_location"]
        target_delta_table_name = baseplus_configs_dictionary["target_table_name"]
        output_path = baseplus_configs_dictionary["output_path"]

        # Defining the write options
        write_options = {
            "mode": "append",
            "checkpointLocation": checkpoint_location,
            "badRecordsPath": bad_records_location,
            "queryName": f'{target_delta_table_name}_query',
            "triggerOnce": trigger_once
        }

        # Writing the transformed dataframe into the target table
        self.curation_common_functions.write_delta_table(output_stream_dataframe,
                                                         target_path=output_path, is_stream=True,
                                                         options=write_options)

    @basic_error_handling([PolicyLogAbort(Exception)])
    def baseplus_data_transformation(self, config_name, params: ParameterBlock):
        params.mount_point = params.mount_point + Constant.AdlsDataStore.CONFIG_TABLE
        baseplus_configs_dictionary = self.curation_common_functions. \
            get_baseplus_configs(params.path, params.mount_point, params.databricks_workspace_id, config_name)

        source_table_name = baseplus_configs_dictionary["source_table_name"][0]
        config_table_storage_account_name = baseplus_configs_dictionary["config_table_storage_account_name"]
        source_path = self.common_functions.get_base_path(source_table_name, config_table_storage_account_name)

        source_dataframe = self.curation_common_functions.read_delta_table(source_path,
                                                                           params.mount_point,
                                                                           params.databricks_workspace_id,
                                                                           params.job_name,
                                                                           None,
                                                                           True)
        job_run_id = self.common_functions.get_job_run_id(baseplus_configs_dictionary, params.job_name,
                                                          params.mount_point)
        dq_checked_dataframe = self.common_functions.check_data_quality(source_dataframe,
                                                                        job_run_id, "base",
                                                                        source_table_name, params.trigger_once)
        transformed_dataframe = self.transformation_query(dq_checked_dataframe)
        self.baseplus_write_stream(baseplus_configs_dictionary, transformed_dataframe, params.trigger_once)
