"""
File Name  : transformation_raw_market_channel_purchase_order_cc_cost_component_base1.py
Class Name : DataTransformationBasePlusOneRawMarketChannelPurchaseOrderCcCostComponent
Description : This file takes the source table present at base+1 layer and applies the transformation logic
             to create flattened out market_channel_purchase_order_cc_cost_component table at base+1 layer.
"""

from lib.gap.core.common_utility.common_functions import CommonFunctions
from lib.gap.core.common_utility.constant import Constant
from dataengglibs.gap.curation.common_utils.generic_functions import CurationCommonFunctions
from dataengglibs.gap.curation.common_utils.curation_constant import CurationCommonConstant
from pyspark.sql.functions import col, explode, to_timestamp, substring, trim, split, concat, lit, to_date
from lib.gap.core.common_utility.exception.ex_handler import basic_error_handling, PolicyLogAbort
from dataengglibs.gap.curation.common_utils.curation_trigger_base import ParameterBlock

# noinspection PyMethodMayBeStatic

class DataTransformationBasePlusOneRawMarketChannelPurchaseOrderCcCostComponent:
    def __init__(self):
        self.common_functions = CommonFunctions()
        self.curation_common_functions = CurationCommonFunctions()
        self.spark = self.common_functions.gdp_spark_session()
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    def transformation_query(self, dq_checked_dataframe):
        """
        Error Message: transformation query stream failed! {0}
        """
        transformed_dataframe = dq_checked_dataframe \
            .withColumn("data1", explode("data.order.styles")) \
            .withColumn("data2", explode("data1.customerChoices")) \
            .withColumn("data3", explode("data2.costComponents")) \
            .select( \
                *CurationCommonConstant.AUDIT_COLUMNS
                , col("data.order.id").alias("market_channel_purchase_order_id")
                , col("data1.id").alias("market_channel_customer_choice_id")
                , col("data3.name").alias("cost_component_name")
                , col("data3.rate").alias("cost_component_rate")
                , col("data3.currency").alias("cost_component_currency")
                , col("data3.amount").alias("cost_component_amount")
                , col("data.order.orderNumber").alias("market_channel_purchase_order")
                , col("data2.universalCustomerChoiceNumber").alias("universal_customer_choice_number")
                , col("data2.styleCustomerChoiceNumber").alias("style_customer_choice_number")
                , to_timestamp(col("data.header.businessEvent.timestamp")).alias("event_timestamp")
                , col("data.order.status").alias("order_status")
                , to_date(col("data.order.createdDate")).alias("created_date")
                , to_timestamp(col("data.order.createdDate")).alias("created_timestamp_utc")
                , to_timestamp(col("data.order.lastUpdateDate")).alias("last_update_date")
                , *CurationCommonConstant.DQ_COLUMNS
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
