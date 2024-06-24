"""
File Name  : transformation_customerorder_oms_release_item_base1.py
Class Name : DataTransformationBasePlusOneOmsRelease
Description : This file takes the source table present at base layer and applies the transformation logic
             to create oms_release table at base+1 layer.
"""

from lib.gap.core.common_utility.common_functions import CommonFunctions
from lib.gap.core.common_utility.constant import Constant
from lib.gap.core.common_utility.exception.ex_handler import basic_error_handling, PolicyLogAbort
from dataengglibs.gap.curation.common_utils.generic_functions import CurationCommonFunctions
from dataengglibs.gap.curation.common_utils.curation_constant import CurationCommonConstant
from dataengglibs.gap.curation.common_utils.curation_trigger_base import ParameterBlock
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StringType, StructField, ArrayType, IntegerType, DecimalType


# noinspection PyMethodMayBeStatic


class DataTransformationBasePlusOneOmsReleaseItem:
    def __init__(self):
        self.common_functions = CommonFunctions()
        self.curation_common_functions = CurationCommonFunctions()
        self.spark = self.common_functions.gdp_spark_session()
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        self.spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

    def transformation_query(self, dq_checked_dataframe, source_dataframe_sku):
        """
        Error Message: transformation query stream failed! {0}
        """
        df = dq_checked_dataframe \
            .withColumn("release_Items", explode("data.releaseEventDetails.releaseItems")) \
            .withColumn("data_item", explode("data.releaseEventDetails.releaseItems.product"))

        source_dataframe_sku = source_dataframe_sku.drop("gdp_processed_timestamp") \
            .drop("gdp_last_processed_by") \
            .drop('failed_rule_id') \
            .drop('dq_processed_timestamp') \
            .drop('latency') \
            .drop('epoch_id')

        join_condition = df.data_item.skuNumber == source_dataframe_sku.sku_number
        joined_df = df.join(source_dataframe_sku, join_condition)

        new_schema_descriptions = ArrayType(StructType([
            StructField("locale_code", StringType()),
            StructField("description", StringType())
        ]), True)

        tranformed_dataframe = joined_df.select(
            *CurationCommonConstant.AUDIT_COLUMNS
            , col("data.releaseEventDetails.id").alias("release_id")
            , col("release_Items.releaseLineNumber").alias("release_line_number")
            , col("data.versionId").alias("event_version_id")
            , col("release_Items.orderLineNumber").alias("order_line_number")
            , col("release_Items.quantity").alias("item_quantity")
            , col("release_Items.shipNode").alias("ship_node")
            , col("release_Items.isBackOrder").alias("is_backorder")
            , col("release_Items.releaseAdjustStatus.releaseAdjustType").alias("adjustment_type")
            , col("release_Items.releaseAdjustStatus.releaseAdjustDate").alias("adjustment_date")
            , col("data_item.skuNumber").alias("sku_number")
            , col("sku_id").alias("sku_id")
            , col("data_item.productTypeName").alias("product_type_name")
            , col("data_item.itemStyleNumber").alias("style_number")
            , col("data_item.skuBrand").alias("sku_brand")
            , col("data_item.itemStyleDescriptions").cast(new_schema_descriptions).alias("product_style_descriptions")
            , col("data_item.itemDescriptions").cast(new_schema_descriptions).alias("item_descriptions")
            , col("data_item.itemSizeDescriptions").cast(new_schema_descriptions).alias("item_size_descriptions")
            , col("data_item.itemColorDescriptions").cast(new_schema_descriptions).alias("product_color_descriptions")
            , col("data_item.price.currentPrice").cast("decimal(13,4)").alias("current_price")
            , col("data_item.price.paidPrice").cast("decimal(13,4)").alias("paid_price")
            , col("data_item.price.listPrice").cast("decimal(13,4)").alias("list_price")
            , col("data_item.price.retailPrice").cast("decimal(13,4)").alias("retail_price")
            , *CurationCommonConstant.DQ_COLUMNS
        )

        return tranformed_dataframe

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

        source_table_name_release = baseplus_configs_dictionary["source_table_name"][0]
        source_table_name_sku = baseplus_configs_dictionary["source_table_name"][1]

        config_table_storage_account_name = baseplus_configs_dictionary["config_table_storage_account_name"]

        source_path_release = self.common_functions.get_base_path(source_table_name_release,
                                                                  config_table_storage_account_name)
        source_path_sku = self.common_functions.get_base_path(source_table_name_sku,
                                                              config_table_storage_account_name)

        source_dataframe_release = self.curation_common_functions.read_delta_table(source_path_release,
                                                                                   params.mount_point,
                                                                                   params.databricks_workspace_id,
                                                                                   params.job_name,
                                                                                   root_column=None,
                                                                                   is_stream=True)

        source_dataframe_sku = self.curation_common_functions.read_delta_table(source_path_sku,
                                                                               params.mount_point,
                                                                               params.databricks_workspace_id,
                                                                               params.job_name,
                                                                               root_column=None,
                                                                               is_stream=True)
        job_run_id = self.common_functions.get_job_run_id(baseplus_configs_dictionary, params.job_name,
                                                          params.mount_point)
        dq_checked_dataframe = self.common_functions.check_data_quality(source_dataframe_release,
                                                                        job_run_id,
                                                                        "base",
                                                                        source_table_name_release,
                                                                        params.trigger_once)

        transformed_dataframe = self.transformation_query(dq_checked_dataframe, source_dataframe_sku)
        self.baseplus_write_stream(baseplus_configs_dictionary, transformed_dataframe, params.trigger_once)
