"""
File Name  : transformation_customerorder_oms_gift_preference_adjustment.py
Class Name : DataTransformationBasePlusOneOmsGiftPreferenceAdjustment
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
from pyspark.sql.types import StructType, StringType, ArrayType,StructField,DecimalType


# noinspection PyMethodMayBeStatic


class DataTransformationBasePlusOneOmsGiftPreference:
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
        convenience_charge_schema = ArrayType(StructType([StructField("convenience_charge_name", StringType(), True),
                                                 StructField("convenience_charge_category", StringType(), True),
                                                 StructField("amount", DecimalType(13,2), True),
                                                 StructField("convenience_charge_amount", DecimalType(13,6), True),
                                                        ]), True)
        convenience_charge_tax_schema = ArrayType(StructType([StructField("tax_charge_category", StringType(), True),
                                                         StructField("tax_charge_name", StringType(), True),
                                                         StructField("taxable_amount", DecimalType(13,2), True),
                                                         StructField("charge_taxable_amount", DecimalType(13,6), True),
                                                         StructField("tax_name", StringType(), True),
                                                         StructField("tax_percentage", DecimalType(13,2), True),
                                                         StructField("charge_tax_percentage", DecimalType(13,6), True),
                                                         StructField("tax_amount", DecimalType(13,2), True),
                                                         StructField("charge_tax_amount", DecimalType(13,6), True),
                                                                ]), True)

        transformed_dataframe=dq_checked_dataframe.select(
                                                  *CurationCommonConstant.AUDIT_COLUMNS
                                                  ,col('data.invoiceEventDetail.id').alias('invoice_id')
                                                  ,col('data.invoiceEventDetail.giftPreference.isGiftIndicator').alias('gift_preference_gift_indicator')  
                                                  ,col('data.invoiceEventDetail.giftPreference.premiumGiftWrapDescription').alias('gift_preference_premium_gift_wrap_description') 
                                                  ,col('data.invoiceEventDetail.giftPreference.complimentaryBoxQuantity').alias('gift_preference_complimentary_box_quantity') 
                                                  ,col('data.invoiceEventDetail.giftPreference.taxCode').alias('gift_preference_tax_code') 
                                                  ,col('data.invoiceEventDetail.giftPreference.premiumGiftWrapSkuNumber').alias('gift_preference_premium_gift_wrap_sku_number') 
                                                  ,col('data.invoiceEventDetail.giftPreference.instruction.header').alias('gift_preference_instruction_header') 
                                                  ,col('data.invoiceEventDetail.giftPreference.instruction.footer').alias('gift_preference_instruction_footer') 
                                                  ,col('data.invoiceEventDetail.giftPreference.instruction.body').alias('gift_preference_instruction_body') 
                                                  ,col('data.invoiceEventDetail.giftPreference.instruction.type').alias('gift_preference_instruction_type') 
                                                  ,col('data.invoiceEventDetail.giftPreference.convenienceCharges').cast(convenience_charge_schema).alias('gift_preference_convenience_charge')
                                                  ,col('data.invoiceEventDetail.giftPreference.convenienceTaxes').cast(convenience_charge_tax_schema).alias('gift_preference_convenience_charge_tax')
                                                  ,col('data.invoiceEventDetail.order.id').alias('order_id')
                                                  ,col('data.invoiceEventDetail.order.versionId').alias('order_version_id')
                                                  ,col('data.invoiceEventDetail.orderDate').cast('date').alias('order_date')  
                                                  ,col('data.invoiceEventDetail.orderDate').alias('order_datetime')
                                                  ,col('data.invoiceEventDetail.orderDate').cast('timestamp').alias('order_datetime_utc')
                                                  ,col('data.invoiceEventDetail.invoicedOn').cast('date').alias('invoice_created_on_date')
                                                  ,col('data.invoiceEventDetail.invoicedOn').alias('invoice_created_on_datetime')
                                                  ,col('data.invoiceEventDetail.invoicedOn').cast('timestamp').alias('invoice_created_on_datetime_utc')
                                                  ,col('data.versionId').alias('version_id')
                                                  ,col('data.publishedTime').alias('published_time')
                                                  ,col('data.publishedTime').cast('timestamp').alias('published_time_utc')
                                                  ,*CurationCommonConstant.DQ_COLUMNS)

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
