"""
File Name  : transformation_customerorder_convenience_charge_base2.py
Class Name : DataTransformationBasePlusTwoConvenienceCharge
Description : This file takes the source table present at base+1 layer and applies the transformation logic
             to create DataTransformationBasePlusTwoInvoiceConvenienceCharge table at base+2 layer.
"""

from lib.gap.core.common_utility.common_functions import CommonFunctions
from lib.gap.core.common_utility.constant import Constant
from lib.gap.core.common_utility.exception.ex_handler import basic_error_handling, PolicyLogAbort
from dataengglibs.gap.curation.common_utils.generic_functions import CurationCommonFunctions
from dataengglibs.gap.curation.common_utils.curation_trigger_base import ParameterBlock
from dataengglibs.gap.curation.common_utils.curation_constant import CurationCommonConstant
from pyspark.sql.functions import col, explode, lit, split, concat, regexp_extract, to_timestamp, to_date


# noinspection PyMethodMayBeStatic


class DataTransformationBasePlusTwoConvenienceCharge:
    def __init__(self):
        self.common_functions = CommonFunctions()
        self.curation_common_functions = CurationCommonFunctions()
        self.spark = self.common_functions.gdp_spark_session()
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        self.spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

    def transformation_query(self, customerOrder_dataframe
                             , ship_preference_convenience_charge_df,gift_preference_convenience_charge_df,
                             ship_preference_adjustment_convenience_charge_df,  gift_preference_adjustment_convenience_charge_df):
        """
        Error Message: transformation query stream failed! {0}
        """

        source1_df = ship_preference_convenience_charge_df.select(
                                        *CurationCommonConstant.AUDIT_COLUMNS
                                        , col("invoice_id")
                                        , col("ship_preference_convenience_charge")
                                        , col("order_id")
                                        , col("order_date")
                                        , col("order_datetime")
                                        , col("order_datetime_utc")
                                        , col("invoiced_on_date")
                                        , col("invoiced_on_datetime")
                                        , col("invoiced_on_datetime_utc")
                                        , col("published_time")
                                         , *CurationCommonConstant.DQ_COLUMNS
        )

        source2_df = gift_preference_convenience_charge_df.select(
                                        *CurationCommonConstant.AUDIT_COLUMNS
                                        , col("invoice_id")
                                        , col("gift_preference_convenience_charge")
                                        , col("order_id")
                                        , col("order_date")
                                        , col("order_datetime")
                                        , col("order_datetime_utc")
                                        , col("invoiced_on_date")
                                        , col("invoiced_on_datetime")
                                        , col("invoiced_on_datetime_utc")
                                        , col("published_time")
                                        , *CurationCommonConstant.DQ_COLUMNS
        )

        source3_df = ship_preference_adjustment_convenience_charge_df.select(
                                        *CurationCommonConstant.AUDIT_COLUMNS
                                        , col("invoice_id")
                                        , col("ship_preference_adjustment_convenience_charge")
                                        , col("order_id")
                                        , col("order_date")
                                        , col("order_datetime")
                                        , col("order_datetime_utc")
                                        , col("invoiced_on_date")
                                        , col("invoiced_on_datetime")
                                        , col("invoiced_on_datetime_utc")
                                        , col("published_time")
                                        , *CurationCommonConstant.DQ_COLUMNS
        )
        source4_df = gift_preference_adjustment_convenience_charge_df.select(
                                        *CurationCommonConstant.AUDIT_COLUMNS
                                        , col("invoice_id")
                                        , col("gift_preference_adjustment_convenience_charge")
                                        , col("order_id")
                                        , col("order_date")
                                        , col("order_datetime")
                                        , col("order_datetime_utc")
                                        , col("invoiced_on_date")
                                        , col("invoiced_on_datetime")
                                        , col("invoiced_on_datetime_utc")
                                        , col("published_time")
                                        , *CurationCommonConstant.DQ_COLUMNS)

        explode1_df = source1_df.withColumn("shipprefconvenienceCharge",
                                             explode("ship_preference_convenience_charge")).withColumn(
            "convenience_charge_type", lit("ShipmentPreference"))
        explode2_df = source2_df.withColumn("giftprefconvenienceCharge",
                                             explode("gift_preference_convenience_charge")).withColumn(
            "convenience_charge_type", lit("GiftPreference"))
        explode3_df = source3_df.withColumn("ShipmentprefadjustmentconvenienceCharge",
                                             explode("ship_preference_adjustment_convenience_charge")).withColumn(
            "convenience_charge_type", lit("ShipmentPreferenceAdjustment"))
        explode4_df = source4_df.withColumn("giftprefadjustmentconvenienceCharge",
                                             explode("gift_preference_adjustment_convenience_charge")).withColumn(
            "convenience_charge_type", lit("GiftPreferenceAdjustment"))

        unioned_df = explode1_df.withColumn("convenience_charge_type", lit("ShipmentPreference")) \
            .union(explode2_df.withColumn("convenience_charge_type", lit("GiftPreference"))) \
            .union(explode3_df.withColumn("convenience_charge_type", lit("ShipmentPreferenceAdjustment"))) \
            .union(explode4_df.withColumn("convenience_charge_type", lit("GiftPreferenceAdjustment")))

        customerOrder_dataframe = customerOrder_dataframe.drop(
            *('gdp_processed_timestamp','gdp_last_processed_by','order_date','epoch_id','latency','failed_rule_id','dq_processed_timestamp'))

        dq_checked_dataframe = unioned_df \
            .withColumn("publishedTime_date", to_date(split(col("published_time"), "T")[0])) \
            .withColumn("publishedTime_time_part", split(split(col("published_time"), "T")[1], "-")[0]) \
            .withColumn("publishedTime_offset_part",
                        concat(regexp_extract(split(split(col("published_time"), "T")[1], "-")[1], '(\\d{2})', 0),
                               lit(":"),
                               regexp_extract(split(split(col("published_time"), "T")[1], "-")[1], '(\\d{2}$)', 0))) \
            .withColumn("published_time_utc",
                        concat(col("publishedTime_date"), lit("T"), col("publishedTime_time_part"), lit("-"),
                               col("publishedTime_offset_part")))

        customerOrder_dataframe = customerOrder_dataframe.withColumnRenamed('order_id', 'cust_order_id')
        joined_df = dq_checked_dataframe \
            .join(customerOrder_dataframe, dq_checked_dataframe.order_id == customerOrder_dataframe.cust_order_id,
                  "left")

        transformed_dataframe = joined_df \
            .select(
            *CurationCommonConstant.AUDIT_COLUMNS
            , col("convenience_charge_type")
            , col("invoice_id")
            , col("shipprefconvenienceCharge.category").alias("convenience_charge_category")
            , col("shipprefconvenienceCharge.name").alias("convenience_charge_name")
            , col("shipprefconvenienceCharge.amount").alias("convenience_charge_amount")
            , col("order_id").alias("external_customer_id")
            , col("order_date")
            , col("order_datetime")
            , col("order_datetime_utc")
            , col("invoiced_on_date")
            , col("invoiced_on_datetime")
            , col("invoiced_on_datetime_utc")
            , col("published_time")
            , to_timestamp(col("published_time_utc")).alias("published_time_utc")
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

        delta_table = self.curation_common_functions.get_delta_table(baseplus_configs_dictionary,
                                                                     output_stream_dataframe.schema)

        primary_key_columns = ["convenience_charge_type", "invoice_id", "convenience_charge_category",
                               "convenience_charge_name"]
        sort_column = "published_time_utc"

        upsert_wrapper = lambda output_stream_dataframe, batch_id: self.curation_common_functions. \
            upsert_into_delta_table(delta_table,
                                    output_stream_dataframe,
                                    primary_keys=primary_key_columns,
                                    sort_column=sort_column,
                                    on_columns=primary_key_columns,
                                    )

        # Defining the write options
        write_options = {
            "mode": "update",
            "checkpointLocation": checkpoint_location,
            "badRecordsPath": bad_records_location,
            "queryName": f'{target_delta_table_name}_query',
            "triggerOnce": trigger_once
        }

        # Writing the transformed dataframe into the target table
        self.curation_common_functions.write_delta_table(output_stream_dataframe,
                                                         target_path=output_path,
                                                         upsert_wrapper=upsert_wrapper,
                                                         is_stream=True,
                                                         options=write_options)


    @basic_error_handling([PolicyLogAbort(Exception)])
    def baseplus_data_transformation(self, config_name, params: ParameterBlock):
        params.mount_point = params.mount_point + Constant.AdlsDataStore.CONFIG_TABLE
        baseplus_configs_dictionary = self.curation_common_functions. \
            get_baseplus_configs(params.path, params.mount_point, params.databricks_workspace_id, config_name)

        source_base1_name = baseplus_configs_dictionary["source_table_name"][0]
        source_base2_name = baseplus_configs_dictionary["source_table_name"][1]
        source_base3_name = baseplus_configs_dictionary["source_table_name"][2]
        source_base4_name = baseplus_configs_dictionary["source_table_name"][3]

        source_customerOrder_name = baseplus_configs_dictionary["source_table_name"][4]

        config_table_storage_account_name = baseplus_configs_dictionary["config_table_storage_account_name"]

        source_base1_path = self.common_functions.get_base_path(source_base1_name, config_table_storage_account_name)
        source_base2_path = self.common_functions.get_base_path(source_base2_name, config_table_storage_account_name)
        source_base3_path = self.common_functions.get_base_path(source_base3_name, config_table_storage_account_name)
        source_base4_path = self.common_functions.get_base_path(source_base4_name, config_table_storage_account_name)
        source_customerOrder_path = self.common_functions.get_base_path(source_customerOrder_name,
                                                                        config_table_storage_account_name)

        source_base1_dataframe = self.curation_common_functions.read_delta_table(source_base1_path,
                                                                                 params.mount_point,
                                                                                 params.databricks_workspace_id,
                                                                                 params.job_name,
                                                                                 None,
                                                                                 True)
        source_base2_dataframe = self.curation_common_functions.read_delta_table(source_base2_path,
                                                                                 params.mount_point,
                                                                                 params.databricks_workspace_id,
                                                                                 params.job_name,
                                                                                 None,
                                                                                 True)
        source_base3_dataframe = self.curation_common_functions.read_delta_table(source_base3_path,
                                                                                 params.mount_point,
                                                                                 params.databricks_workspace_id,
                                                                                 params.job_name,
                                                                                 None,
                                                                                 True)
        source_base4_dataframe = self.curation_common_functions.read_delta_table(source_base4_path,
                                                                                 params.mount_point,
                                                                                 params.databricks_workspace_id,
                                                                                 params.job_name,
                                                                                 None,
                                                                                 True)

        source_customerOrder_dataframe = self.curation_common_functions.read_delta_table(source_customerOrder_path,
                                                                                         params.mount_point,
                                                                                         params.databricks_workspace_id,
                                                                                         params.job_name,
                                                                                         None,
                                                                                         False)

        job_run_id = self.common_functions.get_job_run_id(baseplus_configs_dictionary, params.job_name,
                                                          params.mount_point)

        dq_checked_dataframe = self.common_functions.check_data_quality(source_base1_dataframe,
                                                                        job_run_id, "base",
                                                                        source_base1_name,
                                                                        params.trigger_once)
        transformed_dataframe = self.transformation_query(source_customerOrder_dataframe
                                 , dq_checked_dataframe , source_base2_dataframe,
                                 source_base3_dataframe,  source_base4_dataframe)
        self.baseplus_write_stream(baseplus_configs_dictionary, transformed_dataframe, params.trigger_once)
