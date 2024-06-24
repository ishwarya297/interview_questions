from dataengglibs.gap.curation.testbase.curation_test_base import CurationTestBase
from unittest.mock import Mock, patch
import dataengglibs.gap.curation.testbase.mock_pack as mock_pack

with patch.dict('sys.modules', delta=mock_pack):
    from transformation_customerorder.current import \
        transformation_customerorder_customer_order_line_promotion_base2 as customer_order_line_promotion, \
        transformation_customerorder_customer_order_payment_base2 as customer_order_payment, \
        transformation_customerorder_customer_order_line_item_tax_base2 as customer_order_line_item_tax, \
        transformation_customerorder_customer_order_header_convenience_tax_base2 as \
        customer_order_header_convenience_tax, \
        transformation_customerorder_customer_order_base2 as customer_order, \
        transformation_customerorder_customer_order_line_unit_status_base2 as customer_order_line_unit_status, \
        transformation_customerorder_customer_order_header_convenience_charge_base2 as \
        customer_order_header_convenience_charge, \
        transformation_customerorder_customer_order_donation_base2 as customer_order_donation, \
        transformation_customerorder_customer_order_line_item_base3 as customer_order_line_item, \
        transformation_customerorder_customer_order_line_item_reconciliation_base3 as \
        customer_order_line_item_reconciliation


class TestDataTransformationBasePlusTwoCustomerOrderLinePromotion(CurationTestBase):
    T = customer_order_line_promotion
    obj = T.DataTransformationBasePlusTwoCustomerOrderLinePromotion()
    write_function = obj.baseplus_write_stream
    transformation_query = obj.transformation_query
    transformation_function = obj.baseplus_data_transformation


class TestTransformationCustomerOrderPayment(CurationTestBase):
    T = customer_order_payment
    obj = T.DataTransformationBasePlusTwoCustomerOrderPayment()
    transformation_function = obj.baseplus_data_transformation
    write_function = obj.baseplus_write_stream
    transformation_query = obj.transformation_query


class TestDataTransformationBasePlusTwoCustomerOrderLineItemTax(CurationTestBase):
    T = customer_order_line_item_tax
    obj = T.DataTransformationBasePlusTwoCustomerOrderLineItemTax()
    write_function = obj.baseplus_write_stream
    transformation_query = obj.transformation_query
    transformation_function = obj.baseplus_data_transformation


class TestDataTransformationBasePlusTwoCustomerOrderHeaderConvenienceTax(CurationTestBase):
    T = customer_order_header_convenience_tax
    obj = T.DataTransformationBasePlusTwoCustomerOrderHeaderConvenienceTax()
    write_function = obj.baseplus_write_stream
    transformation_query = obj.transformation_query
    transformation_function = obj.baseplus_data_transformation


class TestDataTransformationBasePlusTwoCustomerOrderHeaderConvenienceCharge(CurationTestBase):
    T = customer_order_header_convenience_charge
    obj = T.DataTransformationBasePlusTwoCustomerOrderHeaderConvenienceCharge()
    write_function = obj.baseplus_write_stream
    transformation_query = obj.transformation_query
    transformation_function = obj.baseplus_data_transformation


class TestDataTransformationBasePlusTwoCustomerOrderLineUnitStatus(CurationTestBase):
    T = customer_order_line_unit_status
    obj = T.DataTransformationBasePlusTwoCustomerOrderLineUnitStatus()
    write_function = obj.baseplus_write_stream
    transformation_query = obj.transformation_query
    transformation_function = obj.baseplus_data_transformation

    def setUp(self) -> None:
        self.T.when = Mock()
        super(TestDataTransformationBasePlusTwoCustomerOrderLineUnitStatus, self).setUp()


class TestDataTransformationBasePlusTwoCustomerOrder(CurationTestBase):
    T = customer_order
    obj = T.DataTransformationBasePlusTwoCustomerOrder()
    write_function = obj.baseplus_write_stream
    transformation_query = obj.transformation_query
    transformation_function = obj.baseplus_data_transformation

    def test_transformation_query(self):
        self.obj.customerorder_common_functions = Mock()
        super(TestDataTransformationBasePlusTwoCustomerOrder, self).test_transformation_query()


class TestDataTransformationBasePlusTwoCustomerOrderDonation(CurationTestBase):
    T = customer_order_donation
    obj = T.DataTransformationBasePlusTwoCustomerOrderDonation()
    write_function = obj.baseplus_write_stream
    transformation_query = obj.transformation_query
    transformation_function = obj.baseplus_data_transformation


class TestDataTransformationBasePlusThreeCustomerOrderLineItem(CurationTestBase):
    T = customer_order_line_item
    obj = T.DataTransformationBasePlusThreeCustomerOrderLineItem()
    write_function = obj.baseplus_write_stream
    transformation_query = obj.transformation_query
    transformation_function = obj.baseplus_data_transformation

    def test_baseplus_one_data_transformation(self):
        self.config_mock['source_table_name'] = ["source_table_name1", "source_table_name2", "source_table_name3",
                                                 "source_table_name4", "source_table_name5", "source_table_name6",
                                                 "source_table_name7", "source_table_name8", "source_table_name9"]
        self.config_mock['target_table_name'] = "target_table"

        super(TestDataTransformationBasePlusThreeCustomerOrderLineItem, self).test_baseplus_one_data_transformation()

    def test_transformation_query(self):
        self.mocked_dataframe.select = Mock(return_value=self.mocked_dataframe)
        self.mocked_dataframe.withColumn = Mock(return_value=self.mocked_dataframe)
        self.mocked_dataframe.alias = Mock(return_value=self.mocked_dataframe)
        self.mocked_dataframe.join = Mock(return_value=self.mocked_dataframe)
        self.mocked_dataframe.order_date = 9
        self.mocked_dataframe.effective_start_date = 8
        self.mocked_dataframe.effective_end_date = 8
        self.obj.customerorder_common_functions = Mock()
        self.transformation_query(self.mocked_dataframe, self.mocked_dataframe, self.mocked_dataframe,
                                  self.mocked_dataframe, self.mocked_dataframe, self.mocked_dataframe,
                                  self.mocked_dataframe, self.mocked_dataframe)


class TestCustomerOrderLineItemReconciliation(CurationTestBase):
    T = customer_order_line_item_reconciliation
    obj = T.DataTransformationBasePlusThreeCustomerOrderLineItemReconciliation()
    transformation_query = obj.transformation_query
    transformation_to_handle_return_orders = obj.transformation_to_handle_return_orders
    write_function = obj.baseplus_write_stream
    transformation_function = obj.baseplus_data_transformation

    def test_transformation_query(self):
        self.mocked_dataframe.collect = Mock(return_value=[[3]])
        self.obj.spark = Mock(sql=Mock(return_value=self.mocked_dataframe))

        self.mocked_dataframe.business_inventory_date = 5
        self.mocked_dataframe.effective_end_date = 5
        self.transformation_query(self.mocked_dataframe,
                                  self.mocked_dataframe,
                                  self.mocked_dataframe
                                  )

    def test_transformation_to_handle_return_orders(self):
        configs_dictionary = {"output_path": "value"
                              }
        self.transformation_to_handle_return_orders(configs_dictionary)

    def test_baseplus_write_stream(self):
        configs_dictionary = {"checkpoint_location": "value",
                              "bad_records_location": "value",
                              "target_table_name": "value",
                              "output_path": "value"
                              }
        self.write_function(configs_dictionary, Mock())

    def test_baseplus_one_data_transformation(self):
        self.config_mock["source_table_name"] = [f"source_table_name_{i}" for i in range(3)]
        super(TestCustomerOrderLineItemReconciliation, self).test_baseplus_one_data_transformation()
