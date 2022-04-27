"""Stream type classes for tap-amazon-seller."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_amazon_seller.client import AmazonSellerStream

from sp_api.api import Orders
from sp_api.base import SellingApiException
from datetime import datetime, timedelta
from sp_api.base.exceptions import SellingApiForbiddenException

from datetime import date
from sp_api.util import throttle_retry, load_all_pages

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.
import backoff

class MarketplacesStream(AmazonSellerStream):
    """Define custom stream."""
    name = "marketplaces"
    primary_keys = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property('id',th.StringType),
        th.Property('name',th.StringType),
    ).to_dict()
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "marketplace_id": record["id"],
        }
    @throttle_retry()            
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        marketplaces = ["US", "CA", "MX", "BR", "ES", "GB", "FR", "NL", "DE", "IT", "SE", "PL", "EG", "TR", "SA", "AE", "IN", "SG", "AU", "JP"]
        # orders = self.get_sp_orders()
        #Fetch minimum number of orders and verify credentials are working
        today_date = datetime.today().strftime('%Y-%m-%d')
        for mp in marketplaces:
            try:
                orders = self.get_sp_orders(mp)
                allorders = orders.get_orders(CreatedAfter=today_date)
                yield {"id":mp}
            except:
                output = f"marketplace {mp} not part of current SP account"
class OrdersStream(AmazonSellerStream):      
    """Define custom stream."""
    name = "orders"
    primary_keys = ["AmazonOrderId"]
    replication_key = "LastUpdateDate"
    records_jsonpath = "$.Orders[*]"
    parent_stream_type = MarketplacesStream
    marketplace_id = "{marketplace_id}"
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("AmazonOrderId", th.StringType),
        th.Property("SellerOrderId", th.StringType),
        th.Property("PurchaseDate", th.DateTimeType),
        th.Property("LastUpdateDate", th.DateTimeType),
        th.Property("OrderStatus", th.StringType),
        th.Property("FulfillmentChannel", th.StringType),
        th.Property("SalesChannel", th.StringType),
        th.Property("ShipServiceLevel", th.StringType),
        th.Property("OrderChannel", th.StringType),
        th.Property("OrderTotal", th.ObjectType(
            th.Property('CurrencyCode',th.StringType),
            th.Property('Amount',th.StringType),
        )),
        th.Property('NumberOfItemsShipped',th.NumberType),
        th.Property('NumberOfItemsUnshipped',th.NumberType),
        th.Property('PaymentMethod',th.StringType),
        th.Property("PaymentMethodDetails", th.CustomType({"type": ["array", "string"]})),
        th.Property("PaymentExecutionDetail", th.CustomType({"type": ["array", "string"]})),
        th.Property("BuyerTaxInformation", th.CustomType({"type": ["object", "string"]})),
        th.Property("MarketplaceTaxInfo", th.CustomType({"type": ["object", "string"]})),
        th.Property("ShippingAddress", th.CustomType({"type": ["object", "string"]})),
        th.Property("BuyerInfo", th.CustomType({"type": ["object", "string"]})),
        th.Property('IsReplacementOrder',th.BooleanType),
        th.Property("ReplacedOrderId", th.StringType),
        th.Property("MarketplaceId", th.StringType),
        th.Property("SellerDisplayName", th.StringType),
        th.Property("EasyShipShipmentStatus", th.StringType),
        th.Property("CbaDisplayableShippingLabel", th.StringType),
        th.Property("ShipmentServiceLevelCategory", th.StringType),
        th.Property("BuyerInvoicePreference", th.StringType),
        th.Property("OrderType", th.StringType),
        th.Property("EarliestShipDate", th.DateTimeType),
        th.Property("LatestShipDate", th.DateTimeType),
        th.Property("EarliestDeliveryDate", th.DateTimeType),
        th.Property("PromiseResponseDueDate", th.DateTimeType),
        th.Property("LatestDeliveryDate", th.DateTimeType),
        th.Property('IsBusinessOrder',th.BooleanType),
        th.Property('IsEstimatedShipDateSet',th.BooleanType),
        th.Property('IsPrime',th.BooleanType),
        th.Property('IsGlobalExpressEnabled',th.BooleanType),
        th.Property('HasRegulatedItems',th.BooleanType),
        th.Property('IsPremiumOrder',th.BooleanType),
        th.Property('IsSoldByAB',th.BooleanType),
        th.Property('IsIBA',th.BooleanType),
        th.Property('DefaultShipFromLocationAddress',th.ObjectType(
            th.Property('Name',th.StringType),
            th.Property('AddressLine1',th.StringType),
            th.Property('City',th.StringType),
            th.Property('StateOrRegion',th.StringType),
            th.Property('PostalCode',th.StringType),
            th.Property('CountryCode',th.StringType),
            th.Property('Phone',th.StringType),
            th.Property('AddressType',th.StringType),
        )),
        th.Property('FulfillmentInstruction',th.ObjectType(
            th.Property('FulfillmentSupplySourceId',th.StringType),
            th.Property('IsISPU',th.BooleanType)
        )),
        th.Property('AutomatedShippingSettings',th.ObjectType(
            th.Property('HasAutomatedShippingSettings',th.BooleanType)
        )),
        
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (SellingApiForbiddenException),
        max_tries=5,
        factor=2,
    )
    @throttle_retry()
    @load_all_pages()
    def load_all_orders(self, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        mp = None
        if 'marketplace_id' in self.partitions[len(self.partitions)-1]:
            mp = self.partitions[len(self.partitions)-1]['marketplace_id']
        
        orders = self.get_sp_orders(mp)

        # Load the orders
        return orders.get_orders(**kwargs)


    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        # Get start_date
        start_date = self.get_starting_timestamp(context)
        if start_date is None:
            #Get all orders
            start_date = '1970-01-01'
        else:
            start_date = self.get_starting_timestamp(context)
            start_date = start_date.strftime("%Y-%m-%d")

        sandbox = self.config.get("sandbox",False)
        if sandbox is True:
            orders = self.get_sp_orders()
            allorders = orders.get_orders(CreatedAfter='TEST_CASE_200', MarketplaceIds=["ATVPDKIKX0DER"])
            for order in allorders.payload.get('Orders'):
                yield order
        else:
            for page in self.load_all_orders(LastUpdatedAfter=start_date):
                for order in page.payload.get('Orders'):
                    yield order

        

    
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        mp = None
        if 'marketplace_id' in self.partitions[len(self.partitions)-1]:
            mp = self.partitions[len(self.partitions)-1]['marketplace_id']
        return {
            "AmazonOrderId": record["AmazonOrderId"],
            "marketplace_id":mp
        }            

class OrderItemsStream(AmazonSellerStream):
    """Define custom stream."""
    name = "orderitems"
    primary_keys = ["OrderItemId"]
    replication_key = None
    order_id = "{AmazonOrderId}"
    parent_stream_type = OrdersStream
    schema_writed = False
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property('AmazonOrderId',th.StringType),
        th.Property('OrderItems',th.ArrayType(
            th.ObjectType(
                th.Property("ASIN", th.StringType),
                th.Property("OrderItemId", th.StringType),
                th.Property("SellerSKU", th.StringType),
                th.Property("Title", th.StringType),
                th.Property("QuantityOrdered", th.NumberType),
                th.Property("QuantityShipped", th.NumberType),
                th.Property("ProductInfo", th.CustomType({"type": ["object", "string"]})),
                th.Property("PointsGranted", th.CustomType({"type": ["object", "string"]})),
                th.Property("ItemPrice", th.CustomType({"type": ["object", "string"]})),
                th.Property("ShippingPrice", th.CustomType({"type": ["object", "string"]})),
                th.Property("ShippingDiscount", th.CustomType({"type": ["object", "string"]})),
                th.Property("ShippingDiscountTax", th.CustomType({"type": ["object", "string"]})),
                th.Property("PromotionDiscount", th.CustomType({"type": ["object", "string"]})),
                th.Property("PromotionDiscountTax", th.CustomType({"type": ["object", "string"]})),
                th.Property("ItemTax", th.CustomType({"type": ["object", "string"]})),
                th.Property("ShippingTax", th.CustomType({"type": ["object", "string"]})),
                th.Property("PromotionIds", th.CustomType({"type": ["array", "string"]})),
                th.Property("CODFee", th.CustomType({"type": ["object", "string"]})),
                th.Property("CODFeeDiscount", th.CustomType({"type": ["object", "string"]})),
                th.Property("TaxCollection", th.CustomType({"type": ["object", "string"]})),
                th.Property("BuyerInfo", th.CustomType({"type": ["object", "string"]})),
                th.Property("BuyerRequestedCancel", th.CustomType({"type": ["object", "string"]})),
                th.Property("IsGift", th.StringType),
                th.Property("ConditionId", th.StringType),
                th.Property("ConditionNote", th.StringType),
                th.Property("ConditionSubtypeId", th.StringType),
                th.Property("ScheduledDeliveryStartDate", th.StringType),
                th.Property("ScheduledDeliveryEndDate", th.StringType),
                th.Property("PriceDesignation", th.StringType),
                th.Property("IsTransparency", th.BooleanType),
                th.Property("SerialNumberRequired", th.BooleanType),
                th.Property("IossNumber", th.StringType),
                th.Property("DeemedResellerCategory", th.StringType),
                th.Property("StoreChainStoreId", th.StringType),
                th.Property("BuyerRequestedCancel", th.CustomType({"type": ["object", "string"]})),
            )
        ))
        
    ).to_dict()
    
    @backoff.on_exception(
        backoff.expo,
        (SellingApiForbiddenException),
        max_tries=5,
        factor=2,
    )
    @throttle_retry()
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:

        if 'AmazonOrderId' in self.partitions[len(self.partitions)-1]:
            order_id = self.partitions[len(self.partitions)-1]['AmazonOrderId'] 
        else:
            return []   

        mp = None
        if 'marketplace_id' in self.partitions[len(self.partitions)-1]:
            mp = self.partitions[len(self.partitions)-1]['marketplace_id']

        orders = self.get_sp_orders(mp)
        self.state_partitioning_keys = self.partitions[len(self.partitions)-1]
        sandbox = self.config.get("sandbox",False)
        if sandbox is False:
            items =   orders.get_order_items(order_id=order_id).payload
        else:
            items =   orders.get_order_items("'TEST_CASE_200'").payload    
        return [items]
       
class OrderBuyerInfo(AmazonSellerStream):
    """Define custom stream."""
    name = "orderbuyerinfo"
    primary_keys = ["AmazonOrderId"]
    replication_key = None
    order_id = "{AmazonOrderId}"
    parent_stream_type = OrdersStream
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property('AmazonOrderId',th.StringType),
        th.Property('BuyerEmail',th.StringType),
        th.Property('BuyerName',th.StringType),
        th.Property('BuyerCounty',th.StringType),
        th.Property("BuyerTaxInfo", th.CustomType({"type": ["object", "string"]})),
        th.Property('PurchaseOrderNumber',th.StringType),
    ).to_dict()

    @throttle_retry()
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:       
        if 'AmazonOrderId' in self.partitions[len(self.partitions)-1]:
            order_id = self.partitions[len(self.partitions)-1]['AmazonOrderId'] 
        else:
            return []   

        mp = None
        if 'marketplace_id' in self.partitions[len(self.partitions)-1]:
            mp = self.partitions[len(self.partitions)-1]['marketplace_id']

        orders = self.get_sp_orders(mp)
        items =   orders.get_order_buyer_info(order_id=order_id).payload
        return [items]  
class OrderAddress(AmazonSellerStream):
    """Define custom stream."""
    name = "orderaddress"
    primary_keys = ["AmazonOrderId"]
    replication_key = None
    order_id = "{AmazonOrderId}"
    parent_stream_type = OrdersStream
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property('AmazonOrderId',th.StringType),
        th.Property('ShippingAddress',th.ObjectType(
            th.Property("Name",th.StringType),
            th.Property("AddressLine1",th.StringType),
            th.Property("AddressLine2",th.StringType),
            th.Property("AddressLine3",th.StringType),
            th.Property("City",th.StringType),
            th.Property("County",th.StringType),
            th.Property("District",th.StringType),
            th.Property("StateOrRegion",th.StringType),
            th.Property("Municipality",th.StringType),
            th.Property("PostalCode",th.StringType),
            th.Property("CountryCode",th.StringType),
            th.Property("Phone",th.StringType),
            th.Property("AddressType",th.StringType),
        ))
    ).to_dict()

    @throttle_retry()
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        
        if 'AmazonOrderId' in self.partitions[len(self.partitions)-1]:
            order_id = self.partitions[len(self.partitions)-1]['AmazonOrderId'] 
        else:
            return []   

        mp = None
        if 'marketplace_id' in self.partitions[len(self.partitions)-1]:
            mp = self.partitions[len(self.partitions)-1]['marketplace_id']

        orders = self.get_sp_orders(mp)
        items =   orders.get_order_address(order_id=order_id).payload
        return [items]  
class OrderFinancialEvents(AmazonSellerStream):
    """Define custom stream."""
    name = "orderfinancialevents"
    primary_keys = []
    replication_key = None
    order_id = "{AmazonOrderId}"
    parent_stream_type = OrdersStream
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("ShipmentEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("RefundEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("GuaranteeClaimEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("ChargebackEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("PayWithAmazonEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("ServiceProviderCreditEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("RetrochargeEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("RentalTransactionEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("ProductAdsPaymentEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("ServiceFeeEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("SellerDealPaymentEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("DebtRecoveryEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("LoanServicingEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("AdjustmentEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("SAFETReimbursementEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("SellerReviewEnrollmentPaymentEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("FBALiquidationEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("CouponPaymentEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("ImagingServicesFeeEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("NetworkComminglingTransactionEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("AffordabilityExpenseEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("AffordabilityExpenseReversalEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("TrialShipmentEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("ShipmentSettleEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("TaxWithholdingEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("RemovalShipmentEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("RemovalShipmentAdjustmentEventList", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    @throttle_retry()
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        
        if 'AmazonOrderId' in self.partitions[len(self.partitions)-1]:
            order_id = self.partitions[len(self.partitions)-1]['AmazonOrderId'] 
        else:
            return []   

        mp = None
        if 'marketplace_id' in self.partitions[len(self.partitions)-1]:
            mp = self.partitions[len(self.partitions)-1]['marketplace_id']

        finance = self.get_sp_finance(mp)
        
        sandbox = self.config.get("sandbox",False)
        if sandbox is False:
            items =   finance.get_financial_events_for_order(order_id).payload
        else:
            items =   finance.get_financial_events_for_order("TEST_CASE_200").payload    
        return [items["FinancialEvents"]]  


        