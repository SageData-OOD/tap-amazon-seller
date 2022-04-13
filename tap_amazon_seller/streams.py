"""Stream type classes for tap-amazon-seller."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_amazon_seller.client import AmazonSellerStream

from sp_api.api import Orders
from sp_api.base import SellingApiException
from datetime import datetime, timedelta

from datetime import date
from sp_api.util import throttle_retry, load_all_pages

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.

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
        th.Property("PurchaseDate", th.DateTimeType),
        th.Property("LastUpdateDate", th.DateTimeType),
        th.Property("OrderStatus", th.StringType),
        th.Property("FulfillmentChannel", th.StringType),
        th.Property("SalesChannel", th.StringType),
        th.Property("ShipServiceLevel", th.StringType),
        th.Property("OrderTotal", th.ObjectType(
            th.Property('CurrencyCode',th.StringType),
            th.Property('Amount',th.StringType),
        )),
        th.Property('NumberOfItemsShipped',th.NumberType),
        th.Property('NumberOfItemsUnshipped',th.NumberType),
        th.Property('PaymentMethod',th.StringType),
        th.Property("PaymentMethodDetails", th.CustomType({"type": ["array", "string"]})),
        th.Property('IsReplacementOrder',th.BooleanType),
        th.Property("MarketplaceId", th.StringType),
        th.Property("ShipmentServiceLevelCategory", th.StringType),
        th.Property("OrderType", th.StringType),
        th.Property("EarliestShipDate", th.DateTimeType),
        th.Property("LatestShipDate", th.DateTimeType),
        th.Property("EarliestDeliveryDate", th.DateTimeType),
        th.Property("LatestDeliveryDate", th.DateTimeType),
        th.Property('IsBusinessOrder',th.BooleanType),
        th.Property('IsPrime',th.BooleanType),
        th.Property('IsGlobalExpressEnabled',th.BooleanType),
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
                th.Property("ItemPrice", th.CustomType({"type": ["object", "string"]})),
                th.Property("ItemTax", th.CustomType({"type": ["object", "string"]})),
                th.Property("PromotionDiscount", th.CustomType({"type": ["object", "string"]})),
                th.Property("IsGift", th.StringType),
                th.Property("ConditionId", th.StringType),
                th.Property("ConditionSubtypeId", th.StringType),
                th.Property("IsTransparency", th.BooleanType),
                th.Property("SerialNumberRequired", th.BooleanType),
                th.Property("IossNumber", th.StringType),
                th.Property("DeemedResellerCategory", th.StringType),
                th.Property("StoreChainStoreId", th.StringType),
                th.Property("BuyerRequestedCancel", th.CustomType({"type": ["object", "string"]})),
            )
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
        items =   orders.get_order_items(order_id=order_id).payload
        return [items]  


        