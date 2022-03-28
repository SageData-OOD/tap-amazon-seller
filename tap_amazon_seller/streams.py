"""Stream type classes for tap-amazon-seller."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_amazon_seller.client import AmazonSellerStream

from sp_api.api import Orders
from sp_api.base import SellingApiException
from datetime import datetime, timedelta
from sp_api.base import Marketplaces
from datetime import date
from sp_api.util import throttle_retry, load_all_pages

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class OrdersStream(AmazonSellerStream):
    def get_credentials(self):
        return dict(
            refresh_token=self.config.get('refresh_token'),
            lwa_app_id=self.config.get('lwa_app_id'),
            lwa_client_secret=self.config.get('lwa_client_secret'),
            aws_access_key=self.config.get('aws_access_key'),
            aws_secret_key=self.config.get('aws_secret_key'),
            role_arn=self.config.get('role_arn'),
        )
        
    """Define custom stream."""
    name = "orders"
    primary_keys = ["AmazonOrderId"]
    replication_key = "LastUpdateDate"
    records_jsonpath = "$.Orders[*]"
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
    def load_all_orders(self):
    
        """
        a generator function to return all pages, obtained by NextToken
        """
        credentials = self.get_credentials()
        #Return test orders only for now. Change the CreatedAfter to replication key before final version
        #and remove marketplaceids for picking up assosiated account's defualt marketplace
        return Orders(credentials=credentials).get_orders(CreatedAfter='TEST_CASE_200',MarketplaceIds=['ATVPDKIKX0DER'])

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        for page in self.load_all_orders():
            for order in page.payload.get('Orders'):
                yield order

class OrderItemsStream(AmazonSellerStream):
    def get_credentials(self):
        return dict(
            refresh_token=self.config.get('refresh_token'),
            lwa_app_id=self.config.get('lwa_app_id'),
            lwa_client_secret=self.config.get('lwa_client_secret'),
            aws_access_key=self.config.get('aws_access_key'),
            aws_secret_key=self.config.get('aws_secret_key'),
            role_arn=self.config.get('role_arn'),
        )
    """Define custom stream."""
    name = "orderitems"
    primary_keys = ["OrderItemId"]
    replication_key = None
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
                th.Property("IsGift", th.BooleanType),
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

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        credentials = self.get_credentials()
        items =   Orders(credentials=credentials).get_order_items("TEST_CASE_200").payload
        return [items]    