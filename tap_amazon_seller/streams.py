"""Stream type classes for tap-amazon-seller."""

from typing import Any, Dict, Optional, Union, List, Iterable
import backoff

from singer_sdk import typing as th

from tap_amazon_seller.client import AmazonSellerStream
from tap_amazon_seller.utils import timeout, Timeout, InvalidResponse

from datetime import datetime
from sp_api.util import load_all_pages


class MarketplacesStream(AmazonSellerStream):
    """Define custom stream."""

    name = "marketplaces"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "marketplace_id": record["id"],
        }

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        marketplaces = [
            "US",
            "CA",
            "MX",
            "BR",
            "ES",
            "GB",
            "FR",
            "NL",
            "DE",
            "IT",
            "SE",
            "PL",
            "EG",
            "TR",
            "SA",
            "AE",
            "IN",
            "SG",
            "AU",
            "JP",
        ]
        # orders = self.get_sp_orders()
        # Fetch minimum number of orders and verify credentials are working
        today_date = datetime.today().strftime("%Y-%m-%d")
        for mp in marketplaces:
            try:
                orders = self.get_sp_orders(mp)
                allorders = orders.get_orders(CreatedAfter=today_date)
                yield {"id": mp}
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
        th.Property(
            "OrderTotal",
            th.ObjectType(
                th.Property("CurrencyCode", th.StringType),
                th.Property("Amount", th.StringType),
            ),
        ),
        th.Property("NumberOfItemsShipped", th.NumberType),
        th.Property("NumberOfItemsUnshipped", th.NumberType),
        th.Property("PaymentMethod", th.StringType),
        th.Property(
            "PaymentMethodDetails", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "PaymentExecutionDetail", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "BuyerTaxInformation", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property(
            "MarketplaceTaxInfo", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("ShippingAddress", th.CustomType({"type": ["object", "string"]})),
        th.Property("BuyerInfo", th.CustomType({"type": ["object", "string"]})),
        th.Property("IsReplacementOrder", th.BooleanType),
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
        th.Property("IsBusinessOrder", th.BooleanType),
        th.Property("IsEstimatedShipDateSet", th.BooleanType),
        th.Property("IsPrime", th.BooleanType),
        th.Property("IsGlobalExpressEnabled", th.BooleanType),
        th.Property("HasRegulatedItems", th.BooleanType),
        th.Property("IsPremiumOrder", th.BooleanType),
        th.Property("IsSoldByAB", th.BooleanType),
        th.Property("IsIBA", th.BooleanType),
        th.Property(
            "DefaultShipFromLocationAddress",
            th.ObjectType(
                th.Property("Name", th.StringType),
                th.Property("AddressLine1", th.StringType),
                th.Property("City", th.StringType),
                th.Property("StateOrRegion", th.StringType),
                th.Property("PostalCode", th.StringType),
                th.Property("CountryCode", th.StringType),
                th.Property("Phone", th.StringType),
                th.Property("AddressType", th.StringType),
            ),
        ),
        th.Property(
            "FulfillmentInstruction",
            th.ObjectType(
                th.Property("FulfillmentSupplySourceId", th.StringType),
                th.Property("IsISPU", th.BooleanType),
            ),
        ),
        th.Property(
            "AutomatedShippingSettings",
            th.ObjectType(th.Property("HasAutomatedShippingSettings", th.BooleanType)),
        ),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (Exception, InvalidResponse),
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    @load_all_pages()
    def load_all_orders(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        try:
            orders = self.get_sp_orders(mp)
            orders_obj = orders.get_orders(**kwargs)
            return orders_obj
        except Exception as e:
            raise InvalidResponse(e)

    def load_order_page(self, mp, **kwargs):
        """
        a generator function to return all pages, obtained by NextToken
        """
        orders = []
        for page in self.load_all_orders(mp, **kwargs):
            for order in page.payload.get("Orders"):
                orders.append(order)
        return orders

    @backoff.on_exception(
        backoff.expo,
        (Exception, InvalidResponse),
        max_tries=10,
        factor=3,
    )
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            # Get start_date
            start_date = self.get_starting_timestamp(context) or datetime(2000, 1, 1)
            start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")

            sandbox = self.config.get("sandbox", False)
            if sandbox is True:
                return self.load_order_page(
                    mp="ATVPDKIKX0DER", CreatedAfter="TEST_CASE_200"
                )
            else:
                return self.load_order_page(
                    mp=context.get("marketplace_id"), LastUpdatedAfter=start_date
                )
        except Exception as e:
            raise InvalidResponse(e)

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        mp = context.get("marketplace_id")
        return {"AmazonOrderId": record["AmazonOrderId"], "marketplace_id": mp}


class OrderItemsStream(AmazonSellerStream):
    """Define custom stream."""

    name = "orderitems"
    primary_keys = ["OrderItemId"]
    replication_key = None
    order_id = "{AmazonOrderId}"
    parent_stream_type = OrdersStream
    schema_writed = False

    schema = th.PropertiesList(
        th.Property("AmazonOrderId", th.StringType),
        th.Property(
            "OrderItems",
            th.ArrayType(
                th.ObjectType(
                    th.Property("ASIN", th.StringType),
                    th.Property("OrderItemId", th.StringType),
                    th.Property("SellerSKU", th.StringType),
                    th.Property("Title", th.StringType),
                    th.Property("QuantityOrdered", th.NumberType),
                    th.Property("QuantityShipped", th.NumberType),
                    th.Property(
                        "ProductInfo", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "PointsGranted", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "ItemPrice", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "ShippingPrice", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "ShippingDiscount",
                        th.CustomType({"type": ["object", "string"]}),
                    ),
                    th.Property(
                        "ShippingDiscountTax",
                        th.CustomType({"type": ["object", "string"]}),
                    ),
                    th.Property(
                        "PromotionDiscount",
                        th.CustomType({"type": ["object", "string"]}),
                    ),
                    th.Property(
                        "PromotionDiscountTax",
                        th.CustomType({"type": ["object", "string"]}),
                    ),
                    th.Property(
                        "ItemTax", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "ShippingTax", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "PromotionIds", th.CustomType({"type": ["array", "string"]})
                    ),
                    th.Property(
                        "CODFee", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "CODFeeDiscount", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "TaxCollection", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "BuyerInfo", th.CustomType({"type": ["object", "string"]})
                    ),
                    th.Property(
                        "BuyerRequestedCancel",
                        th.CustomType({"type": ["object", "string"]}),
                    ),
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
                    th.Property(
                        "BuyerRequestedCancel",
                        th.CustomType({"type": ["object", "string"]}),
                    ),
                )
            ),
        ),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            order_id = context.get("AmazonOrderId", [])

            orders = self.get_sp_orders(context.get("marketplace_id"))
            # self.state_partitioning_keys = context
            self.state_partitioning_keys = self.partitions[len(self.partitions) - 1]
            # self.state_partitioning_keys = self.partitions
            sandbox = self.config.get("sandbox", False)
            if sandbox is False:
                items = orders.get_order_items(order_id=order_id).payload
            else:
                items = orders.get_order_items("'TEST_CASE_200'").payload
            return [items]
        except Exception as e:
            raise InvalidResponse(e)


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
        th.Property("AmazonOrderId", th.StringType),
        th.Property("BuyerEmail", th.StringType),
        th.Property("BuyerName", th.StringType),
        th.Property("BuyerCounty", th.StringType),
        th.Property("BuyerTaxInfo", th.CustomType({"type": ["object", "string"]})),
        th.Property("PurchaseOrderNumber", th.StringType),
    ).to_dict()

    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            order_id = context.get("AmazonOrderId", [])

            orders = self.get_sp_orders(context.get("marketplace_id"))
            items = orders.get_order_buyer_info(order_id=order_id).payload
            return [items]
        except Exception as e:
            raise InvalidResponse(e)


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
        th.Property("AmazonOrderId", th.StringType),
        th.Property(
            "ShippingAddress",
            th.ObjectType(
                th.Property("Name", th.StringType),
                th.Property("AddressLine1", th.StringType),
                th.Property("AddressLine2", th.StringType),
                th.Property("AddressLine3", th.StringType),
                th.Property("City", th.StringType),
                th.Property("County", th.StringType),
                th.Property("District", th.StringType),
                th.Property("StateOrRegion", th.StringType),
                th.Property("Municipality", th.StringType),
                th.Property("PostalCode", th.StringType),
                th.Property("CountryCode", th.StringType),
                th.Property("Phone", th.StringType),
                th.Property("AddressType", th.StringType),
            ),
        ),
    ).to_dict()

    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            order_id = context.get("AmazonOrderId", [])

            orders = self.get_sp_orders(context.get("marketplace_id"))
            items = orders.get_order_address(order_id=order_id).payload
            return [items]
        except Exception as e:
            raise InvalidResponse(e)


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
        th.Property("AmazonOrderId", th.StringType),
        th.Property("ShipmentEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property("RefundEventList", th.CustomType({"type": ["array", "string"]})),
        th.Property(
            "GuaranteeClaimEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "ChargebackEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "PayWithAmazonEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "ServiceProviderCreditEventList",
            th.CustomType({"type": ["array", "string"]}),
        ),
        th.Property(
            "RetrochargeEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "RentalTransactionEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "ProductAdsPaymentEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "ServiceFeeEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "SellerDealPaymentEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "DebtRecoveryEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "LoanServicingEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "AdjustmentEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "SAFETReimbursementEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "SellerReviewEnrollmentPaymentEventList",
            th.CustomType({"type": ["array", "string"]}),
        ),
        th.Property(
            "FBALiquidationEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "CouponPaymentEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "ImagingServicesFeeEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "NetworkComminglingTransactionEventList",
            th.CustomType({"type": ["array", "string"]}),
        ),
        th.Property(
            "AffordabilityExpenseEventList",
            th.CustomType({"type": ["array", "string"]}),
        ),
        th.Property(
            "AffordabilityExpenseReversalEventList",
            th.CustomType({"type": ["array", "string"]}),
        ),
        th.Property(
            "TrialShipmentEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "ShipmentSettleEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "TaxWithholdingEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "RemovalShipmentEventList", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property(
            "RemovalShipmentAdjustmentEventList",
            th.CustomType({"type": ["array", "string"]}),
        ),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            order_id = context.get("AmazonOrderId", [])

            finance = self.get_sp_finance(context.get("marketplace_id"))

            sandbox = self.config.get("sandbox", False)
            if sandbox is False:
                # self.state_partitioning_keys = self.partitions
                self.state_partitioning_keys = self.partitions[len(self.partitions) - 1]
                items = finance.get_financial_events_for_order(order_id).payload
                items["AmazonOrderId"] = order_id
            else:
                items = finance.get_financial_events_for_order("TEST_CASE_200").payload
            return [items["FinancialEvents"]]
        except Exception as e:
            raise InvalidResponse(e)


class ReportsStream(AmazonSellerStream):
    """Define custom stream."""

    name = "reports"
    primary_keys = ["reportId"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("marketplaceIds", th.CustomType({"type": ["array", "string"]})),
        th.Property("reportId", th.StringType),
        th.Property("reportType", th.StringType),
        th.Property("dataStartTime", th.DateTimeType),
        th.Property("dataEndTime", th.DateTimeType),
        th.Property("dataEndreportScheduleIdime", th.StringType),
        th.Property("createdTime", th.DateTimeType),
        th.Property("processingStatus", th.StringType),
        th.Property("processingStartTime", th.DateTimeType),
        th.Property("processingEndTime", th.DateTimeType),
        th.Property("reportDocumentId", th.StringType),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=10,
        factor=3,
    )
    @timeout(15)
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:

            report_types = self.config.get("report_types")
            processing_status = self.config.get("processing_status")
            marketplace_id = None
            if context is not None:
                marketplace_id = context.get("marketplace_id")

            report = self.get_sp_reports()

            items = report.get_reports(
                reportTypes=report_types, processingStatuses=processing_status
            ).payload
            return items["reports"]
        except Exception as e:
            raise InvalidResponse(e)
