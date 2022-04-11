"""Custom client handling, including Amazon-SellerStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from sp_api.api import Orders
from sp_api.base import Marketplaces

from singer_sdk.streams import Stream


class AmazonSellerStream(Stream):
    """Stream class for Amazon-Seller streams."""

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        # TODO: Write logic to extract data from the upstream source.
        # rows = mysource.getall()
        # for row in rows:
        #     yield row.to_dict()
        raise NotImplementedError("The method is not yet implemented (TODO)")

    def get_credentials(self):
        return dict(
            refresh_token=self.config.get('refresh_token'),
            lwa_app_id=self.config.get('lwa_client_id'),
            lwa_client_secret=self.config.get('client_secret'),
            aws_access_key=self.config.get('aws_access_key'),
            aws_secret_key=self.config.get('aws_secret_key'),
            role_arn=self.config.get('role_arn'),
        )
    def get_sp_orders(self,marketplace_id=None):
        if marketplace_id is None:
                marketplace_id = self.config.get('marketplace','US')

        return Orders(credentials=self.get_credentials(),marketplace = Marketplaces[marketplace_id])        
