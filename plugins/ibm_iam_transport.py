# =============================================================================
# IBM watsonx.data Intelligence - OpenLineage Custom Transport
# =============================================================================
# Description:
#   Custom OpenLineage transport that sends lineage events to IBM watsonx.data
#   Intelligence using IBM Cloud IAM authentication. Automatically fetches and
#   renews Bearer tokens using an IBM API key, and sends events to the
#   watsonx.data lineage endpoint.
#  
#   The transport also removes "dataset":[] fields from columnLineage facets as they are not currently supported by .data intelligence
#
# Authentication:
#   IBM Cloud IAM tokens are fetched via API key exchange and cached until
#   60 seconds before expiry, at which point they are automatically renewed.
#
# Configuration (environment variables):
#   IBM_API_KEY          - IBM Cloud API key (required)
#   IBM_OPENLINEAGE_URL  - watsonx.data lineage endpoint (optional)
#                          Default: https://api.ca-tor.dai.cloud.ibm.com/
#                                   gov_lineage/v2/lineage_events/openlineage
#
# openlineage.yml usage:
#   transport:
#     type: "ibm_iam_transport.IbmWatsonxTransport"
#
# References:
#   IBM Cloud IAM:    https://cloud.ibm.com/docs/account?topic=account-iamtoken_from_apikey
#   OpenLineage:      https://openlineage.io/docs/client/python/
#   watsonx.data:     https://www.ibm.com/docs/en/watsonx/wdi/2.2.x?topic=lineage-openlineage-integration
#
# Author:      mark.brooks@ibm.com assisted by Claude Sonnet 4.6
# Created:     April 2026
# Modified:    May 2026 - Strip unsupported dataset:[] field from columnLineage facet
# =============================================================================
import os
import time
import threading
import requests
from openlineage.client.transport.transport import Transport, Config
from openlineage.client.serde import Serde


class IbmWatsonxTransportConfig(Config):
    def __init__(self):
        pass


class IbmWatsonxTransport(Transport):
    kind = "ibm_watsonx"
    config_class = IbmWatsonxTransportConfig

    IAM_TOKEN_URL = "https://iam.cloud.ibm.com/identity/token"

    def __init__(self, config: IbmWatsonxTransportConfig):
        self._api_key = os.environ["IBM_API_KEY"]
        self._url = os.environ.get(
            "IBM_OPENLINEAGE_URL",
            "https://api.ca-tor.dai.cloud.ibm.com/gov_lineage/v2/lineage_events/openlineage"
        )
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at: float = 0.0

    def _fetch_token(self) -> tuple[str, float]:
        resp = requests.post(
            self.IAM_TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
                "apikey": self._api_key,
            },
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json()
        token = payload["access_token"]
        expires_at = time.monotonic() + payload.get("expires_in", 3600) - 60
        return token, expires_at

    def _get_bearer(self) -> str:
        with self._lock:
            if self._token is None or time.monotonic() >= self._expires_at:
                self._token, self._expires_at = self._fetch_token()
        return f"Bearer {self._token}"

    @staticmethod
    def _strip_dataset_facet(event) -> None:
        """Remove the unsupported dataset:[] field from columnLineage facets."""
        for output in getattr(event, 'outputs', []):
            # handle facets as object or dict
            facets = getattr(output, 'facets', None) or {}
            if isinstance(facets, dict):
                cl = facets.get('columnLineage')
                if isinstance(cl, dict) and 'dataset' in cl:
                    del cl['dataset']
                elif cl is not None and hasattr(cl, 'dataset'):
                    cl.dataset = None
            else:
                cl = getattr(facets, 'columnLineage', None)
                if cl is not None:
                    if isinstance(cl, dict) and 'dataset' in cl:
                        del cl['dataset']
                    elif hasattr(cl, 'dataset'):
                        cl.dataset = None

    def emit(self, event):
        self._strip_dataset_facet(event)
        resp = requests.post(
            self._url,
            headers={
                "Authorization": self._get_bearer(),
                "Content-Type": "application/json",
            },
            data=Serde.to_json(event),
            timeout=30,
        )
        resp.raise_for_status()
        return resp
