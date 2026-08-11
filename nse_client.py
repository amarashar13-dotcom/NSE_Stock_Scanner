import threading
import time
from datetime import date, timedelta

from curl_cffi import requests as cffi_requests


class NseClient:
    """Drop-in replacement for the `nse` library using curl_cffi with a real
    Chrome TLS fingerprint, which passes NSE's Akamai WAF from cloud servers.

    Returns the same JSON shapes as the `nse` package:
      - listIndices()                  -> {"data": [...], "timestamp", "marketStatus"}
      - listEquityStocksByIndex(name)  -> {"data": [...], "marketStatus", "timestamp"}
      - fetch_equity_historical_data() -> list of rows (chronological, oldest first)
    """

    BASE_URL = "https://www.nseindia.com"
    API_URL = BASE_URL + "/api"
    NEXT_API_URL = API_URL + "/NextApi/apiClient/GetQuoteApi"
    HANDSHAKE_URL = BASE_URL + "/option-chain"

    def __init__(self, download_folder=None, timeout=20):
        self.timeout = timeout
        self._session = cffi_requests.Session(impersonate="chrome")
        self._lock = threading.Lock()
        self._last_req = 0.0
        self._handshaked = False

    def _throttle(self):
        with self._lock:
            elapsed = time.monotonic() - self._last_req
            gap = 0.35 - elapsed
            if gap > 0:
                time.sleep(gap)
            self._last_req = time.monotonic()

    def _handshake(self):
        self._session.get(self.HANDSHAKE_URL, timeout=self.timeout)
        self._handshaked = True

    def _req(self, url, params=None):
        self._throttle()
        if not self._handshaked:
            self._handshake()
        for attempt in range(3):
            resp = self._session.get(url, params=params, timeout=self.timeout)
            if resp.status_code == 403:
                if attempt < 2:
                    self._handshake()
                    continue
                raise ConnectionError(f"{url} 403: Forbidden")
            if not 200 <= resp.status_code < 300:
                raise ConnectionError(f"{url} {resp.status_code}")
            return resp
        raise ConnectionError(f"{url} 403: Forbidden")

    def listIndices(self):
        return self._req(self.API_URL + "/allIndices").json()

    def listEquityStocksByIndex(self, index="NIFTY 50"):
        return self._req(
            self.API_URL + "/equity-stock-indices",
            params={"index": index.upper()},
        ).json()

    @staticmethod
    def _split_date_range(from_date, to_date, max_chunk_size=100):
        chunks = []
        cursor = from_date
        while cursor <= to_date:
            end = min(cursor + timedelta(days=max_chunk_size - 1), to_date)
            chunks.append((cursor, end))
            cursor = end + timedelta(days=1)
        return chunks

    def fetch_equity_historical_data(self, symbol, from_date=None, to_date=None, series="EQ"):
        if to_date is None:
            to_date = date.today()
        if from_date is None:
            from_date = to_date - timedelta(days=30)
        if to_date < from_date:
            raise ValueError("The from date must occur before the to date")

        data = []
        for chunk in self._split_date_range(from_date, to_date):
            params = {
                "functionName": "getHistoricalTradeData",
                "symbol": symbol,
                "series": series.upper(),
                "fromDate": chunk[0].strftime("%d-%m-%Y"),
                "toDate": chunk[1].strftime("%d-%m-%Y"),
            }
            rows = self._req(self.NEXT_API_URL, params=params).json()
            data += reversed(rows)
        return data
