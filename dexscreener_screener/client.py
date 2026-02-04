"""DexScreener public API client: timeouts, retries, rate-limit friendly."""

import logging
import random
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://api.dexscreener.com"
CHAIN_SOLANA = "solana"
PAIRS_CHUNK_SIZE = 20
TOKENS_CHUNK_SIZE = 30


class DexScreenerClient:
    """HTTP client for DexScreener public API with retries and rate limiting."""

    def __init__(
        self,
        base_url: str = BASE_URL,
        chain_id: str = CHAIN_SOLANA,
        timeout_sec: float = 10.0,
        max_retries: int = 4,
        backoff_base: float = 0.5,
        rate_limit_rps: float = 3.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.chain_id = chain_id
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.rate_limit_rps = rate_limit_rps
        self._last_request_ts = 0.0

    def _throttle(self) -> None:
        min_interval = 1.0 / self.rate_limit_rps if self.rate_limit_rps > 0 else 0
        elapsed = time.monotonic() - self._last_request_ts
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_ts = time.monotonic()

    def _request(self, path: str) -> Any:
        url = f"{self.base_url}{path}"
        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            self._throttle()
            try:
                with httpx.Client(timeout=self.timeout_sec) as client:
                    resp = client.get(url)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"HTTP {resp.status_code}",
                        request=resp.request,
                        response=resp,
                    )
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.ConnectError) as e:
                last_exc = e
                if attempt < self.max_retries - 1:
                    delay = self.backoff_base * (2**attempt) + random.uniform(0, 0.2)
                    logger.warning(
                        "Request failed (attempt %s/%s), retry in %.2fs: %s",
                        attempt + 1,
                        self.max_retries,
                        delay,
                        e,
                    )
                    time.sleep(delay)
                else:
                    logger.error("Request failed after %s retries: %s", self.max_retries, e)
        if last_exc:
            raise last_exc
        raise RuntimeError("Request failed with no exception")

    def get_pairs_by_pair_addresses(self, pair_addresses: list[str]) -> list[dict]:
        """
        Fetch pairs by pair addresses. One request per pair (API accepts single pairId).
        GET /latest/dex/pairs/{chainId}/{pairId}.
        """
        if not pair_addresses:
            return []
        chain = self.chain_id
        all_pairs: list[dict] = []
        for pair_id in pair_addresses:
            path = f"/latest/dex/pairs/{chain}/{pair_id}"
            try:
                data = self._request(path)
            except Exception as e:
                logger.warning("get_pairs_by_pair_addresses failed for %s: %s", pair_id[:16], e)
                continue
            pairs = data.get("pairs")
            pair_one = data.get("pair")
            if isinstance(pairs, list):
                all_pairs.extend(pairs)
            elif isinstance(pair_one, dict) and pair_one.get("pairAddress"):
                all_pairs.append(pair_one)
            elif isinstance(data, dict) and data.get("pairAddress"):
                all_pairs.append(data)
        return all_pairs

    def get_pairs_by_token_addresses_batched(self, token_addresses: list[str]) -> list[dict]:
        """
        Fetch pairs by token addresses. Chunks by TOKENS_CHUNK_SIZE (max 30).
        GET /tokens/v1/{chainId}/{tokenAddresses}.
        """
        if not token_addresses:
            return []
        chain = self.chain_id
        all_pairs: list[dict] = []
        for i in range(0, len(token_addresses), TOKENS_CHUNK_SIZE):
            chunk = token_addresses[i : i + TOKENS_CHUNK_SIZE]
            addrs_param = ",".join(chunk)
            path = f"/tokens/v1/{chain}/{addrs_param}"
            try:
                data = self._request(path)
            except Exception as e:
                logger.warning("get_pairs_by_token_addresses_batched chunk failed: %s", e)
                continue
            if isinstance(data, list):
                all_pairs.extend(data)
            elif isinstance(data, dict) and data.get("pairs"):
                all_pairs.extend(data["pairs"])
            elif isinstance(data, dict) and "pairAddress" in data:
                all_pairs.append(data)
        return all_pairs

    def get_latest_token_profiles(self) -> list[str]:
        """
        Fetch latest token profiles. Returns Solana token addresses only.
        GET /token-profiles/latest/v1. Rate limit: 60 req/min for this endpoint.
        """
        path = "/token-profiles/latest/v1"
        data = self._request(path)
        items: list[dict] = []
        if isinstance(data, list):
            items = [x for x in data if isinstance(x, dict)]
        elif isinstance(data, dict):
            for key in ("profiles", "tokenProfiles", "token_profiles", "data"):
                if isinstance(data.get(key), list):
                    items = [x for x in data[key] if isinstance(x, dict)]
                    break
        addresses: list[str] = []
        for item in items:
            chain = str(item.get("chainId") or item.get("chain_id") or "").strip().lower()
            if chain != CHAIN_SOLANA:
                continue
            addr = (
                str(item.get("tokenAddress") or item.get("token_address") or item.get("address") or "")
            ).strip()
            if addr:
                addresses.append(addr)
        return addresses
