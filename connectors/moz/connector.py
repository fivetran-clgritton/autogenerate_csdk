# Built using Connector SDK AI Plugin
# Plugin: fivetran-connector-sdk (claude-code) | Version: 2026.11.18.3
"""This connector syncs SEO link metrics from the Moz Links API (v2).
It delivers URL metrics, linking domains, top pages and anchor text for the configured targets.
See the Technical Reference documentation (https://fivetran.com/docs/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connector-sdk#connectordevelopmentbestpractices) for details
"""

# For adding a delay between retries
import time

# For making API requests
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert() and checkpoint()
from fivetran_connector_sdk import Operations as op

__BASE_URL = "https://lsapi.seomoz.com/v2"
__URL_METRICS_BATCH_SIZE = 10  # Moz recommends batching targets; max 50 per request
__PAGE_SIZE = 50
__MAX_RETRIES = 5
__BACKOFF_BASE_SECONDS = 2
__REQUEST_TIMEOUT_SECONDS = 30


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or no targets are listed.
    """
    for key in ["access_id", "secret_key", "targets"]:
        if not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")
    if not parse_targets(configuration):
        raise ValueError("Configuration value 'targets' must list at least one domain or URL")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    Only primary keys and the metric columns whose type must be forced are declared, so the SDK infers the rest.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "url_metrics",
            "primary_key": ["page", "target"],
            "columns": {"domain_authority": "FLOAT", "page_authority": "FLOAT", "spam_score": "FLOAT"},
        },
        {
            "table": "linking_domains",
            "primary_key": ["target", "source_domain"],
            "columns": {"domain_authority": "FLOAT", "spam_score": "FLOAT"},
        },
        {
            "table": "top_pages",
            "primary_key": ["target", "page"],
            "columns": {"page_authority": "FLOAT"},
        },
        {"table": "anchor_text", "primary_key": ["target", "anchor_text"]},
    ]


def parse_targets(configuration: dict):
    """
    Split the comma-separated 'targets' configuration value into a list of targets.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        A list of non-empty, stripped target strings.
    """
    raw = configuration.get("targets", "")
    return [t.strip() for t in raw.split(",") if t.strip()]


def make_request(auth: tuple, endpoint: str, body: dict):
    """
    Send a POST request to the Moz API, retrying transient failures with exponential backoff.
    Args:
        auth: (access_id, secret_key) tuple used for HTTP Basic authentication.
        endpoint: the endpoint name under the base URL.
        body: the JSON request body.
    Returns:
        The parsed JSON response body.
    Raises:
        RuntimeError: on authentication failures, other 4xx responses, or when retries are exhausted.
    """
    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.post(
                f"{__BASE_URL}/{endpoint}",
                auth=auth,
                json=body,
                timeout=__REQUEST_TIMEOUT_SECONDS,
            )
        except (requests.ConnectionError, requests.Timeout) as error:
            if attempt == __MAX_RETRIES - 1:
                raise RuntimeError(f"Moz request failed after retries: {error}") from error
            delay = __BACKOFF_BASE_SECONDS**attempt
            log.warning(f"Network error on {endpoint}, retrying in {delay}s: {error}")
            time.sleep(delay)
            continue

        if response.status_code in (401, 403):
            raise RuntimeError(
                f"Moz rejected the credentials (HTTP {response.status_code}). Check access_id and secret_key."
            )
        if response.status_code == 429 or response.status_code >= 500:
            # Honor Retry-After when the API sends it, otherwise back off exponentially.
            retry_after = response.headers.get("Retry-After", "")
            delay = int(retry_after) if retry_after.isdigit() else __BACKOFF_BASE_SECONDS**attempt
            log.warning(f"HTTP {response.status_code} on {endpoint}, retrying in {delay}s")
            time.sleep(delay)
            continue
        if not response.ok:
            raise RuntimeError(f"Moz request to {endpoint} failed: HTTP {response.status_code}")
        return response.json()
    raise RuntimeError(f"Moz request to {endpoint} still failing after {__MAX_RETRIES} retries")


def sync_url_metrics(auth: tuple, targets: list):
    """
    Upsert URL metrics for all targets, requesting them in batches.
    Args:
        auth: (access_id, secret_key) tuple.
        targets: the list of targets to fetch metrics for.
    """
    log.info(f"Fetching url_metrics for {len(targets)} target(s)")
    for i in range(0, len(targets), __URL_METRICS_BATCH_SIZE):
        batch = targets[i:i + __URL_METRICS_BATCH_SIZE]
        data = make_request(auth, "url_metrics", {"targets": batch})
        for item in data.get("results", []):
            # The 'upsert' operation inserts or updates a row by primary key.
            op.upsert(table="url_metrics", data={
                "target": item.get("url", ""),
                "page": item.get("page", item.get("url", "")),
                "domain_authority": item.get("domain_authority"),
                "page_authority": item.get("page_authority"),
                "spam_score": item.get("spam_score"),
                "root_domains_to_root_domain": item.get("root_domains_to_root_domain"),
                "external_equity_links": item.get("external_equity_links"),
            })


def sync_paginated(auth: tuple, endpoint: str, target: str, build_row):
    """
    Page through a limit/offset endpoint for one target and upsert each row into the table of the same name.
    Args:
        auth: (access_id, secret_key) tuple.
        endpoint: the endpoint name, which is also the destination table name.
        target: the target domain or URL being queried.
        build_row: function that maps one API result item to the row dictionary to upsert.
    """
    log.info(f"Fetching {endpoint} for {target}")
    offset = 0
    total = 0
    while True:
        data = make_request(
            auth, endpoint, {"target": target, "limit": __PAGE_SIZE, "offset": offset}
        )
        results = data.get("results", [])
        for item in results:
            op.upsert(table=endpoint, data=build_row(item))
        total += len(results)
        if len(results) < __PAGE_SIZE:
            break
        offset += __PAGE_SIZE
    log.info(f"Synced {total} {endpoint} rows for {target}")


def update(configuration: dict, state: dict):
    """
    Define the update function, which is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    validate_configuration(configuration=configuration)
    auth = (configuration["access_id"], configuration["secret_key"])
    targets = parse_targets(configuration)

    sync_url_metrics(auth, targets)

    for target in targets:
        sync_paginated(auth, "linking_domains", target, lambda item, t=target: {
            "target": t,
            "source_domain": item.get("domain", ""),
            "domain_authority": item.get("domain_authority"),
            "spam_score": item.get("spam_score"),
            "link_count": item.get("link_count"),
            "nofollow_link_count": item.get("nofollow_link_count"),
        })
        sync_paginated(auth, "top_pages", target, lambda item, t=target: {
            "target": t,
            "page": item.get("page", ""),
            "root_domains_to_page": item.get("root_domains_to_page"),
            "page_authority": item.get("page_authority"),
            "external_equity_links": item.get("external_equity_links"),
        })
        sync_paginated(auth, "anchor_text", target, lambda item, t=target: {
            "target": t,
            "anchor_text": item.get("anchor_text", ""),
            "internal_pages_linking": item.get("internal_pages_linking"),
            "external_links": item.get("external_links"),
            "root_domains_linking": item.get("root_domains_linking"),
        })

    # All Moz tables are full refreshes, so there is no cursor to keep. The checkpoint
    # marks the end of a successful sync.
    op.checkpoint(state={})


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
if __name__ == "__main__":
    connector.debug()
