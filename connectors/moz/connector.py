import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log

BASE_URL = "https://lsapi.seomoz.com/v2"
BATCH_SIZE = 10  # Moz recommends batching targets; max 50 per request


def schema(configuration: dict):
    return [
        {
            "table": "url_metrics",
            "primary_key": ["page", "target"],
            "columns": {
                "target": "STRING",
                "page": "STRING",
                "domain_authority": "FLOAT",
                "page_authority": "FLOAT",
                "spam_score": "FLOAT",
                "root_domains_to_root_domain": "INT",
                "external_equity_links": "INT",
            },
        },
        {
            "table": "linking_domains",
            "primary_key": ["target", "source_domain"],
            "columns": {
                "target": "STRING",
                "source_domain": "STRING",
                "domain_authority": "FLOAT",
                "spam_score": "FLOAT",
                "link_count": "INT",
                "nofollow_link_count": "INT",
            },
        },
        {
            "table": "top_pages",
            "primary_key": ["target", "page"],
            "columns": {
                "target": "STRING",
                "page": "STRING",
                "root_domains_to_page": "INT",
                "page_authority": "FLOAT",
                "external_equity_links": "INT",
            },
        },
        {
            "table": "anchor_text",
            "primary_key": ["target", "anchor_text"],
            "columns": {
                "target": "STRING",
                "anchor_text": "STRING",
                "internal_pages_linking": "INT",
                "external_links": "INT",
                "root_domains_linking": "INT",
            },
        },
    ]


def _auth(configuration: dict):
    return (configuration["access_id"], configuration["secret_key"])


def _targets(configuration: dict) -> list:
    raw = configuration.get("targets", "")
    return [t.strip() for t in raw.split(",") if t.strip()]


def _post(auth, endpoint: str, body: dict) -> dict:
    resp = requests.post(
        f"{BASE_URL}/{endpoint}",
        auth=auth,
        json=body,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def update(configuration: dict, state: dict):
    auth = _auth(configuration)
    targets = _targets(configuration)

    if not targets:
        log.warning("No targets configured — set 'targets' in configuration.json")
        return

    # --- URL Metrics ---
    log.info(f"Fetching url_metrics for {len(targets)} target(s)")
    for i in range(0, len(targets), BATCH_SIZE):
        batch = targets[i:i + BATCH_SIZE]
        data = _post(auth, "url_metrics", {"targets": batch})
        for item in data.get("results", []):
            yield op.upsert("url_metrics", {
                "target": item.get("url", ""),
                "page": item.get("page", item.get("url", "")),
                "domain_authority": item.get("domain_authority"),
                "page_authority": item.get("page_authority"),
                "spam_score": item.get("spam_score"),
                "root_domains_to_root_domain": item.get("root_domains_to_root_domain"),
                "external_equity_links": item.get("external_equity_links"),
            })

    # --- Linking Domains, Top Pages, Anchor Text (per target) ---
    for target in targets:
        log.info(f"Fetching linking_domains for {target}")
        offset = 0
        while True:
            data = _post(auth, "linking_domains", {
                "target": target,
                "limit": 50,
                "offset": offset,
            })
            results = data.get("results", [])
            for item in results:
                yield op.upsert("linking_domains", {
                    "target": target,
                    "source_domain": item.get("domain", ""),
                    "domain_authority": item.get("domain_authority"),
                    "spam_score": item.get("spam_score"),
                    "link_count": item.get("link_count"),
                    "nofollow_link_count": item.get("nofollow_link_count"),
                })
            if len(results) < 50:
                break
            offset += 50

        log.info(f"Fetching top_pages for {target}")
        offset = 0
        while True:
            data = _post(auth, "top_pages", {
                "target": target,
                "limit": 50,
                "offset": offset,
            })
            results = data.get("results", [])
            for item in results:
                yield op.upsert("top_pages", {
                    "target": target,
                    "page": item.get("page", ""),
                    "root_domains_to_page": item.get("root_domains_to_page"),
                    "page_authority": item.get("page_authority"),
                    "external_equity_links": item.get("external_equity_links"),
                })
            if len(results) < 50:
                break
            offset += 50

        log.info(f"Fetching anchor_text for {target}")
        offset = 0
        while True:
            data = _post(auth, "anchor_text", {
                "target": target,
                "limit": 50,
                "offset": offset,
            })
            results = data.get("results", [])
            for item in results:
                yield op.upsert("anchor_text", {
                    "target": target,
                    "anchor_text": item.get("anchor_text", ""),
                    "internal_pages_linking": item.get("internal_pages_linking"),
                    "external_links": item.get("external_links"),
                    "root_domains_linking": item.get("root_domains_linking"),
                })
            if len(results) < 50:
                break
            offset += 50

    yield op.checkpoint(state={})


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
