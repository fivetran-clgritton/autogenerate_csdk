import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log


BASE_URL = "https://api.semrush.com/"


def schema(configuration: dict):
    return [
        {
            "table": "domain_ranks",
            "primary_key": ["domain", "database"],
            "columns": {
                "domain": "STRING",
                "database": "STRING",
                "rank": "INT",
                "visits": "INT",
                "desktop_visits": "INT",
                "mobile_visits": "INT",
                "desktop_share": "FLOAT",
                "mobile_share": "FLOAT",
                "keywords": "INT",
                "traffic": "FLOAT",
                "traffic_cost": "FLOAT",
                "adwords_keywords": "INT",
                "adwords_traffic": "FLOAT",
                "adwords_traffic_cost": "FLOAT",
            },
        },
        {
            "table": "keyword_overview",
            "primary_key": ["keyword", "database"],
            "columns": {
                "keyword": "STRING",
                "database": "STRING",
                "search_volume": "INT",
                "cpc": "FLOAT",
                "competition": "FLOAT",
                "results": "INT",
                "trend": "STRING",
                "keyword_difficulty": "INT",
            },
        },
        {
            "table": "backlinks_overview",
            "primary_key": ["domain"],
            "columns": {
                "domain": "STRING",
                "total": "INT",
                "domains_num": "INT",
                "urls_num": "INT",
                "ips_num": "INT",
                "ipclassc_num": "INT",
                "follows_num": "INT",
                "nofollows_num": "INT",
                "sponsored_num": "INT",
                "ugc_num": "INT",
                "texts_num": "INT",
                "images_num": "INT",
                "forms_num": "INT",
                "frames_num": "INT",
            },
        },
        {
            "table": "organic_positions",
            "primary_key": ["domain", "keyword", "database"],
            "columns": {
                "domain": "STRING",
                "database": "STRING",
                "keyword": "STRING",
                "position": "INT",
                "previous_position": "INT",
                "search_volume": "INT",
                "keyword_difficulty": "INT",
                "cpc": "FLOAT",
                "url": "STRING",
                "traffic": "FLOAT",
                "traffic_percent": "FLOAT",
                "traffic_cost": "FLOAT",
                "competition": "FLOAT",
                "results": "INT",
                "trends": "STRING",
                "timestamp": "UTC_DATETIME",
            },
        },
    ]


def _get(api_key: str, params: dict) -> str:
    params["key"] = api_key
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.text


def _parse_semrush_csv(text: str) -> list[dict]:
    """Semrush API returns ';'-delimited CSV with a header row."""
    lines = [l for l in text.strip().splitlines() if l]
    if len(lines) < 2:
        return []
    headers = [h.strip() for h in lines[0].split(";")]
    rows = []
    for line in lines[1:]:
        values = [v.strip() for v in line.split(";")]
        rows.append(dict(zip(headers, values)))
    return rows


def _safe_int(v):
    try:
        return int(v) if v not in (None, "", "N/A") else None
    except (ValueError, TypeError):
        return None


def _safe_float(v):
    try:
        return float(v) if v not in (None, "", "N/A") else None
    except (ValueError, TypeError):
        return None


def update(configuration: dict, state: dict):
    api_key = configuration["api_key"]
    database = configuration.get("database", "us")
    domains = [d.strip() for d in configuration.get("domains", "").split(",") if d.strip()]
    keywords = [k.strip() for k in configuration.get("keywords", "").split(",") if k.strip()]

    # --- Domain ranks ---
    for domain in domains:
        log.info(f"Fetching domain rank for {domain}")
        try:
            text = _get(api_key, {
                "type": "domain_rank",
                "domain": domain,
                "database": database,
                "export_columns": "Dn,Rk,Or,Ot,Oc,Ad,At,Ac,Dt,De",
            })
            rows = _parse_semrush_csv(text)
            for row in rows:
                yield op.upsert("domain_ranks", {
                    "domain": domain,
                    "database": database,
                    "rank": _safe_int(row.get("Rk")),
                    "keywords": _safe_int(row.get("Or")),
                    "traffic": _safe_float(row.get("Ot")),
                    "traffic_cost": _safe_float(row.get("Oc")),
                    "adwords_keywords": _safe_int(row.get("Ad")),
                    "adwords_traffic": _safe_float(row.get("At")),
                    "adwords_traffic_cost": _safe_float(row.get("Ac")),
                })
        except Exception as e:
            log.warning(f"domain_rank failed for {domain}: {e}")

    # --- Keyword overview ---
    for keyword in keywords:
        log.info(f"Fetching keyword overview for '{keyword}'")
        try:
            text = _get(api_key, {
                "type": "phrase_this",
                "phrase": keyword,
                "database": database,
                "export_columns": "Ph,Nq,Cp,Co,Nr,Td,Kd",
            })
            rows = _parse_semrush_csv(text)
            for row in rows:
                yield op.upsert("keyword_overview", {
                    "keyword": keyword,
                    "database": database,
                    "search_volume": _safe_int(row.get("Nq")),
                    "cpc": _safe_float(row.get("Cp")),
                    "competition": _safe_float(row.get("Co")),
                    "results": _safe_int(row.get("Nr")),
                    "trend": row.get("Td"),
                    "keyword_difficulty": _safe_int(row.get("Kd")),
                })
        except Exception as e:
            log.warning(f"keyword_overview failed for '{keyword}': {e}")

    # --- Backlinks overview ---
    for domain in domains:
        log.info(f"Fetching backlinks overview for {domain}")
        try:
            text = _get(api_key, {
                "type": "backlinks_overview",
                "target": domain,
                "target_type": "root_domain",
                "export_columns": "ascore,total,domains_num,urls_num,ips_num,ipclassc_num,follows_num,nofollows_num,sponsored_num,ugc_num,texts_num,images_num,forms_num,frames_num",
            })
            rows = _parse_semrush_csv(text)
            for row in rows:
                yield op.upsert("backlinks_overview", {
                    "domain": domain,
                    "total": _safe_int(row.get("total")),
                    "domains_num": _safe_int(row.get("domains_num")),
                    "urls_num": _safe_int(row.get("urls_num")),
                    "ips_num": _safe_int(row.get("ips_num")),
                    "ipclassc_num": _safe_int(row.get("ipclassc_num")),
                    "follows_num": _safe_int(row.get("follows_num")),
                    "nofollows_num": _safe_int(row.get("nofollows_num")),
                    "sponsored_num": _safe_int(row.get("sponsored_num")),
                    "ugc_num": _safe_int(row.get("ugc_num")),
                    "texts_num": _safe_int(row.get("texts_num")),
                    "images_num": _safe_int(row.get("images_num")),
                    "forms_num": _safe_int(row.get("forms_num")),
                    "frames_num": _safe_int(row.get("frames_num")),
                })
        except Exception as e:
            log.warning(f"backlinks_overview failed for {domain}: {e}")

    # --- Organic positions (incremental, page through results) ---
    for domain in domains:
        log.info(f"Fetching organic positions for {domain}")
        offset = 0
        page_size = 50
        while True:
            try:
                text = _get(api_key, {
                    "type": "domain_organic",
                    "domain": domain,
                    "database": database,
                    "display_limit": page_size,
                    "display_offset": offset,
                    "export_columns": "Ph,Po,Pp,Nq,Kd,Cp,Ur,Tr,Tc,Co,Nr,Td",
                })
                rows = _parse_semrush_csv(text)
                if not rows:
                    break
                for row in rows:
                    yield op.upsert("organic_positions", {
                        "domain": domain,
                        "database": database,
                        "keyword": row.get("Ph"),
                        "position": _safe_int(row.get("Po")),
                        "previous_position": _safe_int(row.get("Pp")),
                        "search_volume": _safe_int(row.get("Nq")),
                        "keyword_difficulty": _safe_int(row.get("Kd")),
                        "cpc": _safe_float(row.get("Cp")),
                        "url": row.get("Ur"),
                        "traffic": _safe_float(row.get("Tr")),
                        "traffic_cost": _safe_float(row.get("Tc")),
                        "competition": _safe_float(row.get("Co")),
                        "results": _safe_int(row.get("Nr")),
                        "trends": row.get("Td"),
                    })
                if len(rows) < page_size:
                    break
                offset += page_size
            except Exception as e:
                log.warning(f"organic_positions failed for {domain} at offset {offset}: {e}")
                break

    yield op.checkpoint(state={})


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
