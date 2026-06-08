"""Create Kibana data views and a decision-oriented mobility dashboard."""

from __future__ import annotations

import json
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

KIBANA_URL = "http://kibana:5601"
ES_URL = "http://elasticsearch:9200"
DASHBOARD_ID = "inclusive_mobility_dashboard"
DASHBOARD_TITLE = "Should I Go Out Today?"
MAP_ID = "mobility_locations_native_map"
SCORE_ALIAS_VIEWS = [
    {
        "id": "inclusive_mobility_scores_low",
        "title": "inclusive_mobility_scores_low",
        "timeFieldName": "weather_date",
        "label": "Low score places",
        "color": "#dc2626",
        "filter": {"range": {"mobility_score": {"lt": 40}}},
    },
    {
        "id": "inclusive_mobility_scores_medium",
        "title": "inclusive_mobility_scores_medium",
        "timeFieldName": "weather_date",
        "label": "Medium score places",
        "color": "#f59e0b",
        "filter": {"range": {"mobility_score": {"gte": 40, "lt": 70}}},
    },
    {
        "id": "inclusive_mobility_scores_high",
        "title": "inclusive_mobility_scores_high",
        "timeFieldName": "weather_date",
        "label": "High score places",
        "color": "#16a34a",
        "filter": {"range": {"mobility_score": {"gte": 70}}},
    },
]

DATA_VIEWS = [
    {
        "id": "inclusive_mobility_scores",
        "title": "inclusive_mobility_scores",
        "timeFieldName": "weather_date",
    },
    {
        "id": "inclusive_mobility_risky_areas",
        "title": "inclusive_mobility_risky_areas",
        "timeFieldName": "weather_date",
    },
    {
        "id": "inclusive_mobility_improvement_priorities",
        "title": "inclusive_mobility_improvement_priorities",
        "timeFieldName": "weather_date",
    },
    {
        "id": "inclusive_mobility_city_daily_summary",
        "title": "inclusive_mobility_city_daily_summary",
        "timeFieldName": "weather_date",
    },
    {
        "id": "inclusive_mobility_scores_dashboard",
        "title": "inclusive_mobility_scores*",
    },
    {
        "id": "inclusive_mobility_city_summary_dashboard",
        "title": "inclusive_mobility_city_daily_summary*",
    },
] + [
    {
        "id": alias["id"],
        "title": alias["title"],
        "timeFieldName": alias["timeFieldName"],
    }
    for alias in SCORE_ALIAS_VIEWS
]


def _json(value: dict | list) -> str:
    return json.dumps(value, separators=(",", ":"))


def _api(method: str, path: str, body: dict | None = None) -> dict:
    url = f"{KIBANA_URL}{path}"
    data = json.dumps(body).encode("utf-8") if body else None
    headers = {"Content-Type": "application/json", "kbn-xsrf": "true"}
    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=30) as resp:
            response_body = resp.read()
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Kibana API {method} {path} failed with HTTP {exc.code}: {error_body}"
        ) from exc

    if not response_body:
        return {}
    return json.loads(response_body)


def _es_api(method: str, path: str, body: dict | None = None) -> dict:
    url = f"{ES_URL}{path}"
    data = json.dumps(body).encode("utf-8") if body else None
    headers = {"Content-Type": "application/json"}
    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=30) as resp:
            response_body = resp.read()
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Elasticsearch API {method} {path} failed with HTTP {exc.code}: {error_body}"
        ) from exc

    if not response_body:
        return {}
    return json.loads(response_body)


def _saved_object(
    object_type: str,
    object_id: str,
    attributes: dict,
    references: list[dict] | None = None,
) -> None:
    _api(
        "POST",
        f"/api/saved_objects/{object_type}/{object_id}?overwrite=true",
        {"attributes": attributes, "references": references or []},
    )


def wait_for_kibana(timeout: int = 120) -> None:
    """Block until Kibana API is ready."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = _api("GET", "/api/status")
            if resp.get("status", {}).get("overall", {}).get("level") == "available":
                print("[Kibana] Ready.")
                return
        except (URLError, OSError):
            pass
        time.sleep(3)
    raise TimeoutError("Kibana did not become ready")


def create_data_views() -> None:
    """Create all data views used by Discover, controls and dashboard panels."""
    wait_for_kibana()
    existing_ids = {
        view.get("id")
        for view in _api("GET", "/api/data_views").get("data_view", [])
    }

    for data_view in DATA_VIEWS:
        print(f"[Kibana] Creating data view: {data_view['title']}")
        if data_view["id"] in existing_ids:
            print("  Already exists.")
            continue

        body = {"id": data_view["id"], "title": data_view["title"]}
        if "timeFieldName" in data_view:
            body["timeFieldName"] = data_view["timeFieldName"]
        _api("POST", "/api/data_views/data_view", {"data_view": body})
        print("  Created.")


def create_score_aliases() -> None:
    """Create filtered aliases for map score classes."""
    print("[Elasticsearch] Creating score class aliases.")
    actions = []
    for alias in SCORE_ALIAS_VIEWS:
        actions.append(
            {
                "remove": {
                    "index": "inclusive_mobility_scores",
                    "alias": alias["id"],
                    "must_exist": False,
                }
            }
        )
        actions.append(
            {
                "add": {
                    "index": "inclusive_mobility_scores",
                    "alias": alias["id"],
                    "filter": alias["filter"],
                }
            }
        )
    _es_api("POST", "/_aliases", {"actions": actions})


def _vega_attrs(title: str, spec: dict) -> dict:
    return {
        "title": title,
        "visState": _json(
            {
                "title": title,
                "type": "vega",
                "params": {"spec": json.dumps(spec, indent=2)},
            }
        ),
        "uiStateJSON": "{}",
        "description": "",
        "version": 1,
        "kibanaSavedObjectMeta": {"searchSourceJSON": "{}"},
    }


def _search_source(index_ref_name: str) -> str:
    return _json(
        {
            "query": {"language": "kuery", "query": ""},
            "filter": [],
            "indexRefName": index_ref_name,
        }
    )


def _visualization_attrs(title: str, vis_state: dict, index_ref_name: str) -> dict:
    return {
        "title": title,
        "visState": _json(vis_state),
        "uiStateJSON": "{}",
        "description": "",
        "version": 1,
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": _search_source(index_ref_name)
        },
    }


def _index_ref(index_id: str, name: str = "source_index") -> list[dict]:
    return [{"name": name, "type": "index-pattern", "id": index_id}]


def _static_style(color: str) -> dict:
    return {"type": "STATIC", "options": {"color": color}}


def _static_size(size: int) -> dict:
    return {"type": "STATIC", "options": {"size": size}}


def _kpi_spec(
    title: str,
    index_name: str,
    agg_name: str,
    agg_body: dict,
    text_signal: str,
    color: str,
) -> dict:
    return {
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "autosize": "none",
        "width": 260,
        "height": 110,
        "padding": 8,
        "data": [
            {
                "name": "metric",
                "url": {
                    "%context%": True,
                    "index": index_name,
                    "body": {"size": 0, "aggs": {agg_name: agg_body}},
                },
                "format": {"property": "aggregations"},
            }
        ],
        "marks": [
            {
                "type": "text",
                "from": {"data": "metric"},
                "encode": {
                    "enter": {
                        "x": {"signal": "width / 2"},
                        "y": {"value": 44},
                        "align": {"value": "center"},
                        "baseline": {"value": "middle"},
                        "fontSize": {"value": 34},
                        "fontWeight": {"value": "bold"},
                        "fill": {"value": color},
                        "text": {"signal": text_signal},
                    }
                },
            },
            {
                "type": "text",
                "encode": {
                    "enter": {
                        "x": {"signal": "width / 2"},
                        "y": {"value": 82},
                        "align": {"value": "center"},
                        "baseline": {"value": "middle"},
                        "fontSize": {"value": 14},
                        "fill": {"value": "#475569"},
                        "text": {"value": title},
                    }
                },
            },
        ],
    }


def create_visualizations() -> None:
    """Create decision-focused Kibana visualizations."""
    vega_panels = [
        (
            "mobility_recommendation",
            "Recommendation",
            {
                "$schema": "https://vega.github.io/schema/vega/v5.json",
                "autosize": "none",
                "width": 300,
                "height": 110,
                "padding": 8,
                "data": [
                    {
                        "name": "summary",
                        "url": {
                            "%context%": True,
                            "index": "inclusive_mobility_city_daily_summary",
                            "body": {
                                "size": 1,
                                "sort": [{"avg_mobility_score": {"order": "desc"}}],
                                "_source": [
                                    "recommendation",
                                    "avg_mobility_score",
                                    "main_risk_reason",
                                ],
                            },
                        },
                        "format": {"property": "hits.hits"},
                    }
                ],
                "marks": [
                    {
                        "type": "text",
                        "from": {"data": "summary"},
                        "encode": {
                            "enter": {
                                "x": {"signal": "width / 2"},
                                "y": {"value": 36},
                                "align": {"value": "center"},
                                "baseline": {"value": "middle"},
                                "fontSize": {"value": 26},
                                "fontWeight": {"value": "bold"},
                                "fill": {
                                    "signal": "datum._source.recommendation == 'Good to go' ? '#15803d' : datum._source.recommendation == 'Go with caution' ? '#ca8a04' : '#b91c1c'"
                                },
                                "text": {"signal": "datum._source.recommendation"},
                            }
                        },
                    },
                    {
                        "type": "text",
                        "from": {"data": "summary"},
                        "encode": {
                            "enter": {
                                "x": {"signal": "width / 2"},
                                "y": {"value": 76},
                                "align": {"value": "center"},
                                "baseline": {"value": "middle"},
                                "fontSize": {"value": 14},
                                "fill": {"value": "#475569"},
                                "text": {
                                    "signal": "'Score ' + format(datum._source.avg_mobility_score, '.1f') + ' | ' + datum._source.main_risk_reason"
                                },
                            }
                        },
                    },
                ],
            },
        ),
        (
            "mobility_avg_score",
            "Avg mobility",
            _kpi_spec(
                "Avg mobility",
                "inclusive_mobility_city_daily_summary",
                "score",
                {"avg": {"field": "avg_mobility_score"}},
                "format(datum.score.value, '.1f')",
                "#1d4ed8",
            ),
        ),
        (
            "mobility_risky_places",
            "Risky places",
            _kpi_spec(
                "Risky places",
                "inclusive_mobility_city_daily_summary",
                "risk",
                {"sum": {"field": "risky_places_count"}},
                "datum.risk.value",
                "#b91c1c",
            ),
        ),
        (
            "mobility_safe_places",
            "Safe places",
            _kpi_spec(
                "Safe places",
                "inclusive_mobility_city_daily_summary",
                "safe",
                {"sum": {"field": "safe_places_count"}},
                "datum.safe.value",
                "#15803d",
            ),
        ),
        (
            "mobility_risk_reason",
            "Main risk reason",
            {
                "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                "autosize": "none",
                "width": 820,
                "height": 150,
                "padding": {"left": 150, "right": 20, "top": 8, "bottom": 35},
                "data": {
                    "url": {
                        "%context%": True,
                        "index": "inclusive_mobility_city_daily_summary",
                        "body": {
                            "size": 0,
                            "aggs": {
                                "reasons": {
                                    "terms": {
                                        "field": "main_risk_reason.keyword",
                                        "size": 4,
                                    }
                                }
                            },
                        },
                    },
                    "format": {"property": "aggregations.reasons.buckets"},
                },
                "mark": {"type": "bar", "color": "#64748b"},
                "encoding": {
                    "x": {"field": "doc_count", "type": "quantitative", "title": "Days"},
                    "y": {"field": "key", "type": "nominal", "title": "", "sort": "-x"},
                    "tooltip": [
                        {"field": "key", "type": "nominal", "title": "Risk"},
                        {"field": "doc_count", "type": "quantitative", "title": "Days"},
                    ],
                },
            },
        ),
        (
            "mobility_score_distribution",
            "Score distribution",
            {
                "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                "autosize": "none",
                "width": 820,
                "height": 150,
                "padding": {"left": 55, "right": 20, "top": 8, "bottom": 45},
                "data": {
                    "url": {
                        "%context%": True,
                        "index": "inclusive_mobility_scores",
                        "body": {
                            "size": 0,
                            "aggs": {
                                "score_hist": {
                                    "histogram": {
                                        "field": "mobility_score",
                                        "interval": 10,
                                        "min_doc_count": 0,
                                    }
                                }
                            },
                        },
                    },
                    "format": {"property": "aggregations.score_hist.buckets"},
                },
                "mark": {"type": "bar", "color": "#2563eb"},
                "encoding": {
                    "x": {"field": "key", "type": "quantitative", "title": "Score"},
                    "y": {"field": "doc_count", "type": "quantitative", "title": "Places"},
                    "tooltip": [
                        {"field": "key", "type": "quantitative", "title": "Score"},
                        {"field": "doc_count", "type": "quantitative", "title": "Places"},
                    ],
                },
            },
        ),
    ]

    for object_id, title, spec in vega_panels:
        print(f"[Kibana] Creating visualization: {title}")
        _saved_object("visualization", object_id, _vega_attrs(title, spec))

    table_and_map_panels = [
        (
            "mobility_top_safe_places",
            "Top safe places",
            {
                "title": "Top safe places",
                "type": "table",
                "params": {"perPage": 5, "showPartialRows": False, "showMetricsAtAllLevels": False},
                "aggs": [
                    {"id": "1", "enabled": True, "type": "max", "schema": "metric", "params": {"field": "mobility_score", "customLabel": "Mobility"}},
                    {"id": "2", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "name.keyword", "size": 5, "order": "desc", "orderBy": "1", "customLabel": "Place"}},
                    {"id": "3", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "city.keyword", "size": 1, "order": "desc", "orderBy": "1", "customLabel": "City"}},
                ],
            },
            "inclusive_mobility_scores_dashboard",
        ),
        (
            "mobility_places_to_avoid",
            "Places to avoid",
            {
                "title": "Places to avoid",
                "type": "table",
                "params": {"perPage": 5, "showPartialRows": False, "showMetricsAtAllLevels": False},
                "aggs": [
                    {"id": "1", "enabled": True, "type": "min", "schema": "metric", "params": {"field": "mobility_score", "customLabel": "Mobility"}},
                    {"id": "2", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "name.keyword", "size": 5, "order": "asc", "orderBy": "1", "customLabel": "Place"}},
                    {"id": "3", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "city.keyword", "size": 1, "order": "desc", "orderBy": "1", "customLabel": "City"}},
                ],
            },
            "inclusive_mobility_scores_dashboard",
        ),
    ]

    for object_id, title, vis_state, index_id in table_and_map_panels:
        print(f"[Kibana] Creating visualization: {title}")
        _saved_object(
            "visualization",
            object_id,
            _visualization_attrs(title, vis_state, "source_index"),
            _index_ref(index_id),
        )


def create_map() -> None:
    """Create the native Kibana Maps panel used by the dashboard."""
    basemap_layer = {
        "id": "mobility_basemap",
        "alpha": 1,
        "sourceDescriptor": {
            "type": "EMS_TMS",
            "isAutoSelect": True,
            "lightModeDefault": "road_map_desaturated",
        },
        "visible": True,
        "style": {},
        "type": "EMS_VECTOR_TILE",
        "minZoom": 0,
        "maxZoom": 24,
    }
    place_layers = []
    references = []
    for position, alias in enumerate(SCORE_ALIAS_VIEWS, start=1):
        ref_name = f"layer_{position}_source_index_pattern"
        references.append({"name": ref_name, "type": "index-pattern", "id": alias["id"]})
        place_layers.append(
            {
                "id": f"mobility_places_{alias['id']}",
                "label": alias["label"],
                "minZoom": 0,
                "maxZoom": 24,
                "alpha": 0.82,
                "sourceDescriptor": {
                    "id": f"mobility_places_source_{alias['id']}",
                    "type": "ES_SEARCH",
                    "geoField": "location",
                    "limit": 1000,
                    "filterByMapBounds": False,
                    "tooltipProperties": [
                        "name",
                        "city",
                        "weather_date",
                        "mobility_score",
                        "accessibility_score",
                        "weather_risk_score",
                        "recommendation",
                    ],
                    "indexPatternRefName": ref_name,
                    "applyGlobalQuery": True,
                    "applyGlobalTime": True,
                    "scalingType": "LIMIT",
                },
                "visible": True,
                "style": {
                    "type": "VECTOR",
                    "properties": {
                        "fillColor": _static_style(alias["color"]),
                        "lineColor": _static_style("#111827"),
                        "lineWidth": _static_size(1),
                        "iconSize": _static_size(9),
                        "symbolizeAs": {"options": {"value": "circle"}},
                        "icon": {"type": "STATIC", "options": {"value": "marker"}},
                    },
                },
                "type": "GEOJSON_VECTOR",
            }
        )
    _saved_object(
        "map",
        MAP_ID,
        {
            "title": "Recommended and risky places",
            "description": "Native map of mobility locations. Uses dashboard city/date filters and supports fit-to-data controls.",
            "layerListJSON": _json([basemap_layer, *place_layers]),
            "mapStateJSON": _json(
                {
                    "zoom": 5,
                    "center": {"lon": 2.2, "lat": 46.5},
                    "timeFilters": {"from": "now-30d", "to": "now+7d"},
                    "refreshConfig": {"isPaused": True, "interval": 0},
                    "query": {"query": "", "language": "kuery"},
                    "filters": [],
                    "settings": {
                        "autoFitToDataBounds": True,
                        "backgroundColor": "#ffffff",
                        "showScaleControl": True,
                        "showSpatialFilters": True,
                        "showTimesliderToggleButton": True,
                    },
                }
            ),
            "uiStateJSON": "{}",
        },
        references,
    )


def _control_group_input() -> dict:
    city_control_id = "city_control"
    date_control_id = "date_control"
    panels = {
        city_control_id: {
            "type": "optionsListControl",
            "order": 0,
            "grow": True,
            "width": "medium",
            "explicitInput": {
                "id": city_control_id,
                "dataViewId": "inclusive_mobility_scores_dashboard",
                "fieldName": "city.keyword",
                "title": "City",
                "searchTechnique": "prefix",
                "selectedOptions": [],
                "enhancements": {},
            },
        },
        date_control_id: {
            "type": "optionsListControl",
            "order": 1,
            "grow": False,
            "width": "medium",
            "explicitInput": {
                "id": date_control_id,
                "dataViewId": "inclusive_mobility_scores_dashboard",
                "fieldName": "weather_date",
                "title": "Weather date",
                "searchTechnique": "exact",
                "selectedOptions": [],
                "enhancements": {},
            },
        },
    }
    return {
        "chainingSystem": "HIERARCHICAL",
        "controlStyle": "oneLine",
        "ignoreParentSettingsJSON": _json(
            {
                "ignoreFilters": False,
                "ignoreQuery": False,
                "ignoreTimerange": True,
                "ignoreValidations": False,
            }
        ),
        "panelsJSON": _json(panels),
    }


def create_dashboard() -> None:
    """Create the functional decision dashboard."""
    panels_to_embed = [
        ("visualization", "mobility_recommendation"),
        ("visualization", "mobility_avg_score"),
        ("visualization", "mobility_risky_places"),
        ("visualization", "mobility_safe_places"),
        ("map", MAP_ID),
        ("visualization", "mobility_top_safe_places"),
        ("visualization", "mobility_places_to_avoid"),
        ("visualization", "mobility_risk_reason"),
        ("visualization", "mobility_score_distribution"),
    ]
    grid = [
        {"x": 0, "y": 0, "w": 12, "h": 7},
        {"x": 12, "y": 0, "w": 12, "h": 7},
        {"x": 24, "y": 0, "w": 12, "h": 7},
        {"x": 36, "y": 0, "w": 12, "h": 7},
        {"x": 0, "y": 7, "w": 48, "h": 22},
        {"x": 0, "y": 29, "w": 24, "h": 12},
        {"x": 24, "y": 29, "w": 24, "h": 12},
        {"x": 0, "y": 41, "w": 24, "h": 12},
        {"x": 24, "y": 41, "w": 24, "h": 12},
    ]
    panels = []
    references = []
    for i, (object_type, object_id) in enumerate(panels_to_embed):
        panel_index = str(i + 1)
        ref_name = f"panel_{i}"
        panels.append(
            {
                "version": "8.11.0",
                "type": object_type,
                "gridData": {**grid[i], "i": panel_index},
                "panelIndex": panel_index,
                "embeddableConfig": {},
                "panelRefName": ref_name,
            }
        )
        references.append({"name": ref_name, "type": object_type, "id": object_id})

    _saved_object(
        "dashboard",
        DASHBOARD_ID,
        {
            "title": DASHBOARD_TITLE,
            "hits": 0,
            "description": "Choose a city and weather date to decide whether going out is reasonable.",
            "panelsJSON": _json(panels),
            "optionsJSON": _json(
                {
                    "useMargins": True,
                    "syncColors": False,
                    "syncCursor": True,
                    "syncTooltips": True,
                    "hidePanelTitles": False,
                }
            ),
            "controlGroupInput": _control_group_input(),
            "timeRestore": True,
            "timeFrom": "now-30d",
            "timeTo": "now+7d",
            "version": 1,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": _json(
                    {"filter": [], "query": {"query": "", "language": "kuery"}}
                )
            },
        },
        references,
    )
    print(f"[Kibana] Dashboard ready: {DASHBOARD_TITLE}")


def setup_kibana_dashboards(**kwargs) -> str:
    """Airflow-callable entry point for Kibana setup."""
    print("Setting up Kibana data views and functional dashboard...")
    create_score_aliases()
    create_data_views()
    create_visualizations()
    create_map()
    create_dashboard()
    print(f"Kibana dashboard ready at {KIBANA_URL}")
    return KIBANA_URL


if __name__ == "__main__":
    setup_kibana_dashboards()
