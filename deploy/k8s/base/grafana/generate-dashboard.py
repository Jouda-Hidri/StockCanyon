"""Generates marketdata-dashboard.json. Edit here, not the JSON.

Rules the dashboard follows: one unit per panel (never two y-axes); headline states as stat
tiles whose colour always comes with text; series colours from a fixed categorical order validated
for the dark theme, assigned per series name so a series keeps its colour when others disappear;
status colours used only for status.
"""
import json

# Categorical, dark-surface steps, in fixed order.
C = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"]
GOOD, WARNING, CRITICAL = "#0ca30c", "#fab219", "#d03b3b"
NO_DATA = "#6e6e6b"  # neutral: a missing metric is not a good or bad state
DS = {"type": "prometheus", "uid": "${datasource}"}
API = 'uri="/api/v1/marketdata/quotes/{isin}/latest"'

panels, y = [], 0
next_id = iter(range(1, 1000))


def row(title):
    global y
    panels.append({"type": "row", "title": title, "id": next(next_id), "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []})
    y += 1


def series(title, unit, queries, x, w=8, h=8, description="", stack=False, names=()):
    """queries: list of (expr, legend). Colours follow legend order; single series is slot 1.
    names: the known values of a label-based legend, pinned to slots in this order."""
    fixed = [legend for _, legend in queries if not legend.startswith("{{")] + list(names)
    overrides = [{"matcher": {"id": "byName", "options": legend},
                  "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": C[i]}}]}
                 for i, legend in enumerate(fixed)]
    panels.append({
        "type": "timeseries", "title": title, "description": description, "id": next(next_id),
        "datasource": DS, "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": [{"refId": chr(65 + i), "datasource": DS, "expr": e, "legendFormat": l}
                    for i, (e, l) in enumerate(queries)],
        "fieldConfig": {"defaults": {
            "unit": unit, "min": 0,
            "color": {"mode": "fixed", "fixedColor": C[0]} if len(queries) == 1 and not queries[0][1].startswith("{{")
            else {"mode": "palette-classic"},
            "custom": {"lineWidth": 2, "fillOpacity": 0, "showPoints": "never", "spanNulls": True,
                       "axisSoftMin": 0, "stacking": {"mode": "normal" if stack else "none"}}},
            "overrides": overrides},
        "options": {"legend": {"displayMode": "list", "placement": "bottom",
                               "showLegend": len(queries) > 1 or queries[0][1].startswith("{{")},
                    "tooltip": {"mode": "multi", "sort": "desc"}}})


def stat(title, expr, x, steps, mappings, unit="none", w=3, description=""):
    """steps: [(value or None, colour)]; mappings: [(from, to, text)] so colour is never alone."""
    panels.append({
        "type": "stat", "title": title, "description": description, "id": next(next_id),
        "datasource": DS, "gridPos": {"h": 4, "w": w, "x": x, "y": y},
        "targets": [{"refId": "A", "datasource": DS, "expr": expr, "instant": True}],
        "fieldConfig": {"defaults": {
            "unit": unit, "color": {"mode": "thresholds"},
            "thresholds": {"mode": "absolute", "steps": [{"value": v, "color": c} for v, c in steps]},
            "mappings": [{"type": "special", "options": {"match": "null+nan",
                                                         "result": {"text": "No data", "color": NO_DATA}}}]
                        + [{"type": "range", "options": {"from": a, "to": b, "result": {"text": t}}}
                           for a, b, t in mappings]}, "overrides": []},
        "options": {"colorMode": "background", "graphMode": "none", "textMode": "value",
                    "justifyMode": "center", "reduceOptions": {"calcs": ["lastNotNull"], "values": False}}})


row("Health")
stat("Leader", "sum(marketdata_leader)", 0,
     [(None, CRITICAL), (1, GOOD), (2, WARNING)], [(0, 0, "No leader"), (1, 1, "Leading"), (2, 99, "Split")])
stat("Feed", "max(marketdata_feed_connected)", 3,
     [(None, CRITICAL), (1, GOOD)], [(0, 0, "Disconnected"), (1, 1, "Connected")])
stat("Queue", "max(marketdata_queue_paused)", 6,
     [(None, GOOD), (1, WARNING)], [(0, 0, "Flowing"), (1, 1, "Paused")],
     description="Paused: the socket is closed for back-pressure until the writer drains the queue.")
stat("Holes", "max(marketdata_sequence_outstanding)", 9,
     [(None, GOOD), (1, WARNING)], [(0, 0, "None")], description="Sequences missing right now, awaiting replay.")
stat("Lost", "sum(increase(marketdata_sequence_lost_total[24h]))", 12,
     [(None, GOOD), (1, CRITICAL)], [(0, 0, "None")], description="Messages written off in the last 24h, after replays failed.")
stat("Writes", "max(marketdata_db_write_consecutive_failures)", 15,
     [(None, GOOD), (1, CRITICAL)], [(0, 0, "OK")], description="Consecutive failed transactions, being retried.")
stat("Freshness p99", 'max(marketdata_projection_staleness_seconds{quantile="0.99"})', 18,
     [(None, GOOD), (1, WARNING), (5, CRITICAL)], [], unit="s", w=6,
     description="Exchange event time to the moment the API can serve it.")
y += 4

row("Ingestion")
series("Checkpoint lag", "s", [("max(marketdata_checkpoint_lag_seconds)", "checkpoint lag")], 0,
       description="Now minus the committed checkpoint's event time.")
series("Queue", "short", [("max(marketdata_queue_depth)", "depth"),
                          ("max(marketdata_queue_high_watermark)", "high watermark")], 8,
       description="The socket closes when depth reaches the high watermark.")
series("Throughput", "short", [("sum(rate(marketdata_db_write_quotes_total[1m]))", "quotes written /s"),
                               ("sum(rate(marketdata_db_write_rows_inserted_total[1m]))", "new rows /s")], 16,
       description="The difference is replayed duplicates dropped by the primary key.")
y += 8
series("Feed disconnects (5m)", "short",
       [('sum by (reason) (increase(marketdata_feed_disconnects_total[5m]))', "{{reason}}")], 0,
       names=("closed", "error", "stall", "connect_failed", "backpressure", "gap_backfill"),
       description="backpressure and gap_backfill are deliberate; the rest are failures.")
series("Sequence events (5m)", "short",
       [("sum(increase(marketdata_sequence_gaps_total[5m]))", "holes opened"),
        ("sum(increase(marketdata_gap_backfills_total[5m]))", "replays forced"),
        ("sum(increase(marketdata_sequence_lost_total[5m]))", "written off")], 8)
series("Ingest lag", "s",
       [('max(marketdata_ingest_lag_seconds{quantile="0.5"})', "p50"),
        ('max(marketdata_ingest_lag_seconds{quantile="0.99"})', "p99")], 16,
       description="Exchange event time to receipt.")
y += 8

row("Database")
series("Write latency", "s",
       [('max(marketdata_db_write_seconds{outcome="success",quantile="0.5"})', "p50"),
        ('max(marketdata_db_write_seconds{outcome="success",quantile="0.99"})', "p99")], 0,
       description="One batch transaction: fence, COPY, history, outbox, checkpoint.")
series("Write outcomes /s", "short",
       [('sum by (outcome) (rate(marketdata_db_write_seconds_count[1m]))', "{{outcome}}")], 8, names=("success", "failure", "fenced"))
series("Connections", "short",
       [("max(hikaricp_connections_active)", "active"), ("max(hikaricp_connections_pending)", "waiting")], 16)
y += 8

row("Outbox → Kafka → Redis")
series("Events /s", "short",
       [("sum(rate(marketdata_outbox_published_total[1m]))", "published"),
        ('sum(rate(marketdata_projection_events_total{outcome="applied"}[1m]))', "applied"),
        ('sum(rate(marketdata_projection_events_total{outcome="stale"}[1m]))', "stale (refused)"),
        ('sum(rate(marketdata_projection_events_total{outcome="invalid"}[1m]))', "invalid")], 0,
       description="Published by ingestion; applied, or refused as older, by distribution.")
series("End-to-end freshness", "s",
       [('max(marketdata_projection_staleness_seconds{quantile="0.5"})', "p50"),
        ('max(marketdata_projection_staleness_seconds{quantile="0.99"})', "p99")], 8)
series("Consumer lag", "short",
       [('max(kafka_consumer_fetch_manager_records_lag_max{client_id=~"latest-quote-projector.*"})', "lag")], 16,
       description="Events on the topic not yet applied to Redis.")
y += 8

row("API")
series("Requests /s", "reqps",
       [(f'sum by (status) (rate(http_server_requests_seconds_count{{{API}}}[1m]))', "{{status}}")], 0, names=("200", "404", "400", "503", "500"))
series("Latency", "s",
       [(f"histogram_quantile(0.5, sum by (le) (rate(http_server_requests_seconds_bucket{{{API}}}[5m])))", "p50"),
        (f"histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{{{API}}}[5m])))", "p99")], 8)
series("Errors", "percentunit",
       [(f'sum(rate(http_server_requests_seconds_count{{{API},status=~"5.."}}[5m])) '
         f"/ sum(rate(http_server_requests_seconds_count{{{API}}}[5m]))", "5xx share")], 16)
y += 8

row("JVM")
series("Heap used (top 5 pods)", "percentunit",
       [('topk(5, sum by (pod) (jvm_memory_used_bytes{area="heap"}) / sum by (pod) (jvm_memory_max_bytes{area="heap"}))',
         "{{pod}}")], 0, w=12)
series("CPU (top 5 pods)", "percentunit",
       [("topk(5, max by (pod) (process_cpu_usage))", "{{pod}}")], 12, w=12)

dashboard = {
    "uid": "stockcanyon-marketdata", "title": "Market data", "tags": ["marketdata"],
    "timezone": "utc", "schemaVersion": 39, "version": 1, "refresh": "30s",
    "time": {"from": "now-1h", "to": "now"}, "editable": True, "graphTooltip": 1,
    "templating": {"list": [{"name": "datasource", "label": "Data source", "type": "datasource",
                             "query": "prometheus", "current": {}, "hide": 0}]},
    "panels": panels,
}
with open("marketdata-dashboard.json", "w") as f:
    json.dump(dashboard, f, indent=2)
    f.write("\n")
print(f"{len(panels)} panels")
