"""Editable dashboard source; generated JSON is delivered by file provisioning."""

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent / "monitoring"
DS = {"type": "prometheus", "uid": "prometheus"}
STATE_MAPPINGS = [
    {
        "type": "value",
        "options": {
            "0": {"text": "Offline", "color": "#8e8e8e"},
            "1": {"text": "Готов", "color": "green"},
            "2": {"text": "Занят", "color": "blue"},
        },
    }
]


def panel(
    title, kind, x, y, w, h, queries=(), unit="short", description="", mappings=None
):
    targets = [
        {
            "refId": chr(65 + i),
            "expr": expr,
            "legendFormat": legend,
            "datasource": DS,
            "range": True,
            "instant": False,
        }
        for i, (expr, legend) in enumerate(queries)
    ]
    return {
        "title": title,
        "type": kind,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "description": description,
        "targets": targets,
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "noValue": "—",
                "color": {"mode": "palette-classic"},
                "mappings": mappings or [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [{"color": "green", "value": None}],
                },
                "custom": {"lineWidth": 2, "fillOpacity": 10, "spanNulls": False},
            },
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom"},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
    }


def stat(title, expr, x, unit="short", description="", health=False):
    result = panel(title, "stat", x, 0, 4, 4, [(expr, "")], unit, description)
    result["targets"][0].update(range=False, instant=True)
    defaults = result["fieldConfig"]["defaults"]
    defaults["color"] = {"mode": "thresholds"}
    if "ошиб" in title.lower() or "lease за период" in title:
        defaults["thresholds"]["steps"].extend(
            [{"color": "orange", "value": 1}, {"color": "red", "value": 5}]
        )
    elif "доступны" in title.lower():
        defaults["thresholds"]["steps"] = [
            {"color": "#8e8e8e", "value": None},
            {"color": "green", "value": 1},
        ]
    result["options"] = {
        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        "orientation": "auto",
        "textMode": "auto",
        "colorMode": "value",
        "graphMode": "none",
    }
    if health:
        result["fieldConfig"]["defaults"]["mappings"] = [
            {
                "type": "value",
                "options": {
                    "0": {"text": "Недоступен", "color": "red"},
                    "1": {"text": "Доступен", "color": "green"},
                },
            }
        ]
    return result


def timeline(expr, x, y, w=24):
    result = panel(
        "Доступность и занятость воркеров",
        "state-timeline",
        x,
        y,
        w,
        8,
        [(expr, "{{worker_id}}")],
        mappings=STATE_MAPPINGS,
        description="Offline — нет ready-отчёта в пределах TTL (90 с по умолчанию). Готов — модель загружена, Занят — есть действующий lease. Пробел означает отсутствие наблюдений, а не offline. Простой Mac/WSL ожидаем и не является аварией.",
    )
    result["options"] = {
        "mergeValues": True,
        "showValue": "auto",
        "alignValue": "center",
        "rowHeight": 0.8,
        "legend": {"displayMode": "list", "placement": "bottom"},
        "tooltip": {"mode": "single"},
    }
    result["fieldConfig"]["defaults"]["custom"] = {
        "lineWidth": 0,
        "fillOpacity": 85,
        "spanNulls": False,
    }
    return result


def text(title, content, y):
    result = panel(title, "text", 0, y, 24, 4)
    result["options"] = {"mode": "markdown", "content": content}
    return result


def dashboard(uid, title, panels, variable=False):
    for index, item in enumerate(panels, 1):
        item["id"] = index
    variables = []
    if variable:
        query = "label_values(transcriber_worker_online, worker_id)"
        variables = [
            {
                "name": "worker",
                "label": "Воркер",
                "type": "query",
                "datasource": DS,
                "definition": query,
                "query": {"query": query, "refId": "worker-variable"},
                "refresh": 1,
                "multi": True,
                "includeAll": True,
                "allValue": ".*",
                "current": {"text": "All", "value": "$__all"},
                "sort": 1,
            }
        ]
    return {
        "uid": uid,
        "title": title,
        "id": None,
        "version": 1,
        "schemaVersion": 41,
        "tags": ["transcriber", "workers", "whisper"],
        "timezone": "browser",
        "editable": False,
        "refresh": "15s",
        "time": {"from": "now-6h", "to": "now"},
        "annotations": {"list": []},
        "templating": {"list": variables},
        "panels": panels,
        "links": [
            {
                "title": "Обзор",
                "url": "/d/transcriber-overview",
                "type": "link",
                "keepTime": True,
            },
            {
                "title": "Воркеры",
                "url": "/d/transcriber-workers",
                "type": "link",
                "keepTime": True,
            },
        ],
    }


def dashboards():
    overview = [
        stat("Метрики очереди", 'max(up{service="transcriber"})', 0, health=True),
        stat(
            "Mac / WSL доступны",
            'sum(transcriber_worker_online{worker_role="preferred"})',
            4,
            description="Число ready worker_id, а не число устройств. На WSL CPU и GPU — два воркера.",
        ),
        stat("В очереди", 'sum(transcriber_jobs{status="queued"})', 8),
        stat("Выполняются", "sum(transcriber_worker_active_jobs)", 12),
        stat(
            "Готово за период",
            "sum(increase(transcriber_jobs_completed_total[$__range]))",
            16,
            description="Prometheus increase экстраполирует края диапазона, поэтому результат может быть дробным.",
        ),
        stat(
            "Ошибок за период",
            "sum(increase(transcriber_jobs_failed_total[$__range]))",
            20,
            description="Терминальные ошибки; отмены пользователем учитываются отдельно.",
        ),
        timeline("transcriber_worker_state", 0, 4, 12),
        panel(
            "Очередь и активная обработка",
            "timeseries",
            12,
            4,
            12,
            8,
            [
                ('sum(transcriber_jobs{status="queued"})', "В очереди"),
                ("sum(transcriber_worker_active_jobs)", "Активные lease"),
                (
                    'sum(transcriber_jobs{status=~"accepted|downloading"})',
                    "Загрузка аудио",
                ),
            ],
        ),
        panel(
            "Возраст самого старого задания в очереди",
            "timeseries",
            0,
            12,
            8,
            7,
            [("transcriber_queue_oldest_wait_seconds", "Ожидание")],
            "s",
        ),
        panel(
            "Расшифровок в минуту",
            "timeseries",
            8,
            12,
            8,
            7,
            [
                (
                    'sum by(worker_id)(rate(transcriber_jobs_completed_total{worker_id!~"ingress|other"}[$__rate_interval])) * 60',
                    "{{worker_id}}",
                )
            ],
        ),
        panel(
            "Повторы, ошибки попыток и истёкшие lease / мин",
            "timeseries",
            16,
            12,
            8,
            7,
            [
                (
                    "sum(rate(transcriber_retry_attempts_total[$__rate_interval])) * 60",
                    "Повторные попытки",
                ),
                (
                    "sum(rate(transcriber_attempt_failures_total[$__rate_interval])) * 60",
                    "Ошибки попыток",
                ),
                (
                    "sum(rate(transcriber_lease_expirations_total[$__rate_interval])) * 60",
                    "Истёкшие lease",
                ),
            ],
        ),
        panel(
            "Длительность P95",
            "timeseries",
            0,
            19,
            12,
            8,
            [
                (
                    "histogram_quantile(0.95, sum by(le)(rate(transcriber_job_queue_wait_seconds_bucket[$__rate_interval])))",
                    "Ожидание до первой попытки",
                ),
                (
                    "histogram_quantile(0.95, sum by(le)(rate(transcriber_job_processing_seconds_bucket[$__rate_interval])))",
                    "Успешная попытка",
                ),
                (
                    "histogram_quantile(0.95, sum by(le)(rate(transcriber_job_turnaround_seconds_bucket[$__rate_interval])))",
                    "От создания до готового результата",
                ),
            ],
            "s",
            description="При отсутствии завершений P95 не определён: это нормальное No data. Гистограммы используют фиксированные интервалы, поэтому P95 — оценка.",
        ),
        panel(
            "Доставка результатов и восстановление",
            "timeseries",
            12,
            19,
            12,
            8,
            [
                ("transcriber_results_unacknowledged", "Результаты без Ack"),
                ("transcriber_queue_retry_jobs", "Повторы в очереди"),
                ("transcriber_expired_leases", "Ожидают восстановления lease"),
            ],
            description="Ack подтверждает сохранение результата клиентом; не подтверждает доставку сообщения в Telegram.",
        ),
        text(
            "Как читать этот дашборд",
            "История собирается с момента установки мониторинга. **Offline у Mac/WSL ожидаем**: задания сохраняются в очереди, серверный fallback работает при отсутствии приоритетных воркеров. Отсутствие метрик означает неизвестное состояние. Счётчики и гистограммы сохраняются в PostgreSQL и переживают рестарты и TTL-очистку заданий. Метрики не содержат аудио, текста, job_id или токенов. [Подробнее о воркерах](/d/transcriber-workers)",
            27,
        ),
    ]
    selector = '{worker_id=~"$worker",worker_id!~"ingress|other"}'
    workers = [
        stat("Доступны выбранные", "sum(transcriber_worker_online" + selector + ")", 0),
        stat(
            "Активные задания", "sum(transcriber_worker_active_jobs" + selector + ")", 4
        ),
        stat(
            "Готово за период",
            "sum(increase(transcriber_jobs_completed_total" + selector + "[$__range]))",
            8,
        ),
        stat(
            "Ошибки попыток за период",
            "sum(increase(transcriber_attempt_failures_total"
            + selector
            + "[$__range]))",
            12,
        ),
        stat(
            "Истёкшие lease за период",
            "sum(increase(transcriber_lease_expirations_total"
            + selector
            + "[$__range]))",
            16,
        ),
        stat("Контроль метрик", 'max(up{service="transcriber"})', 20, health=True),
        timeline("transcriber_worker_state" + selector, 0, 4),
        panel(
            "Текущее состояние",
            "table",
            0,
            12,
            8,
            8,
            [("transcriber_worker_state" + selector, "")],
            mappings=STATE_MAPPINGS,
        ),
        panel(
            "С последнего отчёта о доступности",
            "timeseries",
            8,
            12,
            8,
            8,
            [
                (
                    "transcriber_worker_last_report_age_seconds" + selector,
                    "{{worker_id}}",
                )
            ],
            "s",
        ),
        panel(
            "Доступность за выбранный период",
            "bargauge",
            16,
            12,
            8,
            8,
            [
                (
                    "avg_over_time(transcriber_worker_online"
                    + selector
                    + "[$__range]) * 100",
                    "{{worker_id}}",
                )
            ],
            "percent",
            description="Доля online среди успешных наблюдений Prometheus. Периоды без работающего exporter и история до установки не учитываются.",
        ),
        panel(
            "Расшифровок в минуту",
            "timeseries",
            0,
            20,
            12,
            8,
            [
                (
                    "sum by(worker_id)(rate(transcriber_jobs_completed_total"
                    + selector
                    + "[$__rate_interval])) * 60",
                    "{{worker_id}}",
                )
            ],
        ),
        panel(
            "P95 длительности успешной попытки",
            "timeseries",
            12,
            20,
            12,
            8,
            [
                (
                    "histogram_quantile(0.95, sum by(le,worker_id)(rate(transcriber_job_processing_seconds_bucket"
                    + selector
                    + "[$__rate_interval])))",
                    "{{worker_id}}",
                )
            ],
            "s",
            description="Зависит от длины аудио и выбранной модели. Это не сравнение скорости моделей на одинаковом аудио.",
        ),
        panel(
            "Прогресс текущих заданий",
            "timeseries",
            0,
            28,
            12,
            7,
            [("transcriber_worker_progress_percent" + selector, "{{worker_id}}")],
            "percent",
        ),
        panel(
            "Возраст текущей попытки",
            "timeseries",
            12,
            28,
            12,
            7,
            [("transcriber_worker_attempt_age_seconds" + selector, "{{worker_id}}")],
            "s",
        ),
        panel(
            "Готовые инстансы каждого worker_id",
            "timeseries",
            0,
            35,
            12,
            7,
            [("transcriber_worker_instances" + selector, "{{worker_id}}")],
            description="Значение больше 1 может означать временное перекрытие при рестарте или дублирующий запуск.",
        ),
        panel(
            "Ошибки попыток / мин",
            "timeseries",
            12,
            35,
            12,
            7,
            [
                (
                    "sum by(worker_id)(rate(transcriber_attempt_failures_total"
                    + selector
                    + "[$__rate_interval])) * 60",
                    "{{worker_id}}",
                )
            ],
        ),
        text(
            "Семантика доступности",
            "Ready означает, что модель загружена и воркер сообщает о готовности. Занятость определяется действующим lease; доступность не является гарантией успешной расшифровки. Ноль прогресса означает idle или отсутствие ещё полученных сегментов. **RAM/VRAM удалённых устройств здесь пока не собираются.** [Очередь и доставка результатов](/d/transcriber-overview)",
            42,
        ),
    ]
    state = workers[7]
    state["targets"][0].update(format="table", instant=True, range=False)
    state["options"] = {"showHeader": True, "cellHeight": "sm"}
    state["transformations"] = [
        {
            "id": "organize",
            "options": {
                "excludeByName": {
                    "Time": True,
                    "instance": True,
                    "job": True,
                    "service": True,
                },
                "renameByName": {
                    "worker_id": "Воркер",
                    "worker_role": "Роль",
                    "Value": "Состояние",
                },
            },
        }
    ]
    uptime = workers[9]
    uptime["targets"][0].update(instant=True, range=False)
    uptime["options"] = {
        "orientation": "horizontal",
        "displayMode": "basic",
        "showUnfilled": True,
        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
    }
    uptime["fieldConfig"]["defaults"].update(min=0, max=100)
    return {
        "transcriber-overview.json": dashboard(
            "transcriber-overview", "Расшифровки · обзор", overview
        ),
        "transcriber-workers.json": dashboard(
            "transcriber-workers", "Расшифровки · воркеры", workers, True
        ),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--rules", action="store_true")
    args = parser.parse_args()
    documents = dashboards()
    if args.rules:
        rules = []
        for document in documents.values():
            for item in document["panels"]:
                for target in item["targets"]:
                    expression = (
                        target["expr"]
                        .replace("$__rate_interval", "1m")
                        .replace("$__range", "6h")
                        .replace("$worker", ".*")
                    )
                    rules.append(
                        {
                            "record": "transcriber_dashboard_expression_"
                            + str(len(rules)),
                            "expr": expression,
                        }
                    )
        print(json.dumps({"groups": [{"name": "dashboard-syntax", "rules": rules}]}))
        return
    for name, document in documents.items():
        path = ROOT / "grafana" / name
        content = json.dumps(document, ensure_ascii=False, indent=2) + "\n"
        if args.check:
            if not path.exists() or path.read_text() != content:
                raise SystemExit("Regenerate dashboards with make dashboards: " + name)
            ids = [item["id"] for item in document["panels"]]
            assert len(ids) == len(set(ids))
            assert all(
                item["gridPos"]["x"] + item["gridPos"]["w"] <= 24
                for item in document["panels"]
            )
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
    target = json.loads((ROOT / "prometheus/transcriber.yml").read_text())
    assert target[0]["targets"] == ["transcriber-metrics:9108"]
    print(
        "Monitoring configuration is current."
        if args.check
        else "Rendered two dashboards."
    )


if __name__ == "__main__":
    main()
