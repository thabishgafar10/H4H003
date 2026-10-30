import json
import urllib.request
import pytest

ENDPOINTS = [
    ("GET", "http://127.0.0.1:8000/api/projects", None),
    ("GET", "http://127.0.0.1:8000/api/extraction/statistics?project_id=1&dataset_name=KafkaSQLite", None),
    ("GET", "http://127.0.0.1:8000/api/graph/stats?project_id=1&dataset_name=KafkaSQLite", None),
    ("GET", "http://127.0.0.1:8000/api/features?project_id=1&dataset_name=KafkaSQLite", None),
    ("GET", "http://127.0.0.1:8000/api/features/1?project_id=1", None),
    ("GET", "http://127.0.0.1:8000/api/features/1/history?project_id=1", None),
    ("POST", "http://127.0.0.1:8000/api/features/resolve", {"query": "KRaft", "project_id": 1}),
    ("GET", "http://127.0.0.1:8000/api/graph/features/1?depth=2&max_nodes=50&project_id=1&dataset_name=KafkaSQLite", None),
    ("GET", "http://127.0.0.1:8000/api/graph/nodes/ISSUE/KAFKA-1?depth=1&max_nodes=20&project_id=1", None),
    ("POST", "http://127.0.0.1:8000/api/qa/ask", {"query": "Why was ZooKeeper replaced by KRaft?", "project_id": 1}),
    ("GET", "http://127.0.0.1:8000/api/events?skip=0&limit=15&project_id=1&dataset_name=KafkaSQLite", None),
    ("GET", "http://127.0.0.1:8000/api/events/issues/KAFKA-1/timeline?project_id=1", None),
    ("GET", "http://127.0.0.1:8000/api/extraction/runs?limit=10&project_id=1&dataset_name=KafkaSQLite", None),
    ("GET", "http://127.0.0.1:8000/api/extraction/candidates?limit=25&project_id=1", None),
    ("GET", "http://127.0.0.1:8000/api/projects/1/runs", None),
    ("GET", "http://127.0.0.1:8000/api/projects/1/documents", None),
    ("POST", "http://127.0.0.1:8000/api/projects/1/sources/jira/validate", {"base_url": "https://jira.example.com", "email": "test@example.com", "api_token": "secret"}),
    ("POST", "http://127.0.0.1:8000/api/projects/1/sources/github/validate", {"repo_url": "https://github.com/apache/kafka"}),
]

@pytest.mark.parametrize("method,url,payload", ENDPOINTS)
def test_live_frontend_api_contract(method, url, payload):
    data = json.dumps(payload).encode() if payload else None
    headers = {"Content-Type": "application/json"} if payload else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=10) as resp:
        assert resp.status in (200, 201)
        raw = resp.read()
        res_json = json.loads(raw.decode("utf-8"))
        assert res_json is not None
