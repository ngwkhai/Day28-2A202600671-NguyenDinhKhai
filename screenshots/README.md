# Screenshots

Điền ảnh chụp màn hình vào đây theo đúng tên file (theo cấu trúc yêu cầu trong `SUBMISSION.md`):

| File | Nội dung | Lệnh/URL để lấy |
|---|---|---|
| `prefect_ui.png` | Prefect UI — deployment `kafka-to-delta` (status READY) hoặc 1 flow run Completed | `open http://localhost:4200/deployments` |
| `api_gateway.png` | Kết quả `curl http://localhost:8000/health` | `curl http://localhost:8000/health` |
| `grafana_dashboard.png` | Dashboard "Lab28 API Gateway" (Prometheus datasource, 3 panel) | `open http://localhost:3000/d/asbkbm/lab28-api-gateway` (login admin/admin) |

Hai ảnh còn lại theo cấu trúc `SUBMISSION.md` đặt ở **thư mục gốc** repo (không phải trong `screenshots/`):

| File (ở root) | Nội dung | Lệnh |
|---|---|---|
| `smoke_tests_results.png` | Kết quả `pytest smoke-tests/ -v` — kỳ vọng 8/8 (5/5 test class) PASSED | `set -a && source .env && set +a && pytest smoke-tests/ -v` |
| `production_readiness.png` | Kết quả production readiness check — kỳ vọng >=80% | `python3 scripts/production_readiness_check.py` |
