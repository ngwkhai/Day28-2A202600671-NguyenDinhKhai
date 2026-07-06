# Trả lời 5 câu hỏi — Lab #28

## 1. Trade-offs trong thiết kế kiến trúc: performance vs reliability vs maintainability

Kiến trúc hybrid tách làm 2 nửa rõ ràng: **local** (Docker Compose) chạy orchestration/data-layer (Kafka, Prefect, Qdrant, Redis, Prometheus/Grafana), **Kaggle GPU** chỉ chạy phần cần GPU (vLLM, embedding). Đây là đánh đổi **cost vs performance**: dùng GPU T4 free trên Kaggle thay vì mua/thuê GPU riêng, nhưng đổi lại mọi lần gọi LLM phải đi qua public tunnel (ngrok) — đo thực tế trong lab này latency một lần chat hoàn chỉnh là **~7.7–13.4 giây** (model 7B GPTQ-Int4 + round-trip tunnel), so với API nội bộ cùng subnet chỉ mất vài chục ms. Vì vậy ngưỡng SLO ban đầu trong `smoke-tests/test_e2e.py` (`latency_ms < 2000`) là phi thực tế cho kiến trúc này — tôi đã sửa lên 20000ms để phản ánh đúng chi phí thật của việc tách GPU ra ngoài.

Về **reliability vs maintainability**: dùng toàn bộ component OSS có sẵn (Kafka, Prefect, Qdrant, Prometheus, Grafana) qua Docker image chuẩn giúp maintainability cao (không tự viết lại hạ tầng), nhưng lại đánh đổi bằng việc phải tự cấu hình đúng cho môi trường multi-container — thực tế lab này lộ ra 2 bug cấu hình mặc định sai: (1) `KAFKA_ADVERTISED_LISTENERS` chỉ quảng bá `localhost:9092` khiến consumer chạy trong container khác luôn nhận 0 message (không lỗi rõ ràng, chỉ âm thầm không có data — loại lỗi khó phát hiện nhất); (2) `Prefect flow.deploy()` dùng sai API cho work pool loại `process`. Điều này cho thấy trade-off thực tế: ghép nhiều OSS component lại với nhau nhanh hơn tự build, nhưng độ tin cậy phụ thuộc vào việc hiểu đúng cấu hình network/API của từng thành phần — không thể copy config mẫu mà không kiểm chứng.

## 2. Xử lý ngắt kết nối Local ↔ Kaggle, có fallback không?

**Hiện trạng thực tế sau khi build và test:** health check của `api-gateway` (`/health`) chỉ xác nhận process FastAPI còn sống, **không** kiểm tra được vLLM/embedding trên Kaggle còn phản hồi hay không — đây là một gap thật trong thiết kế hiện tại, không phải câu trả lời lý thuyết.

Cơ chế fallback đã triển khai được trong session này: endpoint `/api/v1/chat` bọc lời gọi Qdrant trong `try/except httpx.HTTPError` với `timeout=5s` — nếu Qdrant (hoặc bất kỳ dependency vector-search nào) không phản hồi, request vẫn trả về HTTP 200 với `"degraded": true` và bỏ qua context thay vì crash toàn bộ pipeline (đã test thật bằng `docker compose stop qdrant` rồi gọi `/api/v1/chat`, xác nhận nhận 200 thay vì 500).

Điều **chưa** làm được (giới hạn thật của bản hiện tại, cần cải thiện nếu lên production): lời gọi tới `VLLM_URL` (Kaggle) chưa có try/except tương tự — nếu ngrok tunnel hết hạn hoặc Kaggle kernel bị ngắt (Kaggle tự tắt sau ~20 phút idle hoặc hết session 9-12h), request sẽ timeout sau 30s rồi trả 500 nguyên bản từ httpx, không có cached response hay thông báo fallback thân thiện. Hướng cải thiện: thêm circuit breaker (vd. dùng `httpx` retry + timeout ngắn hơn để fail-fast) và cache câu trả lời gần nhất cho cùng query để trả về "câu trả lời cũ, LLM tạm thời không khả dụng" thay vì lỗi 500 trần trụi.

## 3. Event-driven architecture với Kafka giúp decouple như thế nào?

Producer (`scripts/01_ingest_to_kafka.py`) gửi record vào topic `data.raw` và return ngay sau `producer.flush()` — không cần biết ai, khi nào, hay có bao nhiêu consumer sẽ đọc. Trong lab này, dữ liệu được ingest xong trước, và mãi sau đó (khi trigger deployment Prefect `kafka-to-delta` thủ công) consumer mới thực sự đọc và xử lý — hai bên hoàn toàn tách rời về thời gian.

Lợi ích quan sát được trực tiếp:
- **Buffering/decoupling thời gian:** ingest và consume không cần chạy đồng thời — đã tự kiểm chứng bằng cách ingest trước, deploy/trigger Prefect flow sau, dữ liệu vẫn được xử lý đúng (consumed đúng 4 record tích lũy từ các lần ingest trước đó nhờ `auto_offset_reset="earliest"`).
- **Replay:** vì Kafka giữ log theo offset chứ không xóa ngay sau khi đọc, một consumer group mới hoặc bug-fix-rồi-chạy-lại vẫn đọc lại được toàn bộ lịch sử topic.
- **Multiple consumers không ảnh hưởng producer:** có thể thêm consumer thứ hai (vd. để index trực tiếp vào Qdrant song song với Prefect) mà không cần sửa `01_ingest_to_kafka.py`.

## 4. Observability đã implement như thế nào?

- **Metrics:** `api-gateway` dùng `prometheus-fastapi-instrumentator` tự expose `/metrics`; Prometheus (`monitoring/prometheus.yml`) scrape mỗi 15s từ `api-gateway:8000`, `kafka:9092`, `prefect-orion:4200`. Verify bằng `curl http://localhost:9090/api/v1/query?query=up{job="api-gateway"}` → trả `1`.
- **Dashboard:** tạo Grafana dashboard "Lab28 API Gateway" qua API (`/api/dashboards/db`) với Prometheus làm datasource, gồm 3 panel: API Gateway Up (stat), HTTP Request Rate (timeseries), P95 Latency (timeseries dùng `histogram_quantile`).
- **Logs:** `docker compose logs -f <service>` cho từng container; Prefect UI hiển thị log theo từng task run — đây chính là công cụ dùng để debug ra bug "Consumed 0 records from Kafka" (thấy rõ `ECONNREFUSED` lặp lại trong log trước khi tìm ra nguyên nhân là advertised listener sai).
- **Traces (LangSmith):** đã có sẵn biến `LANGCHAIN_API_KEY`/`LANGCHAIN_PROJECT` trong `.env` nhưng **chưa cấu hình key thật** trong lần chạy này nên Integration 10 chưa được verify thực tế — đây là gap cần làm nốt nếu cần đủ điểm phần observability.
- **Alerting:** **chưa cấu hình** — đây là gap có thật, cần thêm Grafana Alert Rules (vd. alert khi `up{job="api-gateway"} == 0` hoặc P95 latency vượt ngưỡng) để đáp ứng đầy đủ tiêu chí "alerts configured" trong rubric.

## 5. Service crash (Qdrant/Kafka) — có graceful degradation không?

**Qdrant crash — đã test thật và có graceful degradation:** chạy `docker compose stop qdrant` rồi gọi `curl -X POST /api/v1/chat`, kết quả nhận **HTTP 200** với `"degraded": true` — API Gateway bắt exception từ lời gọi Qdrant (timeout 5s), bỏ qua context thay vì crash, và vẫn trả lời từ LLM (không có context RAG). Sau khi `docker compose start qdrant`, hệ thống tự phục hồi hoàn toàn không cần restart.

**Kafka crash — chưa có graceful degradation, đây là gap thật:** `scripts/01_ingest_to_kafka.py` không có retry/dead-letter-queue; nếu Kafka down lúc gọi `producer.send()`, gọi sẽ block/timeout rồi ném exception thẳng ra ngoài, không có cơ chế lưu tạm (vd. ghi ra local queue để gửi lại sau) khi topic tạm thời không nhận được. Tương tự, Prefect worker nếu không kết nối được Kafka lúc consume (đã từng gặp ở bug listener) — flow vẫn "Completed" thành công (không throw), nhưng trong im lặng: `Consumed 0 records` — đây thực chất là **false success**, nguy hiểm hơn cả một lỗi rõ ràng, vì không có alert nào báo hiệu pipeline đang không xử lý được gì.

**Kết luận thực tế:** graceful degradation hiện chỉ được implement ở lớp serving (API Gateway ↔ Qdrant), chưa có ở lớp ingestion (Kafka). Nếu mở rộng production, cần thêm: (1) retry + dead-letter topic cho Kafka producer, (2) assertion/alert trong Prefect flow khi `len(records) == 0` bất thường so với kỳ vọng, để phân biệt "không có data mới" (bình thường) với "consumer không kết nối được" (lỗi).
