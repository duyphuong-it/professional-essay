# Professional Essay Project

## Tổng quan hệ thống

...

- **Airflow**: Điều phối cho các pipeline ETL (nếu cần).
- **Postgres**: ...
- **DBT**: ...

Tất cả các thành phần đều được cấu hình để giao tiếp qua mạng Docker `tlcn-net`.

---

## Kiến trúc luồng dữ liệu ETL

1. **Data Source**: Dữ liệu gốc (có thể là file, database, API, v.v.).
2. **ETL với DBT và Airflow**:
   - ...
3. **Power BI**:
   - ...

---

## Hướng dẫn sử dụng hệ thống

### 1. Khởi động hệ thống

Để khởi động hệ thống, ta chạy như sau:

```sh
# Khởi động Airflow để orchestrate pipeline
docker compose -f tlcn_tools/docker-compose.yml up -d
```

Kiểm tra các container đã chạy bằng:

```sh
docker ps
```

### 5.2 Chạy DBT thủ công

1. **Truy cập Airflow Container**:

```bash
docker exec -it tlcn-airflow-scheduler bash
```

2. **Vào thư mục DBT Project**:

```bash
cd dbt_project
```

3. **Kiểm tra cấu hình**:

```bash
dbt debug
```

> **Thành công**: Ta thấy `All checks passed!` trong terminal output.

4. **Tạo và phục vụ DBT Docs**:

```bash
dbt docs generate
dbt docs serve --host 0.0.0.0
```

5. **Truy cập DBT UI**: [http://localhost:8090](http://localhost:8090)

### 3. Trực quan hóa dữ liệu với Power BI

- ...

---

## Một số lưu ý

- ...
- **Airflow**: Nếu sử dụng Airflow để orchestrate ETL, cấu hình DAGs trong thư mục `airflow/dags` và đảm bảo các connection tới Spark/HDFS đã được thiết lập.
- **Dừng hệ thống**:
  ```sh
  docker compose -f docker-compose.yml stop
  ```

---

## Tham khảo

- [Apache Airflow](https://airflow.apache.org/)
