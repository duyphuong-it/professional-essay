## Tổng quan hệ thống

...

- **MySQL**: Nguồn dữ liệu
- **Airflow**: Điều phối cho các pipeline ETL (nếu cần).
- **Postgres**: Kho dữ liệu
- **DBT**: công cụ xử lý, biến đổi dữ liệu

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
docker compose -f tlcn_tools/docker-compose.yml up -d
```

Kiểm tra các container đã chạy bằng:

```sh
docker ps
```

### 2. Tạo dữ liệu trong MySQL

1. **Truy cập MySQL container**

```bash
docker exec -it tlcn-mysql bash
```

2. **Chạy các lệnh sau thêm dữ liệu**

```bash
mysql -u root -p mysql < /var/lib/mysql-files/scripts/create_db.sql

mysql -u root -p uber_ride_bookings < /var/lib/mysql-files/scripts/import_data.sql
```

Lưu ý: Mỗi lần chạy lệnh sẽ được hỏi mật khẩu, nhập `root` để xác thực.

### 3. Kích hoạt Airflow DAG để thực hiện ETL

1. **Truy cập Airflow Web UI**:

   Truy cập [http://localhost:8080](http://localhost:8080)

   Nhập tài khoản và mật khẩu đề là `airflow` để đăng nhập.

   Tại giao diện chính của Airflow, chọn `Admin` > `Connections`.

   Tại giao diện `Connections`, nhấn nút `+` để thêm connection cho MySQL, điền các thông tin như sau:

   - Connection Id: `mysql_default`
   - Connection Type: `MySQL`
   - Host: `mysql`
   - Schema: `uber_ride_bookings`
   - Login: `root`
   - Password: `root`
   - Port: `3306`
   - Để trống các thông tin còn lại và chọn `Save`

   Sau khi trở về giao diện `Connections`, tiếp tục nhấn `+` để thêm connection cho PostgreSQL, điền các thông tin như sau:

   - Connection Id: `postgres_default`
   - Connection Type: `Postgres`
   - Host: `postgres`
   - Database: `warehouse`
   - Login: `admin`
   - Password: `admin`
   - Port: `5432`
   - Để trống các thông tin còn lại và chọn `Save`

   Sau khi trở về giao diện `Connections`, nhấn biểu tương `Airflow` hoặc chọn `DAGs` để trở về trang chủ.

2. **Chuyển dữ liệu từ MySQL sang tầng `bronze` của Postgres**:

   Tại trang chủ Airflow, chọn DAG có tên là `mysql_to_postgres_bronze` và kích hoạt DAG để chuyển dữ liệu.

   Đợi đến khi chạy xong, truy cập vào `Postgres` container để kiểm tra:

   ```bash
   docker exec -it tlcn-postgres psql -U admin -d warehouse
   ```

   Chạy lần lượt các lệnh sau:

   ```bash
   \dn

   \dt bronze.*

   SELECT * FROM bronze.ride_bookings LIMIT 5;
   ```

3. **Thực hiện quá trình ETL**:

   Tại trang chủ Airflow, chọn DAG có tên là `dbt_sql_parse_dag` và kích hoạt DAG để thực hiện quá trình ETL `bronze -> silver -> gold`.

   Đợi đến khi chạy xong, truy cập vào `Postgres` container để kiểm tra:

   ```bash
   docker exec -it tlcn-postgres psql -U admin -d warehouse
   ```

   Chạy lần lượt các lệnh sau:

   ```bash
   \dn

   \dt gold.*

   SELECT * FROM gold.fact_ride_booking LIMIT 5;
   ```

4. **Tạo và xem DBT Docs**:

   Truy cập Airflow Scheduler:

   ```bash
   docker exec -it tlcn-airflow-scheduler bash
   ```

   Chạy các lệnh sau:

   ```bash
   cd /opt/airflow/dbt_project
   dbt docs generate
   dbt docs serve --host 0.0.0.0
   ```

5. **Truy cập DBT UI**: [http://localhost:8090](http://localhost:8090)

### 4. Trực quan hóa dữ liệu với Power BI

- ...

---

## Một số lưu ý

- ...
- **Airflow**: Nếu sử dụng Airflow để orchestrate ETL, cấu hình DAGs trong thư mục `airflow/dags` và đảm bảo các connection tới MySQL và Postgres đã được thiết lập.
- **Dừng hệ thống**:
  ```sh
  docker compose -f tlcn_tools/docker-compose.yml stop
  ```

---

## Tham khảo

- [Apache Airflow](https://airflow.apache.org/)
