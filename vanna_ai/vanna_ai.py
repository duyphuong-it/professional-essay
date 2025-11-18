from vanna.ollama import Ollama
from vanna.chromadb import ChromaDB_VectorStore

# Custom Vanna class cho local (ChromaDB + Ollama)
class MyVanna(ChromaDB_VectorStore, Ollama):
    def __init__(self, config=None):
        self.config = config or {}
        ChromaDB_VectorStore.__init__(self, config=config)
        Ollama.__init__(self, config=config)

# Khởi tạo Vanna với Ollama model
vn = MyVanna(config={'model': 'mistral', 'allow_llm_to_see_data': True, 'allow_llm_to_see_schema': True})

# Kết nối PostgreSQL Warehouse
try:
    vn.connect_to_postgres(
        host='localhost',  # Hoặc 'postgres' nếu chạy trong Docker network
        dbname='warehouse',
        user='admin',
        password='admin',  # Mật khẩu từ Docker Compose
        port=5432
    )
    print("Test kết nối PostgreSQL:", vn.run_sql("SELECT 1 AS test;"))
except Exception as e:
    print(f"Lỗi kết nối PostgreSQL: {str(e)}")
    exit(1)

# DDL cho các bảng trong schema gold
ddl_statements = [
    """CREATE TABLE IF NOT EXISTS gold.dim_payment (
        payment_key BIGINT PRIMARY KEY,
        payment_method VARCHAR(50) NOT NULL UNIQUE,
        category VARCHAR(50) NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS gold.dim_time (
        time_key BIGINT PRIMARY KEY,
        time TIME NOT NULL UNIQUE,
        hour INTEGER NOT NULL,
        minute INTEGER NOT NULL,
        second INTEGER NOT NULL,
        time_of_day VARCHAR(20) NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS gold.dim_location (
        location_key BIGINT PRIMARY KEY,
        location VARCHAR(100) NOT NULL UNIQUE,
        city VARCHAR(50) NOT NULL,
        zone VARCHAR(50) NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS gold.dim_status (
        status_key BIGINT PRIMARY KEY,
        status_name VARCHAR(50) NOT NULL UNIQUE,
        CONSTRAINT valid_status CHECK (status_name IN ('Cancelled by Customer', 'Cancelled by Driver', 'Completed', 'Incomplete', 'No Driver Found'))
    );""",
    """CREATE TABLE IF NOT EXISTS gold.dim_date (
        date_key BIGINT PRIMARY KEY,
        full_date DATE NOT NULL UNIQUE,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        day INTEGER NOT NULL,
        quarter INTEGER NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS gold.dim_vehicle (
        vehicle_key BIGINT PRIMARY KEY,
        vehicle_type VARCHAR(50) NOT NULL UNIQUE,
        category VARCHAR(50) NOT NULL,
        capacity INTEGER NOT NULL,
        segment VARCHAR(50) NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS gold.fact_ride_booking (
        booking_key BIGINT PRIMARY KEY,
        date_key BIGINT NOT NULL,
        time_key BIGINT NOT NULL,
        pickup_key BIGINT NOT NULL,
        drop_key BIGINT NOT NULL,
        vehicle_key BIGINT NOT NULL,
        payment_key BIGINT,
        status_key BIGINT NOT NULL,
        cust_cancel BOOLEAN,
        cust_cancel_reason VARCHAR(200),
        driver_cancel BOOLEAN,
        driver_cancel_reason VARCHAR(200),
        avg_vtat NUMERIC(10,2),
        avg_ctat NUMERIC(10,2),
        booking_value NUMERIC(10,2),
        ride_distance NUMERIC(10,2),
        driver_rating NUMERIC(3,1),
        customer_rating NUMERIC(3,1),
        FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key),
        FOREIGN KEY (time_key) REFERENCES gold.dim_time(time_key),
        FOREIGN KEY (pickup_key) REFERENCES gold.dim_location(location_key),
        FOREIGN KEY (drop_key) REFERENCES gold.dim_location(location_key),
        FOREIGN KEY (vehicle_key) REFERENCES gold.dim_vehicle(vehicle_key),
        FOREIGN KEY (payment_key) REFERENCES gold.dim_payment(payment_key),
        FOREIGN KEY (status_key) REFERENCES gold.dim_status(status_key)
    );"""
]

# Huấn luyện Vanna với DDL
try:
    for ddl in ddl_statements:
        vn.train(ddl=ddl)
    print("DDL training completed.")
except Exception as e:
    print(f"Lỗi khi train DDL: {str(e)}")

# Thêm documentation
vn.train(documentation="""
Warehouse là database cho dịch vụ đặt xe (giống Uber). 
- gold.dim_payment: Thông tin phương thức thanh toán (payment_method, category).
- gold.dim_time: Thông tin thời gian (time, hour, minute, second, time_of_day).
- gold.dim_location: Địa điểm đón/trả khách (location, city, zone).
- gold.dim_status: Trạng thái chuyến xe (status_name, ví dụ: 'Completed', 'Cancelled by Customer').
- gold.dim_date: Thông tin ngày (full_date, year, month, day, quarter).
- gold.dim_vehicle: Loại xe (vehicle_type, category, capacity, segment).
- gold.fact_ride_booking: Bảng fact chứa thông tin chuyến xe, liên kết với các bảng dimension qua các khóa (date_key, time_key, pickup_key, drop_key, vehicle_key, payment_key, status_key). Bao gồm số liệu như booking_value (giá trị chuyến), ride_distance (khoảng cách), driver_rating, customer_rating.
Sử dụng JOIN để truy vấn dữ liệu phức tạp. Dữ liệu số dùng NUMERIC, ngày dùng DATE, thời gian dùng TIME. Tất cả bảng nằm trong schema gold.
""")

# Thêm SQL mẫu (sử dụng schema gold)
sql_examples = [
    "SELECT COUNT(*) AS num_payments FROM gold.dim_payment;",
    "SELECT v.vehicle_type, COUNT(f.booking_key) AS num_rides FROM gold.fact_ride_booking f JOIN gold.dim_vehicle v ON f.vehicle_key = v.vehicle_key GROUP BY v.vehicle_type ORDER BY num_rides DESC LIMIT 5;",
    "SELECT l.city, SUM(f.booking_value) AS total_revenue FROM gold.fact_ride_booking f JOIN gold.dim_location l ON f.pickup_key = l.location_key GROUP BY l.city ORDER BY total_revenue DESC;",
    "SELECT s.status_name, COUNT(f.booking_key) AS num_bookings FROM gold.fact_ride_booking f JOIN gold.dim_status s ON f.status_key = s.status_key GROUP BY s.status_name;",
    "SELECT d.year, d.quarter, AVG(f.driver_rating) AS avg_driver_rating FROM gold.fact_ride_booking f JOIN gold.dim_date d ON f.date_key = d.date_key GROUP BY d.year, d.quarter HAVING AVG(f.driver_rating) > 4.0;",
    "SELECT p.payment_method, SUM(f.booking_value) AS total_value FROM gold.fact_ride_booking f JOIN gold.dim_payment p ON f.payment_key = p.payment_key GROUP BY p.payment_method ORDER BY total_value DESC;",
    "SELECT t.time_of_day, AVG(f.ride_distance) AS avg_distance FROM gold.fact_ride_booking f JOIN gold.dim_time t ON f.time_key = t.time_key GROUP BY t.time_of_day ORDER BY avg_distance DESC;"
]

try:
    for sql in sql_examples:
        vn.train(sql=sql)
    print("SQL training completed.")
except Exception as e:
    print(f"Lỗi khi train SQL: {str(e)}")

print("Training hoàn tất! Training data:", len(vn.get_training_data()))

# Chạy UI web
if __name__ == "__main__":
    from vanna.flask import VannaFlaskApp
    print("\nKhởi động UI web tại http://localhost:8000 (Ctrl+C để dừng).")
    app = VannaFlaskApp(vn)
    app.run(host="0.0.0.0", port=8000, debug=True)