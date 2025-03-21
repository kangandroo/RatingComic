import sys
import os
from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import QThreadPool
from ui.main_window import MainWindow
from utils.logger import setup_logger
from database.db_manager import DatabaseManager

def main():
    # Tạo thư mục cho các file database
    os.makedirs('data', exist_ok=True)
    
    # Thiết lập logging
    logger = setup_logger()
    logger.info("Khởi động ứng dụng phân tích truyện tranh")
    
    # Khởi tạo database
    db = DatabaseManager('data/comics.db')
    db.setup_database()
    
    # Khởi tạo thread pool
    logger.info(f"Số lượng thread tối đa: {QThreadPool.globalInstance().maxThreadCount()}")
    
    # Khởi tạo ứng dụng và cửa sổ chính
    app = QApplication(sys.argv)
    app.setStyle('Fusion')  # Sử dụng style Fusion cho giao diện nhất quán
    
    # Đặt stylesheet (tùy chọn)
    with open('utils/style.css', 'r') as f:
        app.setStyleSheet(f.read())
    
    # Khởi tạo cửa sổ chính
    main_window = MainWindow()
    main_window.show()
    
    # Chạy ứng dụng
    sys.exit(app.exec())

if __name__ == "__main__":
    main()