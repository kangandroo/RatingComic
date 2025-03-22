from PyQt6.QtWidgets import (QMainWindow, QTabWidget, QVBoxLayout, 
                            QWidget, QHBoxLayout, QTextEdit, QLabel, 
                            QSplitter, QMessageBox, QPushButton)
from PyQt6.QtCore import Qt
import logging
import sys

from ui.website_tab import WebsiteTab
from ui.analysis_tab import DetailAnalysisTab
from ui.settings_tab import SettingsTab
from utils.multi_db_manager import MultipleDBManager
from crawlers.crawler_factory import CrawlerFactory

logger = logging.getLogger(__name__)

class LogHandler(logging.Handler):
    """
    Handler tùy chỉnh để chuyển log messages đến QTextEdit
    """
    
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget
        self.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    def emit(self, record):
        msg = self.format(record)
        self.text_widget.append(msg)
        self.text_widget.ensureCursorVisible()

class MainWindow(QMainWindow):
    """
    Cửa sổ chính của ứng dụng
    """
    
    def __init__(self, config_manager):
        super().__init__()
        
        self.config_manager = config_manager
        
        # Thiết lập window properties
        self.setWindowTitle("Rating Comic System")
        self.setGeometry(100, 100, 1200, 800)
        
        # Khởi tạo các thành phần
        self.init_components()
        
        # Thiết lập UI
        self.setup_ui()
        
        # Thiết lập logging
        self.setup_logging()
        
        logger.info("Khởi tạo MainWindow thành công")
    
    def init_components(self):
        """Khởi tạo các thành phần cần thiết"""
        
        # Khởi tạo database manager
        db_folder = self.config_manager.get_database_folder()
        self.db_manager = MultipleDBManager(db_folder)
        
        # Khởi tạo CrawlerFactory
        CrawlerFactory.initialize(self.config_manager)
        
        logger.info("Đã khởi tạo các thành phần cơ bản")
    
    def setup_ui(self):
        """Thiết lập giao diện người dùng"""
        
        # Widget chính
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        
        # Layout chính
        main_layout = QVBoxLayout(main_widget)
        
        # Splitter để điều chỉnh kích thước giữa tabs và log
        splitter = QSplitter(Qt.Orientation.Vertical)
        
        # Tab container
        self.tabs = QTabWidget()
        
        # Thêm các tab
        self.website_tab = WebsiteTab(self.db_manager, self.config_manager)
        self.analysis_tab = DetailAnalysisTab(self.db_manager, CrawlerFactory, None, self.config_manager)
        self.settings_tab = SettingsTab(self.config_manager)
        
        self.tabs.addTab(self.website_tab, "Thu thập dữ liệu")
        self.tabs.addTab(self.analysis_tab, "Phân tích đánh giá")
        self.tabs.addTab(self.settings_tab, "Cài đặt")
        
        # Kết nối signals
        self.website_tab.selection_updated.connect(self.update_selection)
        self.settings_tab.settings_saved.connect(self.on_settings_saved)
        
        # Log viewer
        log_container = QWidget()
        log_layout = QVBoxLayout(log_container)
        
        log_header = QHBoxLayout()
        log_label = QLabel("Nhật ký:")
        clear_button = QPushButton("Xóa")
        clear_button.clicked.connect(self.clear_log)
        log_header.addWidget(log_label)
        log_header.addStretch()
        log_header.addWidget(clear_button)
        
        self.log_widget = QTextEdit()
        self.log_widget.setReadOnly(True)
        
        log_layout.addLayout(log_header)
        log_layout.addWidget(self.log_widget)
        
        # Thêm vào splitter
        splitter.addWidget(self.tabs)
        splitter.addWidget(log_container)
        
        # Thiết lập kích thước ban đầu
        splitter.setSizes([600, 200])
        
        # Thêm vào layout chính
        main_layout.addWidget(splitter)
        
        # Status bar
        self.statusBar().showMessage("Sẵn sàng")
    
    def setup_logging(self):
        """Thiết lập logging"""
        
        # Tạo handler để chuyển log messages đến QTextEdit
        text_handler = LogHandler(self.log_widget)
        text_handler.setLevel(logging.INFO)
        
        # Thêm handler vào root logger
        logging.getLogger().addHandler(text_handler)
        
        logger.info("Đã thiết lập logging cho UI")
    
    def update_selection(self, selected_comics):
        """
        Cập nhật danh sách truyện đã chọn
        
        Args:
            selected_comics: Danh sách truyện đã chọn
        """
        self.analysis_tab.set_selected_comics(selected_comics)
        logger.info(f"Đã cập nhật {len(selected_comics)} truyện đã chọn")
    
    def on_settings_saved(self):
        """Xử lý khi cài đặt được lưu"""
        QMessageBox.information(self, "Thông báo", "Cài đặt đã được lưu. Một số thay đổi sẽ có hiệu lực sau khi khởi động lại ứng dụng.")
    
    def clear_log(self):
        """Xóa nội dung log widget"""
        self.log_widget.clear()
    
    def closeEvent(self, event):
        """
        Xử lý khi đóng cửa sổ
        
        Args:
            event: QCloseEvent
        """
        # Xác nhận trước khi đóng
        reply = QMessageBox.question(
            self, 'Xác nhận thoát',
            'Bạn có chắc chắn muốn thoát không?',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Dọn dẹp tài nguyên
            logger.info("Đóng ứng dụng...")
            event.accept()
        else:
            event.ignore()