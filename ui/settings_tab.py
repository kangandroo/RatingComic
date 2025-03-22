from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QLineEdit, QPushButton, QSpinBox, QFileDialog,
                             QGroupBox, QFormLayout, QCheckBox, QMessageBox, QComboBox)
from PyQt6.QtCore import pyqtSignal
import logging
import os

logger = logging.getLogger(__name__)

class SettingsTab(QWidget):
    """
    Tab cài đặt cho ứng dụng
    """
    
    # Signal để thông báo cài đặt đã được lưu
    settings_saved = pyqtSignal()
    
    def __init__(self, config_manager):
        super().__init__()
        
        self.config_manager = config_manager
        
        # Thiết lập UI
        self.init_ui()
        
        # Load cài đặt hiện tại
        self.load_settings()
        
        logger.info("Khởi tạo SettingsTab thành công")
    
    def init_ui(self):
        """Thiết lập giao diện người dùng"""
        
        # Layout chính
        main_layout = QVBoxLayout(self)
        
        # Group box cho cài đặt chung
        general_group = QGroupBox("Cài đặt chung")
        general_layout = QFormLayout(general_group)
        
        # ChromeDriver path
        self.chromedriver_path_edit = QLineEdit()
        
        browse_button = QPushButton("Tìm")
        browse_button.clicked.connect(self.browse_chrome_driver)
        
        chromedriver_layout = QHBoxLayout()
        chromedriver_layout.addWidget(self.chromedriver_path_edit)
        chromedriver_layout.addWidget(browse_button)
        
        general_layout.addRow("ChromeDriver Path:", chromedriver_layout)
        
        # Số trang tối đa
        self.max_pages_spin = QSpinBox()
        self.max_pages_spin.setRange(1, 100)
        general_layout.addRow("Số trang mặc định:", self.max_pages_spin)
        
        # Số worker
        self.worker_count_spin = QSpinBox()
        self.worker_count_spin.setRange(1, 20)
        general_layout.addRow("Số worker mặc định:", self.worker_count_spin)
        
        # Group box cho cài đặt websitee
        websites_group = QGroupBox("URL Website")
        websites_layout = QFormLayout(websites_group)
        
        # URL TruyenQQ
        self.truyenqq_url_edit = QLineEdit()
        websites_layout.addRow("TruyenQQ:", self.truyenqq_url_edit)
        
        # URL NetTruyen
        self.nettruyen_url_edit = QLineEdit()
        websites_layout.addRow("NetTruyen:", self.nettruyen_url_edit)
        
        # URL Manhuavn
        self.manhuavn_url_edit = QLineEdit()
        websites_layout.addRow("Manhuavn:", self.manhuavn_url_edit)
        
        # Group box cho cài đặt phân tích sentiment
        sentiment_group = QGroupBox("Phân tích sentiment")
        sentiment_layout = QFormLayout(sentiment_group)
        
        # Sử dụng transformer
        self.use_transformer_checkbox = QCheckBox("Sử dụng mô hình transformer")
        sentiment_layout.addRow(self.use_transformer_checkbox)
        
        # Model name
        self.model_name_edit = QLineEdit()
        sentiment_layout.addRow("Tên mô hình:", self.model_name_edit)
        
        # Cache directory
        self.cache_dir_edit = QLineEdit()
        
        cache_browse_button = QPushButton("Tìm")
        cache_browse_button.clicked.connect(self.browse_cache_dir)
        
        cache_layout = QHBoxLayout()
        cache_layout.addWidget(self.cache_dir_edit)
        cache_layout.addWidget(cache_browse_button)
        
        sentiment_layout.addRow("Thư mục cache:", cache_layout)
        
        # Nút save
        self.save_button = QPushButton("Lưu cài đặt")
        self.save_button.clicked.connect(self.save_settings)
        
        # Thêm vào layout chính
        main_layout.addWidget(general_group)
        main_layout.addWidget(websites_group)
        main_layout.addWidget(sentiment_group)
        main_layout.addStretch()
        main_layout.addWidget(self.save_button)
    
    def load_settings(self):
        """Load cài đặt từ file config"""
        
        # Cài đặt chung
        self.chromedriver_path_edit.setText(self.config_manager.get("chrome_driver_path", ""))
        self.max_pages_spin.setValue(self.config_manager.get("max_pages", 10))
        self.worker_count_spin.setValue(self.config_manager.get("worker_count", 5))
        
        # URL websites
        websites = self.config_manager.get_supported_websites()
        self.truyenqq_url_edit.setText(websites.get("TruyenQQ", "https://truyenqqto.com"))
        self.nettruyen_url_edit.setText(websites.get("NetTruyen", "https://nettruyenvie.com"))
        self.manhuavn_url_edit.setText(websites.get("Manhuavn", "https://manhuavn.top"))
        
        # Cài đặt sentiment
        sentiment_settings = self.config_manager.get("sentiment_analysis", {})
        self.use_transformer_checkbox.setChecked(sentiment_settings.get("use_transformer", True))
        self.model_name_edit.setText(sentiment_settings.get("model_name", "cardiffnlp/twitter-xlm-roberta-base-sentimen"))
        self.cache_dir_edit.setText(sentiment_settings.get("cache_dir", "models"))
        
        logger.info("Đã load cài đặt từ file config")
    
    def save_settings(self):
        """Lưu cài đặt vào file config"""
        
        # Cài đặt chung
        self.config_manager.set("chrome_driver_path", self.chromedriver_path_edit.text())
        self.config_manager.set("max_pages", self.max_pages_spin.value())
        self.config_manager.set("worker_count", self.worker_count_spin.value())
        
        # URL websites
        websites = {
            "TruyenQQ": self.truyenqq_url_edit.text(),
            "NetTruyen": self.nettruyen_url_edit.text(),
            "Manhuavn": self.manhuavn_url_edit.text()
        }
        self.config_manager.set("supported_websites", websites)
        
        # Cài đặt sentiment
        sentiment_settings = {
            "use_transformer": self.use_transformer_checkbox.isChecked(),
            "model_name": self.model_name_edit.text(),
            "cache_dir": self.cache_dir_edit.text()
        }
        self.config_manager.set("sentiment_analysis", sentiment_settings)
        
        # Lưu vào file
        if self.config_manager.save_config():
            logger.info("Đã lưu cài đặt vào file config")
            self.settings_saved.emit()
            QMessageBox.information(self, "Thông báo", "Đã lưu cài đặt thành công")
        else:
            logger.error("Lỗi khi lưu cài đặt vào file config")
            QMessageBox.critical(self, "Lỗi", "Lỗi khi lưu cài đặt")
    
    def browse_chrome_driver(self):
        """Mở dialog chọn file ChromeDriver"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Chọn ChromeDriver", "", "ChromeDriver (chromedriver.exe);; All Files (*)"
        )
        if file_path:
            self.chromedriver_path_edit.setText(file_path)
    
    def browse_cache_dir(self):
        """Mở dialog chọn thư mục cache"""
        dir_path = QFileDialog.getExistingDirectory(
            self, "Chọn thư mục cache", ""
        )
        if dir_path:
            self.cache_dir_edit.setText(dir_path)