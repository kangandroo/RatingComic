from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                            QComboBox, QPushButton, QProgressBar, QSpacerItem,
                            QSizePolicy, QGroupBox, QFormLayout)
from PyQt6.QtCore import pyqtSignal, QThreadPool
import logging

from crawlers.crawler_factory import CrawlerFactory
from utils.worker import Worker

logger = logging.getLogger(__name__)

class WebsiteSelectionTab(QWidget):
    # Signal phát ra khi crawl xong
    crawl_finished = pyqtSignal()
    
    def __init__(self, db_manager, log_widget):
        super().__init__()
        self.db_manager = db_manager
        self.log_widget = log_widget
        
        # Danh sách website hỗ trợ
        self.supported_websites = {
            "TruyenQQ": "https://truyenqqto.com",
            "NetTruyen": "https://www.nettruyenmax.com",
            "TruyenFull": "https://truyenfull.vn"
        }
        
        # Trạng thái crawl
        self.is_crawling = False
        
        # Thiết lập UI
        self.init_ui()
    
    def init_ui(self):
        # Layout chính
        main_layout = QVBoxLayout(self)
        
        # Group box thông tin
        info_group = QGroupBox("Chọn website để crawl dữ liệu")
        info_layout = QFormLayout(info_group)
        
        # Combobox chọn website
        self.website_combo = QComboBox()
        for name in self.supported_websites.keys():
            self.website_combo.addItem(name)
        
        info_layout.addRow("Website:", self.website_combo)
        
        # Hiển thị URL
        self.url_label = QLabel(self.supported_websites["TruyenQQ"])
        info_layout.addRow("URL:", self.url_label)
        
        # Kết nối sự kiện thay đổi website
        self.website_combo.currentIndexChanged.connect(self.on_website_changed)
        
        main_layout.addWidget(info_group)
        
        # Group box cấu hình
        config_group = QGroupBox("Cấu hình crawl")
        config_layout = QFormLayout(config_group)
        
        # Số trang tối đa
        self.max_pages_combo = QComboBox()
        for pages in [10, 20, 50, 100, "Tất cả"]:
            self.max_pages_combo.addItem(str(pages))
        
        config_layout.addRow("Số trang tối đa:", self.max_pages_combo)
        
        main_layout.addWidget(config_group)
        
        # Spacer để đẩy các controls xuống dưới
        main_layout.addSpacerItem(QSpacerItem(20, 40, QSizePolicy.Policy.Minimum, 
                                              QSizePolicy.Policy.Expanding))
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        main_layout.addWidget(self.progress_bar)
        
        # Nút điều khiển
        button_layout = QHBoxLayout()
        
        self.start_button = QPushButton("Bắt đầu crawl")
        self.start_button.clicked.connect(self.start_crawl)
        
        self.clear_button = QPushButton("Xóa dữ liệu cũ")
        self.clear_button.clicked.connect(self.clear_data)
        
        button_layout.addWidget(self.clear_button)
        button_layout.addWidget(self.start_button)
        
        main_layout.addLayout(button_layout)
    
    def on_website_changed(self, index):
        website_name = self.website_combo.currentText()
        self.url_label.setText(self.supported_websites[website_name])
    
    def start_crawl(self):
        if self.is_crawling:
            logger.warning("Đang trong quá trình crawl, vui lòng đợi")
            return
        
        # Cập nhật UI
        self.is_crawling = True
        self.start_button.setEnabled(False)
        self.clear_button.setEnabled(False)
        self.progress_bar.setValue(0)
        
        # Lấy thông tin cấu hình
        website_name = self.website_combo.currentText()
        website_url = self.supported_websites[website_name]
        
        max_pages = self.max_pages_combo.currentText()
        if max_pages == "Tất cả":
            max_pages = None
        else:
            max_pages = int(max_pages)
        
        logger.info(f"Bắt đầu crawl từ {website_name} ({website_url})")
        
        # Tạo crawler thông qua factory
        crawler = CrawlerFactory.create_crawler(
            website_name, 
            self.db_manager,
            base_url=website_url,
            max_pages=max_pages
        )
        
        # Tạo worker để chạy trong thread riêng
        worker = Worker(crawler.crawl_basic_data)
        worker.signals.progress.connect(self.update_progress)
        worker.signals.result.connect(self.on_crawl_finished)
        worker.signals.error.connect(self.on_crawl_error)
        
        # Chạy worker
        QThreadPool.globalInstance().start(worker)
    
    def update_progress(self, progress):
        """Cập nhật thanh tiến trình"""
        self.progress_bar.setValue(int(progress))
    
    def on_crawl_finished(self, result):
        """Xử lý khi crawl hoàn tất"""
        self.is_crawling = False
        self.start_button.setEnabled(True)
        self.clear_button.setEnabled(True)
        
        logger.info(f"Đã crawl xong {result['count']} truyện từ {result['website']}")
        logger.info(f"Thời gian thực hiện: {result['time_taken']:.2f} giây")
        
        # Thông báo hoàn tất để chuyển tab
        self.crawl_finished.emit()
    
    def on_crawl_error(self, error):
        """Xử lý khi có lỗi xảy ra"""
        self.is_crawling = False
        self.start_button.setEnabled(True)
        self.clear_button.setEnabled(True)
        
        logger.error(f"Lỗi khi crawl: {error}")
    
    def clear_data(self):
        """Xóa dữ liệu cũ trong database"""
        if self.is_crawling:
            logger.warning("Không thể xóa dữ liệu khi đang crawl")
            return
        
        # Xác nhận từ người dùng (có thể thêm dialog)
        logger.info("Đang xóa dữ liệu cũ...")
        
        # Xóa dữ liệu
        self.db_manager.clear_comics_data()
        
        logger.info("Đã xóa dữ liệu cũ")