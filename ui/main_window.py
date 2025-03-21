from PyQt6.QtWidgets import QMainWindow, QTabWidget, QVBoxLayout, QWidget
from PyQt6.QtGui import QIcon
from PyQt6.QtCore import QSize
import logging

from ui.website_tab import WebsiteSelectionTab
from ui.comic_list_tab import ComicListTab
from ui.analysis_tab import DetailAnalysisTab
from ui.log_widget import LogWidget
from database.db_manager import DatabaseManager

logger = logging.getLogger(__name__)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        
        # Thiết lập cửa sổ chính
        self.setWindowTitle("Phân tích truyện tranh")
        self.setMinimumSize(1000, 700)
        
        # Lấy database manager
        self.db_manager = DatabaseManager('data/comics.db')
        
        # Khởi tạo widget central
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        
        # Thiết lập layout
        self.layout = QVBoxLayout(self.central_widget)
        
        # Khởi tạo TabWidget
        self.tabs = QTabWidget()
        
        # Khởi tạo LogWidget
        self.log_widget = LogWidget(max_lines=1000)
        self.log_widget.setMaximumHeight(150)
        
        # Khởi tạo các tab
        self.website_tab = WebsiteSelectionTab(self.db_manager, self.log_widget)
        self.comic_list_tab = ComicListTab(self.db_manager, self.log_widget)
        self.analysis_tab = DetailAnalysisTab(self.db_manager, self.log_widget)
        
        # Thêm các tab vào TabWidget
        self.tabs.addTab(self.website_tab, "Chọn Website")
        self.tabs.addTab(self.comic_list_tab, "Danh sách truyện")
        self.tabs.addTab(self.analysis_tab, "Phân tích chi tiết")
        
        # Kết nối các signal
        self.website_tab.crawl_finished.connect(self.on_basic_crawl_finished)
        self.comic_list_tab.analysis_started.connect(self.on_analysis_started)
        
        # Thêm TabWidget và LogWidget vào layout
        self.layout.addWidget(self.tabs)
        self.layout.addWidget(self.log_widget)
        
        # Hiển thị thông tin khởi động
        logger.info("Ứng dụng phân tích truyện tranh đã sẵn sàng")
    
    def on_basic_crawl_finished(self):
        """Xử lý khi quá trình crawl dữ liệu cơ bản hoàn tất"""
        self.tabs.setCurrentIndex(1)  # Chuyển sang tab danh sách truyện
        self.comic_list_tab.load_comics()  # Tải lại danh sách truyện
        
    def on_analysis_started(self, selected_comics):
        """Xử lý khi bắt đầu phân tích chi tiết"""
        self.tabs.setCurrentIndex(2)  # Chuyển sang tab phân tích chi tiết
        self.analysis_tab.set_selected_comics(selected_comics)