from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                            QComboBox, QPushButton, QProgressBar, QTableWidget, 
                            QTableWidgetItem, QCheckBox, QHeaderView, QMessageBox,
                            QSpinBox, QGroupBox)
from PyQt6.QtCore import Qt, QThreadPool, pyqtSignal, pyqtSlot
import logging
import time
from analysis.rating_factory import RatingFactory

from utils.worker import Worker
from crawlers.crawler_factory import CrawlerFactory

logger = logging.getLogger(__name__)

class WebsiteTab(QWidget):
    """
    Tab để thu thập dữ liệu từ các trang web
    """
    
    # Signal để thông báo danh sách truyện đã chọn thay đổi
    selection_updated = pyqtSignal(list)
    
    def __init__(self, db_manager, config_manager):
        super().__init__()
        
        self.db_manager = db_manager
        self.config_manager = config_manager
        self.is_crawling = False
        self.all_comics = []
        self.checked_comics = []
        
        # Thiết lập UI
        self.init_ui()
        
        # Load dữ liệu ban đầu
        self.load_initial_data()
        
        logger.info("Khởi tạo WebsiteTab thành công")
    
    def init_ui(self):
        """Thiết lập giao diện người dùng"""
        
        # Layout chính
        main_layout = QVBoxLayout(self)
        
        # Header section
        header_layout = QHBoxLayout()
        
        # Website selection
        website_layout = QVBoxLayout()
        website_label = QLabel("Chọn website:")
        self.website_combo = QComboBox()
        self.website_combo.addItems(["TruyenQQ", "NetTruyen", "Manhuavn"])
        self.website_combo.currentTextChanged.connect(self.on_website_changed)
        
        website_layout.addWidget(website_label)
        website_layout.addWidget(self.website_combo)
        
        # Crawl options
        options_group = QGroupBox("Tùy chọn:")
        options_layout = QHBoxLayout(options_group)
        
        # Pages option
        pages_layout = QVBoxLayout()
        pages_label = QLabel("Số trang:")
        self.pages_spin = QSpinBox()
        self.pages_spin.setRange(1, 1000000)
        self.pages_spin.setValue(self.config_manager.get("max_pages", 10))
        
        pages_layout.addWidget(pages_label)
        pages_layout.addWidget(self.pages_spin)
        
        # Workers option
        workers_layout = QVBoxLayout()
        workers_label = QLabel("Số worker:")
        self.workers_spin = QSpinBox()
        self.workers_spin.setRange(1, 100)
        self.workers_spin.setValue(self.config_manager.get("worker_count", 5))
        
        workers_layout.addWidget(workers_label)
        workers_layout.addWidget(self.workers_spin)
        
        # Add options to options layout
        options_layout.addLayout(pages_layout)
        options_layout.addLayout(workers_layout)
        
        # Crawl button
        self.crawl_button = QPushButton("Bắt đầu crawl")
        self.crawl_button.clicked.connect(self.start_crawling)
        
        # Add all to header layout
        header_layout.addLayout(website_layout)
        header_layout.addWidget(options_group)
        header_layout.addStretch()
        header_layout.addWidget(self.crawl_button)
        
        # Progress section
        progress_layout = QHBoxLayout()
        progress_label = QLabel("Tiến độ:")
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        
        progress_layout.addWidget(progress_label)
        progress_layout.addWidget(self.progress_bar)
        
        # Results section
        results_layout = QVBoxLayout()
        
        # Results filter
        # Thêm vào phần filter_layout trong init_ui()

        filter_layout = QHBoxLayout()

        filter_label = QLabel("Kết quả:")
        self.select_all_checkbox = QCheckBox("Chọn tất cả")
        self.select_all_checkbox.stateChanged.connect(self.on_select_all_changed)

        # Thêm combobox để chọn trường sắp xếp
        sort_label = QLabel("Sắp xếp theo:")
        self.sort_field_combo = QComboBox()
        self.sort_field_combo.addItems([
            "Tên truyện", "Số chương", "Lượt xem", 
            "Lượt thích", "Lượt theo dõi", "Rating", "Lượt đánh giá", "Điểm cơ bản"
        ])

        # Thêm combobox để chọn thứ tự sắp xếp
        self.sort_order_combo = QComboBox()
        self.sort_order_combo.addItems(["Cao đến thấp", "Thấp đến cao"])

        # Nút áp dụng sắp xếp
        self.apply_sort_button = QPushButton("Áp dụng")
        self.apply_sort_button.clicked.connect(self.apply_sorting)

        self.select_for_analysis_button = QPushButton("Chọn để phân tích")
        self.select_for_analysis_button.clicked.connect(self.select_for_analysis)

        filter_layout.addWidget(filter_label)
        filter_layout.addStretch()
        filter_layout.addWidget(sort_label)
        filter_layout.addWidget(self.sort_field_combo)
        filter_layout.addWidget(self.sort_order_combo) 
        filter_layout.addWidget(self.apply_sort_button)
        filter_layout.addWidget(self.select_all_checkbox)
        filter_layout.addWidget(self.select_for_analysis_button)

        # Results table
        self.results_table = QTableWidget()
        self.results_table.setColumnCount(11)
        self.results_table.setHorizontalHeaderLabels([
            "Chọn", "Tên truyện", "Mô tả", "Số chương", 
            "Lượt xem", "Lượt thích", "Lượt theo dõi", 
            "Rating", "Lượt đánh giá", "Điểm cơ bản", "Trạng thái"
        ])

        # Điều chỉnh header
        header = self.results_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)  # Chọn
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)  # Tên truyện
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)  # Mô tả
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)  # Số chương
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)  # Lượt xem
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)  # Lượt thích
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)  # Lượt theo dõi
        header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)  # Rating
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.ResizeToContents)  # Lượt đánh giá
        header.setSectionResizeMode(9, QHeaderView.ResizeMode.ResizeToContents)  # Điểm cơ bản
        header.setSectionResizeMode(10, QHeaderView.ResizeMode.ResizeToContents)  # Trạng thái

        # Cho phép sắp xếp
        self.results_table.setSortingEnabled(True)
        
        results_layout.addLayout(filter_layout)
        results_layout.addWidget(self.results_table)
        
        # Add all sections to main layout
        main_layout.addLayout(header_layout)
        main_layout.addLayout(progress_layout)
        main_layout.addLayout(results_layout)
        
        # Enable drag and drop
        self.setAcceptDrops(True)
    
    def apply_sorting(self):
        """Áp dụng sắp xếp theo trường đã chọn"""
        field_index = self.sort_field_combo.currentIndex()
        
        # Chuyển đổi index combo sang index cột thực tế (bỏ qua cột checkbox)
        if field_index == 0:  # Tên truyện
            column = 1
        elif field_index == 1:  # Số chương
            column = 3
        elif field_index == 2:  # Lượt xem
            column = 4
        elif field_index == 3:  # Lượt thích
            column = 5
        elif field_index == 4:  # Lượt theo dõi
            column = 6
        elif field_index == 5:  # Rating
            column = 7
        elif field_index == 6:  # Lượt đánh giá
            column = 8
        elif field_index == 7:  # Điểm cơ bản
            column = 9
        else:
            column = 1
        
        # Xác định thứ tự sắp xếp
        order = Qt.SortOrder.DescendingOrder if self.sort_order_combo.currentIndex() == 0 else Qt.SortOrder.AscendingOrder
        
        # Áp dụng sắp xếp
        self.results_table.sortByColumn(column, order)
    
    def load_initial_data(self):
        """Load dữ liệu ban đầu"""
        
        # Lấy website hiện tại
        website = self.website_combo.currentText()
        
        # Đặt nguồn dữ liệu
        self.db_manager.set_source(website)
        
        # Lấy danh sách truyện
        self.all_comics = self.db_manager.get_all_comics()
        
        # Hiển thị danh sách truyện
        self.populate_results_table()
        
        logger.info(f"Đã load {len(self.all_comics)} truyện từ nguồn {website}")
    
    def populate_results_table(self):
        """Hiển thị danh sách truyện trong bảng kết quả"""
        
        # Xóa dữ liệu cũ
        self.results_table.setRowCount(0)
        
        # Tạm thời tắt sorting
        self.results_table.setSortingEnabled(False)
        
        # Thêm dữ liệu mới
        for comic in self.all_comics:
            row = self.results_table.rowCount()
            self.results_table.insertRow(row)
            
            # Checkbox
            checkbox = QTableWidgetItem()
            checkbox.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled)
            checkbox.setCheckState(Qt.CheckState.Unchecked)
            self.results_table.setItem(row, 0, checkbox)
            
            # Thông tin chi tiết
            self.results_table.setItem(row, 1, QTableWidgetItem(comic.get("ten_truyen", "")))
            
            # Mô tả - hiện chỉ một phần nhỏ
            mo_ta_full = comic.get("mo_ta", "")
            mo_ta = mo_ta_full[:100] + "..." if len(mo_ta_full) > 100 else mo_ta_full
            self.results_table.setItem(row, 2, QTableWidgetItem(mo_ta))
            
            # Thông tin số liệu
            self.results_table.setItem(row, 3, QTableWidgetItem(str(comic.get("so_chuong", 0))))
            
            # Khởi tạo QTableWidgetItem cho các giá trị số để đảm bảo sắp xếp đúng
            luot_xem_item = QTableWidgetItem()
            luot_xem_item.setData(Qt.ItemDataRole.DisplayRole, int(comic.get("luot_xem", 0)))
            self.results_table.setItem(row, 4, luot_xem_item)
            
            # Xử lý tùy theo nguồn dữ liệu
            if comic.get("nguon") == "TruyenQQ":
                luot_thich_item = QTableWidgetItem()
                luot_thich_item.setData(Qt.ItemDataRole.DisplayRole, int(comic.get("luot_thich", 0)))
                self.results_table.setItem(row, 5, luot_thich_item)
                
                luot_theo_doi_item = QTableWidgetItem()
                luot_theo_doi_item.setData(Qt.ItemDataRole.DisplayRole, int(comic.get("luot_theo_doi", 0)))
                self.results_table.setItem(row, 6, luot_theo_doi_item)
                
                self.results_table.setItem(row, 7, QTableWidgetItem("N/A"))  # Rating không có
                self.results_table.setItem(row, 8, QTableWidgetItem("N/A"))  # Lượt đánh giá không có
                
            elif comic.get("nguon") == "NetTruyen":
                luot_thich_item = QTableWidgetItem()
                luot_thich_item.setData(Qt.ItemDataRole.DisplayRole, int(comic.get("luot_thich", 0)))
                self.results_table.setItem(row, 5, luot_thich_item)
                
                luot_theo_doi_item = QTableWidgetItem()
                luot_theo_doi_item.setData(Qt.ItemDataRole.DisplayRole, int(comic.get("luot_theo_doi", 0)))
                self.results_table.setItem(row, 6, luot_theo_doi_item)
                
                self.results_table.setItem(row, 7, QTableWidgetItem(comic.get("rating", "")))
                
                luot_danh_gia_item = QTableWidgetItem()
                luot_danh_gia_item.setData(Qt.ItemDataRole.DisplayRole, int(comic.get("luot_danh_gia", 0)))
                self.results_table.setItem(row, 8, luot_danh_gia_item)
                
            elif comic.get("nguon") == "Manhuavn":
                self.results_table.setItem(row, 5, QTableWidgetItem("N/A"))  # Lượt thích không có
                
                luot_theo_doi_item = QTableWidgetItem()
                luot_theo_doi_item.setData(Qt.ItemDataRole.DisplayRole, int(comic.get("luot_theo_doi", 0)))
                self.results_table.setItem(row, 6, luot_theo_doi_item)
                
                self.results_table.setItem(row, 7, QTableWidgetItem(comic.get("danh_gia", "")))
                
                luot_danh_gia_item = QTableWidgetItem()
                luot_danh_gia_item.setData(Qt.ItemDataRole.DisplayRole, int(comic.get("luot_danh_gia", 0)))
                self.results_table.setItem(row, 8, luot_danh_gia_item)
            
            nguon = comic.get("nguon", "TruyenQQ")
            rating_calculator = RatingFactory.get_calculator(nguon)
            base_rating = rating_calculator.calculate(comic)
            
            base_rating_item = QTableWidgetItem()
            base_rating_item.setData(Qt.ItemDataRole.DisplayRole, float(base_rating))
            self.results_table.setItem(row, 9, base_rating_item)
        
            self.results_table.setItem(row, 10, QTableWidgetItem(comic.get("trang_thai", "")))
        
        # Bật lại tính năng sorting
        self.results_table.setSortingEnabled(True)
        
    def on_website_changed(self, website):
        """
        Xử lý khi website được chọn thay đổi
        
        Args:
            website: Tên website mới
        """
        # Nếu đang crawl, không cho phép thay đổi
        if self.is_crawling:
            QMessageBox.warning(self, "Cảnh báo", "Không thể thay đổi website khi đang crawl!")
            return
        
        # Đặt nguồn dữ liệu
        self.db_manager.set_source(website)
        
        # Lấy danh sách truyện
        self.all_comics = self.db_manager.get_all_comics()
        
        # Hiển thị danh sách truyện
        self.populate_results_table()
        
        logger.info(f"Đã chuyển sang nguồn: {website}, {len(self.all_comics)} truyện")
    
    def on_select_all_changed(self, state):
        """
        Xử lý khi checkbox chọn tất cả thay đổi
        
        Args:
            state: Trạng thái mới của checkbox
        """
        # Đặt trạng thái cho tất cả checkbox
        check_state = Qt.CheckState.Checked if state == Qt.CheckState.Checked.value else Qt.CheckState.Unchecked
        
        for row in range(self.results_table.rowCount()):
            self.results_table.item(row, 0).setCheckState(check_state)
    
    def start_crawling(self):
        """Bắt đầu quá trình crawl"""
        
        # Nếu đang crawl, dừng lại
        if self.is_crawling:
            QMessageBox.information(self, "Thông báo", "Đang trong quá trình crawl. Vui lòng đợi!")
            return
        
        # Lấy website hiện tại
        website = self.website_combo.currentText()
        
        # Lấy số trang và số worker
        max_pages = self.pages_spin.value()
        worker_count = self.workers_spin.value()
        
        # Hiển thị thông báo xác nhận
        reply = QMessageBox.question(
            self, 'Xác nhận',
            f'Bạn có chắc chắn muốn bắt đầu crawl từ {website} với {max_pages} trang không?',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.Yes
        )
        
        if reply == QMessageBox.StandardButton.No:
            return
        
        # Đặt nguồn dữ liệu
        self.db_manager.set_source(website)
        
        # Cập nhật trạng thái
        self.is_crawling = True
        self.crawl_button.setText("Đang crawl...")
        self.crawl_button.setEnabled(False)
        self.progress_bar.setValue(0)
        
        # Tạo crawler
        crawler = CrawlerFactory.create_crawler(
            website, 
            self.db_manager, 
            self.config_manager,
            max_pages=max_pages,
            worker_count=worker_count
        )
        
        # Tạo worker để chạy trong thread riêng
        worker = Worker(crawler.crawl_basic_data)
        worker.signals.progress.connect(self.update_progress)
        worker.signals.result.connect(self.on_crawl_complete)
        worker.signals.error.connect(self.on_crawl_error)
        
        # Bắt đầu crawl
        logger.info(f"Bắt đầu crawl từ {website} với {max_pages} trang và {worker_count} worker")
        QThreadPool.globalInstance().start(worker)
    
    @pyqtSlot(int)
    def update_progress(self, progress):
        """
        Cập nhật thanh tiến trình
        
        Args:
            progress: Giá trị tiến trình (0-100)
        """
        self.progress_bar.setValue(progress)
    
    @pyqtSlot(object)
    def on_crawl_complete(self, result):
        """
        Xử lý khi crawl hoàn tất
        
        Args:
            result: Kết quả crawl
        """
        # Cập nhật trạng thái
        self.is_crawling = False
        self.crawl_button.setText("Bắt đầu crawl")
        self.crawl_button.setEnabled(True)
        self.progress_bar.setValue(100)
        
        # Lấy danh sách truyện
        self.all_comics = self.db_manager.get_all_comics()
        
        # Hiển thị danh sách truyện
        self.populate_results_table()
        
        # Hiển thị thông báo
        QMessageBox.information(
            self, "Thông báo", 
            f"Đã crawl xong {result.get('count', 0)} truyện từ {result.get('website', '')} trong {result.get('time_taken', 0):.2f} giây"
        )
        
        logger.info(f"Đã crawl xong {result.get('count', 0)} truyện từ {result.get('website', '')}")
    
    @pyqtSlot(str)
    def on_crawl_error(self, error):
        """
        Xử lý khi có lỗi trong quá trình crawl
        
        Args:
            error: Thông báo lỗi
        """
        # Cập nhật trạng thái
        self.is_crawling = False
        self.crawl_button.setText("Bắt đầu crawl")
        self.crawl_button.setEnabled(True)
        
        # Hiển thị thông báo lỗi
        QMessageBox.critical(self, "Lỗi", f"Lỗi khi crawl: {error}")
        
        logger.error(f"Lỗi khi crawl: {error}")
    
    def select_for_analysis(self):
        """Chọn truyện để phân tích"""
        
        # Thu thập truyện đã chọn
        self.checked_comics = []
        
        for row in range(self.results_table.rowCount()):
            if self.results_table.item(row, 0).checkState() == Qt.CheckState.Checked:
                # Lấy ID của truyện
                comic_id = self.all_comics[row]["id"]
                
                # Lấy thông tin đầy đủ của truyện
                comic = self.db_manager.get_comic_by_id(comic_id)
                
                if comic:
                    self.checked_comics.append(comic)
        
        # Gửi signal với danh sách truyện đã chọn
        self.selection_updated.emit(self.checked_comics)
        
        # Hiển thị thông báo
        QMessageBox.information(self, "Thông báo", f"Đã chọn {len(self.checked_comics)} truyện để phân tích")
        
        logger.info(f"Đã chọn {len(self.checked_comics)} truyện để phân tích")