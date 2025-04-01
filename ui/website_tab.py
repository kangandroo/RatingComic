from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                            QComboBox, QPushButton, QProgressBar, QTableWidget, 
                            QTableWidgetItem, QCheckBox, QHeaderView, QMessageBox,
                            QSpinBox, QGroupBox, QApplication, QFileDialog)
from PyQt6.QtCore import Qt, QThreadPool, pyqtSignal, pyqtSlot, QTimer
from analysis.rating_thread import RatingCalculationThread
import logging
import time
from analysis.rating_factory import RatingFactory
import pandas as pd
import os
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
        
        # Thêm biến cho xử lý batch
        self.rating_completed = False
        self.rating_in_progress = False
        self.batch_size = 50
        self.current_batch = 0
        self.total_batches = 0
        self.batch_timer = QTimer(self)
        self.batch_timer.timeout.connect(self.process_next_batch)
        self.rating_results = {} 
        
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
        
        self.export_excel_button = QPushButton("Xuất Excel")
        self.export_excel_button.clicked.connect(self.export_to_excel)

        filter_layout.addWidget(filter_label)
        filter_layout.addStretch()
        filter_layout.addWidget(sort_label)
        filter_layout.addWidget(self.sort_field_combo)
        filter_layout.addWidget(self.sort_order_combo) 
        filter_layout.addWidget(self.apply_sort_button)
        filter_layout.addWidget(self.select_all_checkbox)
        filter_layout.addWidget(self.select_for_analysis_button)
        filter_layout.addWidget(self.export_excel_button)

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
    
    def export_to_excel(self):
        """Xuất dữ liệu từ bảng kết quả ra file Excel"""
        try:
            # Lấy tên website hiện tại làm tên file mặc định
            current_website = self.website_combo.currentText()
            default_filename = f"{current_website}_comics_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
            
            # Hiển thị hộp thoại chọn nơi lưu file
            file_path, _ = QFileDialog.getSaveFileName(
                self, "Lưu file Excel", 
                default_filename,
                "Excel Files (*.xlsx)"
            )
            
            if not file_path: 
                return
                
            # Nếu người dùng không thêm .xlsx vào tên file, thêm đuôi
            if not file_path.endswith('.xlsx'):
                file_path += '.xlsx'
            
            # Kiểm tra xem có dữ liệu để xuất không
            row_count = self.results_table.rowCount()
            if row_count == 0:
                QMessageBox.warning(self, "Cảnh báo", "Không có dữ liệu để xuất!")
                return
            
            # Hiển thị dialog tiến trình
            # progress_dialog = QMessageBox(self)
            # progress_dialog.setWindowTitle("Đang xuất dữ liệu")
            # progress_dialog.setText("Đang chuẩn bị dữ liệu để xuất ra Excel...")
            # progress_dialog.setStandardButtons(QMessageBox.StandardButton.NoButton)
            # progress_dialog.show()
            # QApplication.processEvents()
            
            # Thu thập dữ liệu từ bảng
            data = []
            headers = []
            
            # Lấy tên các cột (bỏ qua cột checkbox)
            for col in range(1, self.results_table.columnCount()):
                headers.append(self.results_table.horizontalHeaderItem(col).text())
            
            # Thu thập dữ liệu từng dòng
            for row in range(row_count):
                row_data = []
                for col in range(1, self.results_table.columnCount()):
                    item = self.results_table.item(row, col)
                    if item is not None:
                        row_data.append(item.text())
                    else:
                        row_data.append("")
                data.append(row_data)
            
            # Tạo DataFrame từ dữ liệu
            df = pd.DataFrame(data, columns=headers)
            
            # Cập nhật thông báo
            # progress_dialog.setText("Đang ghi dữ liệu vào file Excel...")
            QApplication.processEvents()
            
            # Xuất DataFrame ra file Excel
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=current_website, index=False)
                
                # Điều chỉnh độ rộng cột
                worksheet = writer.sheets[current_website]
                for i, col in enumerate(df.columns):
                    # Xác định độ rộng dựa trên giá trị dài nhất
                    max_length = max(df[col].astype(str).map(len).max(), len(col)) + 2
                    worksheet.column_dimensions[chr(65 + i)].width = min(max_length, 50) 
            
            # Đóng dialog tiến trình
            # progress_dialog.close()
            
            # Hiển thị thông báo thành công
            QMessageBox.information(
                self, 
                "Xuất dữ liệu thành công", 
                f"Đã xuất {row_count} dòng dữ liệu ra file:\n{file_path}"
            )
            
            # Mở file hoặc thư mục chứa file
            try:
                if os.name == 'nt':  # Windows
                    os.startfile(os.path.dirname(file_path))
                elif os.name == 'posix':  # macOS và Linux
                    import subprocess
                    subprocess.call(['open', os.path.dirname(file_path)])
            except Exception as e:
                logger.warning(f"Không thể mở thư mục chứa file: {e}")
            
            logger.info(f"Đã xuất {row_count} dòng dữ liệu ra file: {file_path}")
            
        except Exception as e:
            logger.error(f"Lỗi khi xuất dữ liệu ra Excel: {e}")
            QMessageBox.critical(self, "Lỗi", f"Không thể xuất file Excel: {str(e)}")

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
    
    def load_initial_data(self):
        """Load dữ liệu ban đầu"""
        
        # Lấy website hiện tại
        website = self.website_combo.currentText()
        
        # Đặt nguồn dữ liệu
        self.db_manager.set_source(website)
        
        # Lấy danh sách truyện
        self.all_comics = self.db_manager.get_all_comics()
        
        # Hiển thị danh sách truyện
        self.populate_results_table(caculate_rating=False)
        
        logger.info(f"Đã load {len(self.all_comics)} truyện từ nguồn {website}")
    
    def populate_results_table(self, caculate_rating=True):
        """Hiển thị danh sách truyện trong bảng kết quả"""
        # Xóa dữ liệu cũ
        self.results_table.setRowCount(0)
        
        if not self.all_comics:
            return
        
        # Reset rating results
        self.rating_results = {}
        
        # Hiển thị tất cả dữ liệu cơ bản
        need_calculation = self.display_all_comics()
        
        # Bắt đầu tính rating theo batch chỉ khi cần
        if need_calculation and caculate_rating:
            self.start_batch_processing()
        elif need_calculation:
            logger.info("Bỏ qua rating")
        else:
            logger.info("Tất cả truyện đã có rating từ database, không cần tính toán lại")
    
    def display_all_comics(self, caculate_rating=True):
        """Hiển thị dữ liệu cơ bản cho tất cả truyện"""
        # Tạm thời tắt sorting
        self.results_table.setSortingEnabled(False)
        
        # Thiết lập số dòng cho bảng
        self.results_table.setRowCount(len(self.all_comics))
        
        current_website = self.website_combo.currentText()
        source_comics = []
        
        for comic in self.all_comics:
            if comic.get("nguon") == current_website:
                source_comics.append(comic)
        
        if len(source_comics) != len(self.all_comics):
            logger.warning(f"Phát hiện {len(self.all_comics) - len(source_comics)} truyện không thuộc nguồn {current_website}")
            # Cập nhật lại danh sách truyện nếu cần
            self.all_comics = source_comics
            self.results_table.setRowCount(len(source_comics))
        
        # Thêm dữ liệu mới
        ratings_to_calculate = []
        
        for row, comic in enumerate(self.all_comics):
            # Checkbox với ID truyện
            checkbox = QTableWidgetItem()
            checkbox.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled)
            checkbox.setCheckState(Qt.CheckState.Unchecked)
            checkbox.setData(Qt.ItemDataRole.UserRole, comic.get("id"))  # Lưu ID vào UserRole
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
            
            # Hiển thị rating từ database nếu có
            if "base_rating" in comic and comic["base_rating"] is not None:
                base_rating_item = QTableWidgetItem()
                base_rating_item.setData(Qt.ItemDataRole.DisplayRole, float(comic["base_rating"]))
                self.results_table.setItem(row, 9, base_rating_item)
                
                # Lưu vào cache
                self.rating_results[row] = comic["base_rating"]
            else:
                self.results_table.setItem(row, 9, QTableWidgetItem("Đang chờ..."))
                ratings_to_calculate.append(row)
                
            self.results_table.setItem(row, 10, QTableWidgetItem(comic.get("trang_thai", "")))
        
        # Bật lại tính năng sorting
        self.results_table.setSortingEnabled(True)
        
        # Trả về True nếu cần tính toán rating, False nếu tất cả đã có rating
        return len(ratings_to_calculate) > 0
    
    def start_batch_processing(self):
        """Bắt đầu xử lý rating theo batch"""
        self.rating_completed = False
        self.rating_in_progress = True
        
        # Dừng timer nếu đang chạy
        if self.batch_timer.isActive():
            self.batch_timer.stop()
        
        # Tính toán số batch
        self.total_batches = (len(self.all_comics) + self.batch_size - 1) // self.batch_size
        self.current_batch = 0
        
        # Cập nhật progress bar
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(True)
        
        # Bắt đầu xử lý batch đầu tiên
        self.process_next_batch()
        
        logger.info(f"Bắt đầu tính toán rating theo lô: {self.total_batches} lô, mỗi lô {self.batch_size} truyện")
    
    def process_next_batch(self):
        """Xử lý batch tiếp theo"""
        if self.current_batch >= self.total_batches:
            # Đã xử lý xong tất cả batch
            self.on_all_batches_complete()
            return
            
        # Kiểm tra nếu thread đang chạy
        if hasattr(self, 'rating_thread') and self.rating_thread is not None and self.rating_thread.isRunning():
            logger.debug("Thread tính toán đang chạy, chờ hoàn thành...")
            # Lập lịch kiểm tra lại sau 200ms
            QTimer.singleShot(200, self.process_next_batch)
            return
        
        # Tính index bắt đầu và kết thúc của batch hiện tại
        start_idx = self.current_batch * self.batch_size
        end_idx = min(start_idx + self.batch_size, len(self.all_comics))
        
        # Lấy truyện cho batch hiện tại
        current_batch_comics = self.all_comics[start_idx:end_idx]
        
        # Chỉ xử lý những truyện chưa có rating
        comics_to_process = []
        indices_to_process = []
        
        for i, comic in enumerate(current_batch_comics):
            global_idx = start_idx + i
            if "base_rating" not in comic or comic["base_rating"] is None:
                comics_to_process.append(comic)
                indices_to_process.append(i)
                
                # Cập nhật UI để hiển thị "Đang tính..."
                if global_idx < self.results_table.rowCount():
                    self.results_table.setItem(global_idx, 9, QTableWidgetItem("Đang tính..."))
        
        # Cập nhật UI một lần
        QApplication.processEvents()
        
        # Tăng batch hiện tại
        batch_number = self.current_batch
        self.current_batch += 1
        
        # Nếu không có truyện nào cần tính toán trong batch này
        if not comics_to_process:
            # Lên lịch xử lý batch tiếp theo
            QTimer.singleShot(50, self.process_next_batch)
            return
        
        # Xử lý an toàn rating thread cũ
        self.safe_terminate_rating_thread()
        
        # Tạo và chạy thread tính toán rating cho batch hiện tại
        worker_count = min(8, len(comics_to_process))
        self.rating_thread = RatingCalculationThread(comics_to_process, worker_count)
        self.rating_thread.progress_updated.connect(lambda p: self.update_batch_progress(p))
        self.rating_thread.calculation_finished.connect(
            lambda results: self.handle_batch_result(results, start_idx, indices_to_process, batch_number)
        )
        self.rating_thread.start()
        
        logger.info(f"Đang xử lý batch {batch_number + 1}/{self.total_batches} ({len(comics_to_process)} truyện)")
    
    def update_batch_progress(self, progress):
        """Cập nhật tiến trình cho batch hiện tại"""
        # Tính toán tiến trình tổng thể
        if self.total_batches > 0:
            overall_progress = ((self.current_batch - 1) * 100 + progress) // self.total_batches
            self.progress_bar.setValue(min(100, overall_progress))
        
    def handle_batch_result(self, results, start_idx, indices, batch_number):
        """Xử lý kết quả tính toán cho một batch cụ thể"""
        
        # Kiểm tra tính hợp lệ của kết quả
        if not results:
            logger.warning(f"Nhận được kết quả rỗng từ batch {batch_number}")
            # Tiếp tục xử lý batch tiếp theo
            QTimer.singleShot(50, self.process_next_batch)
            return
            
        # Bắt đầu cập nhật UI và lưu kết quả
        comics_to_update = []
        
        # Xử lý từng kết quả trong batch
        try:
            # Tạm thời tắt sorting
            was_sorting_enabled = self.results_table.isSortingEnabled()
            if was_sorting_enabled:
                self.results_table.setSortingEnabled(False)
                
            for result in results:
                # Lấy thông tin kết quả
                result_index = result.get("index", 0)
                global_index = start_idx + indices[result_index]
                comic_id = result.get("id")
                base_rating = result.get("base_rating", 0)
                
                # Lưu vào cache
                self.rating_results[global_index] = base_rating
                
                # Chuẩn bị dữ liệu để cập nhật database
                if comic_id:
                    comic_update = {"id": comic_id, "base_rating": base_rating}
                    comics_to_update.append(comic_update)
                
                # Cập nhật UI nếu index hợp lệ
                if 0 <= global_index < self.results_table.rowCount():
                    base_rating_item = QTableWidgetItem()
                    base_rating_item.setData(Qt.ItemDataRole.DisplayRole, float(base_rating))
                    self.results_table.setItem(global_index, 9, base_rating_item)
                
            # Bật lại tính năng sorting
            if was_sorting_enabled:
                self.results_table.setSortingEnabled(True)
                
            # Cập nhật UI
            QApplication.processEvents()
                
            # Lưu kết quả vào database
            if comics_to_update:
                self.db_manager.update_comics_rating(comics_to_update)
                logger.info(f"Đã lưu {len(comics_to_update)} ratings vào database")
                
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật UI với kết quả batch {batch_number}: {e}")
        
        # Cập nhật progress
        batch_progress = min(100, int(((batch_number + 1) * 100) / self.total_batches))
        self.progress_bar.setValue(batch_progress)
        
        # Đặt lịch xử lý batch tiếp theo
        QTimer.singleShot(50, self.process_next_batch)
    
    def safe_terminate_rating_thread(self):
        """Đóng rating thread an toàn"""
        if hasattr(self, 'rating_thread') and self.rating_thread is not None:
            try:
                # Ngắt kết nối signals trước
                try:
                    self.rating_thread.progress_updated.disconnect()
                    self.rating_thread.calculation_finished.disconnect()
                except Exception:
                    pass
                
                # Kiểm tra xem thread có đang chạy không
                if self.rating_thread.isRunning():
                    # Thử đợi thread kết thúc với timeout
                    if not self.rating_thread.wait(300):  # 300ms timeout
                        logger.warning("Thread không dừng, buộc phải terminate")
                        self.rating_thread.terminate()
                        self.rating_thread.wait(200)  # Đợi thêm sau khi terminate
                        
                # Đánh dấu thread đã xử lý
                self.rating_thread = None
            except Exception as e:
                logger.error(f"Lỗi khi đóng rating thread: {e}")
                self.rating_thread = None
    
    def on_batch_complete(self, results, start_idx):
        """
        Hàm xử lý hoàn thành batch cũ, giữ lại để tương thích ngược
        Khuyến nghị sử dụng handle_batch_result thay thế
        """
        # Cập nhật kết quả lên UI
        for result in results:
            index = result.get("index")
            global_index = start_idx + index
            base_rating = result.get("base_rating")
            
            # Lưu vào cache kết quả
            self.rating_results[global_index] = base_rating
            
        # Cập nhật UI theo đợt, không phải từng dòng
        if self.results_table.rowCount() > 0:
            # Tạm thời tắt sorting
            was_sorting_enabled = self.results_table.isSortingEnabled()
            if was_sorting_enabled:
                self.results_table.setSortingEnabled(False)
                
            # Cập nhật UI hàng loạt
            for global_index, base_rating in self.rating_results.items():
                if global_index < self.results_table.rowCount():
                    base_rating_item = QTableWidgetItem()
                    base_rating_item.setData(Qt.ItemDataRole.DisplayRole, float(base_rating))
                    self.results_table.setItem(global_index, 9, base_rating_item)
            
            # Bật lại sorting nếu trước đó đã bật
            if was_sorting_enabled:
                self.results_table.setSortingEnabled(True)
        
        # Tăng batch hiện tại
        self.current_batch += 1
        
        # Hiển thị thông báo tiến độ
        batch_percent = (self.current_batch * 100) // self.total_batches
        self.progress_bar.setValue(batch_percent)
        
        if len(self.rating_results) > self.batch_size * 2:
            temp_results = {}
            for i in range(self.current_batch * self.batch_size, min((self.current_batch + 1) * self.batch_size, len(self.all_comics))):
                if i in self.rating_results:
                    temp_results[i] = self.rating_results[i]
            self.rating_results = temp_results
            
        # Delay nhỏ trước khi xử lý batch tiếp theo để cho phép UI cập nhật
        self.batch_timer.start(100)  # 100ms delay
    
    def on_all_batches_complete(self):
        """Xử lý khi tất cả batch đã hoàn thành"""
        # Đảm bảo chỉ thực hiện một lần
        if self.rating_completed:
            return  
            
        self.rating_completed = True
        self.rating_in_progress = False
        
        # Đóng thread nếu còn chạy
        self.safe_terminate_rating_thread()
        
        # Lưu ratings còn lại vào database nếu cần
        self.save_ratings_to_database()
        
        # Ẩn progress bar
        self.progress_bar.setVisible(False)
        
        # Dừng timer
        if self.batch_timer.isActive():
            self.batch_timer.stop()
        
        # Thông báo hoàn thành trong log
        logger.info(f"Đã hoàn thành tính toán rating cho tất cả {len(self.all_comics)} truyện")
    
    def save_ratings_to_database(self):
        """Lưu kết quả tính toán rating vào database"""
        if not self.rating_results:
            return
            
        # Tạo danh sách các comic cần cập nhật
        comics_to_update = []
        
        for index, rating in self.rating_results.items():
            if index < len(self.all_comics):
                comic_id = self.all_comics[index].get("id")
                if comic_id:
                    comic_update = {"id": comic_id, "base_rating": rating}
                    comics_to_update.append(comic_update)
        
        # Thực hiện cập nhật theo batch để tránh quá tải DB
        batch_size = 50
        for i in range(0, len(comics_to_update), batch_size):
            batch = comics_to_update[i:i+batch_size]
            self.db_manager.update_comics_rating(batch)
            QApplication.processEvents()  # Cho phép UI cập nhật
        
        logger.info(f"Đã lưu {len(comics_to_update)} ratings vào database")
    
    def calculate_optimal_worker_count(self, batch_size):
        """Trả về số worker cố định vì không muốn thay đổi tham số"""
        return 8  # Giá trị cố định hoặc dựa trên cấu hình
    
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
        
        # Dừng quá trình tính toán rating
        self.stop_rating_calculation()
        
        # Đặt nguồn dữ liệu
        self.db_manager.set_source(website)
        
        # Lấy danh sách truyện
        self.all_comics = self.db_manager.get_all_comics()
        
        # Hiển thị danh sách truyện
        self.populate_results_table(caculate_rating=True)
        
        logger.info(f"Đã chuyển sang nguồn: {website}, {len(self.all_comics)} truyện")
    
    def stop_rating_calculation(self):
        """Phương thức tập trung dừng tất cả các tác vụ tính toán rating"""
        if self.batch_timer.isActive():
            self.batch_timer.stop()
        
        self.safe_terminate_rating_thread()
        
        # Reset trạng thái
        self.rating_in_progress = False
        self.rating_completed = False
    
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
        
        # Dừng quá trình tính toán rating
        self.stop_rating_calculation()
        
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
        """
        # Cập nhật trạng thái
        self.is_crawling = False
        self.crawl_button.setText("Bắt đầu crawl")
        self.crawl_button.setEnabled(True)
        self.progress_bar.setValue(100)
        
        # Lấy danh sách truyện
        self.all_comics = self.db_manager.get_all_comics()
        
        # Hiển thị thông báo crawl hoàn tất
        QMessageBox.information(
            self, "Thông báo", 
            f"Đã crawl xong {result.get('count', 0)} truyện từ {result.get('website', '')} trong {result.get('time_taken', 0):.2f} giây"
        )
        
        # Hiển thị dữ liệu và bắt đầu tính toán rating
        self.display_all_comics(caculate_rating=True)
        
        # Tính toán rating theo batch
        QTimer.singleShot(500, self.delayed_start_rating)
        
        logger.info(f"Đã crawl xong {result.get('count', 0)} truyện từ {result.get('website', '')}")
        
    def delayed_start_rating(self):
        """Khởi động tính toán rating sau khi UI đã cập nhật"""
        if self.rating_in_progress:
            logger.warning("Đã có quá trình tính toán rating đang chạy")
            return
            
        logger.info("Bắt đầu tính toán rating sau delay...")
        
        # Cho phép UI update hoàn toàn trước khi bắt đầu tính toán
        QApplication.processEvents()
        
        # Đảm bảo không có quá trình cũ đang chạy
        self.stop_rating_calculation()
        
        # Bắt đầu tính toán rating theo batch
        self.start_batch_processing()        
    
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
        
        current_website = self.website_combo.currentText()
        self.db_manager.set_source(current_website)
        
        for row in range(self.results_table.rowCount()):
            checkbox_item = self.results_table.item(row, 0)
            if checkbox_item.checkState() == Qt.CheckState.Checked:
                # Lấy ID của truyện
                comic_id = checkbox_item.data(Qt.ItemDataRole.UserRole)
                
                if comic_id is not None:
                    # Lấy thông tin đầy đủ của truyện từ database
                    comic = self.db_manager.get_comic_by_id(comic_id)
                    
                    if comic and comic.get("nguon") == current_website:
                        self.checked_comics.append(comic)
                    elif comic:
                        logger.warning(f"Truyện ID {comic_id} có nguồn {comic.get('nguon')} khác với nguồn hiện tại {current_website}")

        
        # Gửi signal với danh sách truyện đã chọn
        self.selection_updated.emit(self.checked_comics)
        
        # Hiển thị thông báo
        QMessageBox.information(self, "Thông báo", f"Đã chọn {len(self.checked_comics)} truyện để phân tích")
        
        logger.info(f"Đã chọn {len(self.checked_comics)} truyện để phân tích")
    
    def closeEvent(self, event):
        """Xử lý khi tab bị đóng"""
        # Dừng timer nếu đang chạy
        if self.batch_timer.isActive():
            self.batch_timer.stop()
            
        # Dừng thread nếu đang chạy
        self.safe_terminate_rating_thread()
        
        # Lưu ratings còn lại vào database
        self.save_ratings_to_database()
            
        event.accept()