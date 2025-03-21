from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                            QTableWidget, QTableWidgetItem, QPushButton, 
                            QComboBox, QCheckBox, QHeaderView, QGroupBox,
                            QFormLayout, QSlider, QSpinBox)
from PyQt6.QtCore import Qt, pyqtSignal
import logging

from analysis.base_rating import BaseRatingCalculator

logger = logging.getLogger(__name__)

class ComicListTab(QWidget):
    # Signal phát ra khi bắt đầu phân tích
    analysis_started = pyqtSignal(list)
    
    def __init__(self, db_manager, log_widget):
        super().__init__()
        self.db_manager = db_manager
        self.log_widget = log_widget
        self.comics_data = []
        self.selected_comics = []
        
        # Thiết lập UI
        self.init_ui()
    
    def init_ui(self):
        # Layout chính
        main_layout = QVBoxLayout(self)
        
        # Group box filter
        filter_group = QGroupBox("Bộ lọc")
        filter_layout = QFormLayout(filter_group)
        
        # Combobox thể loại
        self.genre_combo = QComboBox()
        self.genre_combo.addItem("Tất cả thể loại")
        # Sẽ được populate từ database sau
        
        # Slider lượt xem
        self.view_slider = QSlider(Qt.Orientation.Horizontal)
        self.view_slider.setRange(0, 100)
        self.view_slider.setValue(0)
        
        # SpinBox số comment tối thiểu
        self.min_comments_spin = QSpinBox()
        self.min_comments_spin.setRange(0, 1000)
        self.min_comments_spin.setValue(0)
        
        # Thêm các control vào layout
        filter_layout.addRow("Thể loại:", self.genre_combo)
        filter_layout.addRow("Lượt xem tối thiểu:", self.view_slider)
        filter_layout.addRow("Số comment tối thiểu:", self.min_comments_spin)
        
        # Nút áp dụng filter
        self.apply_filter_button = QPushButton("Áp dụng bộ lọc")
        self.apply_filter_button.clicked.connect(self.apply_filters)
        
        filter_layout.addRow("", self.apply_filter_button)
        
        main_layout.addWidget(filter_group)
        
        # Bảng danh sách truyện
        self.comics_table = QTableWidget()
        self.comics_table.setColumnCount(9)
        self.comics_table.setHorizontalHeaderLabels([
            "Chọn", "Tên truyện", "Tác giả", "Thể loại", 
            "Số chương", "Lượt xem", "Lượt thích", 
            "Lượt theo dõi", "Điểm cơ bản"
        ])
        
        # Điều chỉnh header
        header = self.comics_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        
        main_layout.addWidget(self.comics_table)
        
        # Controls dưới bảng
        bottom_layout = QHBoxLayout()
        
        # Checkbox chọn tất cả
        self.select_all_checkbox = QCheckBox("Chọn tất cả")
        self.select_all_checkbox.stateChanged.connect(self.toggle_select_all)
        
        # Label thông tin
        self.info_label = QLabel("0 truyện được chọn")
        
        # Nút phân tích
        self.analyze_button = QPushButton("Phân tích truyện đã chọn")
        self.analyze_button.clicked.connect(self.start_analysis)
        
        # Nút làm mới
        self.refresh_button = QPushButton("Làm mới danh sách")
        self.refresh_button.clicked.connect(self.load_comics)
        
        # Thêm vào layout
        bottom_layout.addWidget(self.select_all_checkbox)
        bottom_layout.addWidget(self.info_label)
        bottom_layout.addStretch()
        bottom_layout.addWidget(self.refresh_button)
        bottom_layout.addWidget(self.analyze_button)
        
        main_layout.addLayout(bottom_layout)
        
        # Kết nối sự kiện thay đổi selection
        self.comics_table.itemChanged.connect(self.on_item_changed)
    
    def load_comics(self):
        """Tải danh sách truyện từ database"""
        logger.info("Đang tải danh sách truyện từ database...")
        
        # Lấy dữ liệu từ database
        self.comics_data = self.db_manager.get_all_comics()
        
        # Tải thể loại vào combo box
        self.load_genres()
        
        # Hiển thị dữ liệu trong bảng
        self.populate_table(self.comics_data)
        
        logger.info(f"Đã tải {len(self.comics_data)} truyện")
    
    def load_genres(self):
        """Tải danh sách thể loại vào combo box"""
        # Lưu trữ thể loại hiện tại
        current_genre = self.genre_combo.currentText()
        
        # Xóa tất cả item
        self.genre_combo.clear()
        self.genre_combo.addItem("Tất cả thể loại")
        
        # Lấy danh sách thể loại từ database
        genres = self.db_manager.get_all_genres()
        
        # Thêm vào combo box
        for genre in genres:
            self.genre_combo.addItem(genre)
        
        # Khôi phục lại thể loại đã chọn
        index = self.genre_combo.findText(current_genre)
        if index >= 0:
            self.genre_combo.setCurrentIndex(index)
    
    def populate_table(self, comics):
        """Hiển thị dữ liệu truyện vào bảng"""
        # Tắt sự kiện để tránh trigger on_item_changed
        self.comics_table.blockSignals(True)
        
        # Xóa tất cả hàng
        self.comics_table.setRowCount(0)
        
        # Thêm dữ liệu
        for i, comic in enumerate(comics):
            self.comics_table.insertRow(i)
            
            # Checkbox chọn
            checkbox = QTableWidgetItem()
            checkbox.setFlags(Qt.ItemFlag.ItemIsUserCheckable | 
                             Qt.ItemFlag.ItemIsEnabled)
            checkbox.setCheckState(Qt.CheckState.Unchecked)
            self.comics_table.setItem(i, 0, checkbox)
            
            # Thông tin truyện
            self.comics_table.setItem(i, 1, QTableWidgetItem(comic["ten_truyen"]))
            self.comics_table.setItem(i, 2, QTableWidgetItem(comic["tac_gia"]))
            self.comics_table.setItem(i, 3, QTableWidgetItem(comic["the_loai"]))
            self.comics_table.setItem(i, 4, QTableWidgetItem(str(comic["so_chuong"])))
            self.comics_table.setItem(i, 5, QTableWidgetItem(str(comic["luot_xem"])))
            self.comics_table.setItem(i, 6, QTableWidgetItem(str(comic["luot_thich"])))
            self.comics_table.setItem(i, 7, QTableWidgetItem(str(comic["luot_theo_doi"])))
            
            # Tính điểm cơ bản
            base_rating = BaseRatingCalculator.calculate(comic)
            self.comics_table.setItem(i, 8, QTableWidgetItem(f"{base_rating:.2f}"))
        
        # Bật lại sự kiện
        self.comics_table.blockSignals(False)
        
        # Cập nhật thông tin
        self.update_selection_info()
    
    def apply_filters(self):
        """Áp dụng bộ lọc vào danh sách truyện"""
        logger.info("Đang áp dụng bộ lọc...")
        
        # Lấy giá trị filter
        genre = self.genre_combo.currentText()
        min_views = self.view_slider.value() * 1000  # Chuyển đổi thành lượt xem thực
        min_comments = self.min_comments_spin.value()
        
        # Lọc dữ liệu
        filtered_comics = []
        for comic in self.comics_data:
            # Lọc theo thể loại
            if genre != "Tất cả thể loại" and genre not in comic["the_loai"]:
                continue
            
            # Lọc theo lượt xem
            if comic["luot_xem"] < min_views:
                continue
            
            # Lọc theo số lượng comment
            if comic["so_binh_luan"] < min_comments:
                continue
            
            filtered_comics.append(comic)
        
        # Cập nhật bảng
        self.populate_table(filtered_comics)
        
        logger.info(f"Kết quả lọc: {len(filtered_comics)}/{len(self.comics_data)} truyện")
    
    def toggle_select_all(self, state):
        """Chọn/bỏ chọn tất cả truyện"""
        # Tắt sự kiện để tránh trigger on_item_changed
        self.comics_table.blockSignals(True)
        
        check_state = Qt.CheckState.Checked if state == Qt.CheckState.Checked else Qt.CheckState.Unchecked
        
        # Cập nhật tất cả các checkbox
        for i in range(self.comics_table.rowCount()):
            item = self.comics_table.item(i, 0)
            item.setCheckState(check_state)
        
        # Bật lại sự kiện
        self.comics_table.blockSignals(False)
        
        # Cập nhật danh sách đã chọn
        self.update_selection()
    
    def on_item_changed(self, item):
        """Xử lý khi có item thay đổi"""
        # Chỉ quan tâm đến cột checkbox
        if item.column() == 0:
            self.update_selection()
    
    def update_selection(self):
        """Cập nhật danh sách truyện đã chọn"""
        self.selected_comics = []
        
        for i in range(self.comics_table.rowCount()):
            item = self.comics_table.item(i, 0)
            if item.checkState() == Qt.CheckState.Checked:
                # Lấy tên truyện
                comic_name = self.comics_table.item(i, 1).text()
                
                # Tìm thông tin truyện trong dữ liệu gốc
                for comic in self.comics_data:
                    if comic["ten_truyen"] == comic_name:
                        self.selected_comics.append(comic)
                        break
        
        # Cập nhật thông tin
        self.update_selection_info()
    
    def update_selection_info(self):
        """Cập nhật thông tin về số lượng truyện đã chọn"""
        self.info_label.setText(f"{len(self.selected_comics)} truyện được chọn")
        
        # Cập nhật trạng thái nút phân tích
        self.analyze_button.setEnabled(len(self.selected_comics) > 0)
    
    def start_analysis(self):
        """Bắt đầu phân tích chi tiết các truyện đã chọn"""
        if not self.selected_comics:
            logger.warning("Chưa có truyện nào được chọn để phân tích")
            return
        
        logger.info(f"Bắt đầu phân tích {len(self.selected_comics)} truyện")
        
        # Phát signal để thông báo bắt đầu phân tích
        self.analysis_started.emit(self.selected_comics)