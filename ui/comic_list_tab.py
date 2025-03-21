from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                            QTableWidget, QTableWidgetItem, QPushButton, 
                            QComboBox, QCheckBox, QHeaderView, QGroupBox,
                            QFormLayout, QSlider, QSpinBox, QTabWidget, QRadioButton,
                            QButtonGroup, QGridLayout)
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
        filter_group = QGroupBox("Bộ lọc và Sắp xếp")
        filter_layout = QVBoxLayout(filter_group)
        
        # Thêm tab cho các loại bộ lọc
        filter_tabs = QTabWidget()
        
        # === Tab 1: Sắp xếp ===  [Thay thế cho bộ lọc cơ bản]
        sorting_tab = QWidget()
        sorting_layout = QVBoxLayout(sorting_tab)
        
        # GroupBox cho tiêu chí sắp xếp
        sort_criteria_group = QGroupBox("Tiêu chí sắp xếp")
        sort_criteria_layout = QVBoxLayout(sort_criteria_group)
        
        # Radio buttons cho các tiêu chí sắp xếp
        self.sort_criteria_group = QButtonGroup(self)
        
        self.sort_views_radio = QRadioButton("Lượt xem")
        self.sort_likes_radio = QRadioButton("Lượt thích")
        self.sort_follows_radio = QRadioButton("Lượt theo dõi")
        self.sort_rating_radio = QRadioButton("Điểm cơ bản")
        
        self.sort_views_radio.setChecked(True)  # Mặc định sắp xếp theo lượt xem
        
        self.sort_criteria_group.addButton(self.sort_views_radio, 1)
        self.sort_criteria_group.addButton(self.sort_likes_radio, 2)
        self.sort_criteria_group.addButton(self.sort_follows_radio, 3)
        self.sort_criteria_group.addButton(self.sort_rating_radio, 4)
        
        # Layout cho các radio button tiêu chí
        sort_criteria_radio_layout = QHBoxLayout()
        sort_criteria_radio_layout.addWidget(self.sort_views_radio)
        sort_criteria_radio_layout.addWidget(self.sort_likes_radio)
        sort_criteria_radio_layout.addWidget(self.sort_follows_radio)
        sort_criteria_radio_layout.addWidget(self.sort_rating_radio)
        
        sort_criteria_layout.addLayout(sort_criteria_radio_layout)
        
        # Radio buttons cho thứ tự sắp xếp
        self.sort_order_group = QButtonGroup(self)
        
        self.sort_desc_radio = QRadioButton("Giảm dần (cao đến thấp)")
        self.sort_asc_radio = QRadioButton("Tăng dần (thấp đến cao)")
        
        self.sort_desc_radio.setChecked(True)  # Mặc định sắp xếp giảm dần
        
        self.sort_order_group.addButton(self.sort_desc_radio, 1)
        self.sort_order_group.addButton(self.sort_asc_radio, 2)
        
        # Layout cho các radio button thứ tự
        sort_order_layout = QHBoxLayout()
        sort_order_layout.addWidget(self.sort_desc_radio)
        sort_order_layout.addWidget(self.sort_asc_radio)
        
        sort_criteria_layout.addLayout(sort_order_layout)
        
        sorting_layout.addWidget(sort_criteria_group)
        
        # === Tab 2: Lọc theo chỉ số tương tác ===
        engagement_filter_tab = QWidget()
        engagement_layout = QVBoxLayout(engagement_filter_tab)
        
        # Tiêu chí lọc
        criteria_group = QGroupBox("Tiêu chí lọc")
        criteria_layout = QVBoxLayout(criteria_group)
        
        # Grid layout cho các tiêu chí
        metrics_grid = QGridLayout()
        
        # Radio buttons cho việc chọn tiêu chí
        self.filter_criteria_group = QButtonGroup(self)
        
        self.views_radio = QRadioButton("Lượt xem")
        self.likes_radio = QRadioButton("Lượt thích")
        self.follows_radio = QRadioButton("Lượt theo dõi")
        
        self.views_radio.setChecked(True)  # Mặc định chọn lượt xem
        
        self.filter_criteria_group.addButton(self.views_radio, 1)
        self.filter_criteria_group.addButton(self.likes_radio, 2)
        self.filter_criteria_group.addButton(self.follows_radio, 3)
        
        # Thêm label
        metrics_grid.addWidget(QLabel("Chọn tiêu chí:"), 0, 0)
        metrics_grid.addWidget(self.views_radio, 0, 1)
        metrics_grid.addWidget(self.likes_radio, 0, 2)
        metrics_grid.addWidget(self.follows_radio, 0, 3)
        
        # SpinBox cho giá trị tối thiểu
        metrics_grid.addWidget(QLabel("Giá trị tối thiểu:"), 1, 0)
        
        # SpinBox cho lượt xem
        self.min_views_spin = QSpinBox()
        self.min_views_spin.setRange(0, 10000000)
        self.min_views_spin.setValue(0)
        self.min_views_spin.setSingleStep(1000)
        self.min_views_spin.setSuffix(" lượt")
        metrics_grid.addWidget(self.min_views_spin, 1, 1)
        
        # SpinBox cho lượt thích
        self.min_likes_spin = QSpinBox()
        self.min_likes_spin.setRange(0, 100000)
        self.min_likes_spin.setValue(0)
        self.min_likes_spin.setSingleStep(100)
        self.min_likes_spin.setSuffix(" lượt")
        metrics_grid.addWidget(self.min_likes_spin, 1, 2)
        
        # SpinBox cho lượt theo dõi
        self.min_follows_spin = QSpinBox()
        self.min_follows_spin.setRange(0, 100000)
        self.min_follows_spin.setValue(0)
        self.min_follows_spin.setSingleStep(100)
        self.min_follows_spin.setSuffix(" lượt")
        metrics_grid.addWidget(self.min_follows_spin, 1, 3)
        
        # Thêm grid vào layout
        criteria_layout.addLayout(metrics_grid)
        
        # Thêm vào layout tab
        engagement_layout.addWidget(criteria_group)
        
        # === Tab 3: Lọc theo tỷ lệ ===
        ratio_filter_tab = QWidget()
        ratio_form = QFormLayout(ratio_filter_tab)
        
        # Hiệu suất (lượt xem/chương)
        self.min_views_per_chapter_spin = QSpinBox()
        self.min_views_per_chapter_spin.setRange(0, 100000)
        self.min_views_per_chapter_spin.setValue(0)
        self.min_views_per_chapter_spin.setSingleStep(100)
        self.min_views_per_chapter_spin.setSuffix(" lượt/chương")
        
        # Hiệu suất (lượt thích/chương)
        self.min_likes_per_chapter_spin = QSpinBox()
        self.min_likes_per_chapter_spin.setRange(0, 1000)
        self.min_likes_per_chapter_spin.setValue(0)
        self.min_likes_per_chapter_spin.setSingleStep(5)
        self.min_likes_per_chapter_spin.setSuffix(" lượt/chương")
        
        ratio_form.addRow("Lượt xem/chương tối thiểu:", self.min_views_per_chapter_spin)
        ratio_form.addRow("Lượt thích/chương tối thiểu:", self.min_likes_per_chapter_spin)
        
        # Thêm các tab vào tab widget
        filter_tabs.addTab(sorting_tab, "Sắp xếp")
        filter_tabs.addTab(engagement_filter_tab, "Lọc theo lượt tương tác")
        filter_tabs.addTab(ratio_filter_tab, "Lọc theo tỷ lệ")
        
        # Thêm tab widget vào layout
        filter_layout.addWidget(filter_tabs)
        
        # Nút áp dụng filter
        self.apply_filter_button = QPushButton("Áp dụng bộ lọc và sắp xếp")
        self.apply_filter_button.clicked.connect(self.apply_filters)
        filter_layout.addWidget(self.apply_filter_button)
        
        main_layout.addWidget(filter_group)
        
        # Bảng danh sách truyện - THAY ĐỔI: Bỏ cột thể loại, thêm cột mô tả
        self.comics_table = QTableWidget()
        self.comics_table.setColumnCount(9)
        self.comics_table.setHorizontalHeaderLabels([
            "Chọn", "Tên truyện", "Tác giả", "Mô tả", 
            "Số chương", "Lượt xem", "Lượt thích", 
            "Lượt theo dõi", "Điểm cơ bản"
        ])
        
        # Điều chỉnh header
        header = self.comics_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)  # Chọn
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)  # Tên truyện
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)  # Tác giả
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)           # Mô tả - cho phép mở rộng
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)  # Số chương
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)  # Lượt xem
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)  # Lượt thích
        header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)  # Lượt theo dõi
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.ResizeToContents)  # Điểm cơ bản
        
        # Đặt chiều cao hàng lớn hơn để hiển thị mô tả dài
        self.comics_table.verticalHeader().setDefaultSectionSize(60)
        
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
        
        # Hiển thị dữ liệu trong bảng với sắp xếp mặc định
        self.apply_filters()
        
        logger.info(f"Đã tải {len(self.comics_data)} truyện")
    
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
            
            # Cắt ngắn mô tả nếu quá dài
            mo_ta = comic.get("mo_ta", "Không có mô tả")
            if len(mo_ta) > 200:
                mo_ta = mo_ta[:197] + "..."
            
            mo_ta_item = QTableWidgetItem(mo_ta)
            mo_ta_item.setToolTip(comic.get("mo_ta", "Không có mô tả"))  # Hiển thị toàn bộ mô tả khi hover
            self.comics_table.setItem(i, 3, mo_ta_item)
            
            # Các thông tin còn lại
            self.comics_table.setItem(i, 4, QTableWidgetItem(str(comic["so_chuong"])))
            self.comics_table.setItem(i, 5, QTableWidgetItem(str(comic["luot_xem"])))
            self.comics_table.setItem(i, 6, QTableWidgetItem(str(comic["luot_thich"])))
            self.comics_table.setItem(i, 7, QTableWidgetItem(str(comic["luot_theo_doi"])))
            
            # Tính điểm cơ bản
            base_rating = BaseRatingCalculator.calculate(comic)
            comic["base_rating"] = base_rating  # Lưu lại để sử dụng cho sắp xếp
            self.comics_table.setItem(i, 8, QTableWidgetItem(f"{base_rating:.2f}"))
        
        # Bật lại sự kiện
        self.comics_table.blockSignals(False)
        
        # Cập nhật thông tin
        self.update_selection_info()
    
    def apply_filters(self):
        """Áp dụng bộ lọc và sắp xếp vào danh sách truyện"""
        logger.info("Đang áp dụng bộ lọc và sắp xếp...")
        
        if not self.comics_data:
            logger.info("Không có dữ liệu truyện để lọc")
            return
        
        # Sao chép danh sách để tránh thay đổi dữ liệu gốc
        working_comics = self.comics_data.copy()
        
        # === Bước 1: Lọc dữ liệu ===
        
        # Lấy giá trị từ các bộ lọc tương tác
        min_views = self.min_views_spin.value()
        min_likes = self.min_likes_spin.value()
        min_follows = self.min_follows_spin.value()
        
        # Xác định tiêu chí lọc chính
        primary_filter = None
        if self.views_radio.isChecked():
            primary_filter = "views"
        elif self.likes_radio.isChecked():
            primary_filter = "likes"
        elif self.follows_radio.isChecked():
            primary_filter = "follows"
        
        # Lấy giá trị tỷ lệ
        min_views_per_chapter = self.min_views_per_chapter_spin.value()
        min_likes_per_chapter = self.min_likes_per_chapter_spin.value()
        
        # Lọc dữ liệu
        filtered_comics = []
        for comic in working_comics:
            # Lấy và chuyển đổi các giá trị số
            try:
                views = int(comic["luot_xem"])
                likes = int(comic["luot_thich"])
                follows = int(comic["luot_theo_doi"])
                chapters = int(comic["so_chuong"]) if comic["so_chuong"] > 0 else 1
            except (ValueError, TypeError):
                views = 0
                likes = 0
                follows = 0
                chapters = 1
            
            # Tính tỷ lệ
            views_per_chapter = views / chapters
            likes_per_chapter = likes / chapters
            
            # Lọc theo tiêu chí chính được chọn
            if primary_filter == "views" and views < min_views:
                continue
            elif primary_filter == "likes" and likes < min_likes:
                continue
            elif primary_filter == "follows" and follows < min_follows:
                continue
            
            # Lọc theo tỷ lệ
            if views_per_chapter < min_views_per_chapter:
                continue
            
            if likes_per_chapter < min_likes_per_chapter:
                continue
            
            filtered_comics.append(comic)
        
        # === Bước 2: Sắp xếp dữ liệu ===
        
        # Xác định tiêu chí sắp xếp
        sort_key = None
        if self.sort_views_radio.isChecked():
            sort_key = "luot_xem"
            key_func = lambda x: int(x.get(sort_key, 0))
        elif self.sort_likes_radio.isChecked():
            sort_key = "luot_thich"
            key_func = lambda x: int(x.get(sort_key, 0))
        elif self.sort_follows_radio.isChecked():
            sort_key = "luot_theo_doi"
            key_func = lambda x: int(x.get(sort_key, 0))
        elif self.sort_rating_radio.isChecked():
            sort_key = "base_rating"
            # Tính điểm cơ bản cho các truyện chưa có
            for comic in filtered_comics:
                if "base_rating" not in comic:
                    comic["base_rating"] = BaseRatingCalculator.calculate(comic)
            key_func = lambda x: float(x.get(sort_key, 0))
        
        # Xác định hướng sắp xếp
        reverse_sort = self.sort_desc_radio.isChecked()  # True nếu sắp xếp giảm dần
        
        # Sắp xếp danh sách
        if sort_key:
            filtered_comics.sort(key=key_func, reverse=reverse_sort)
            
            # Log thông tin sắp xếp
            order_text = "giảm dần" if reverse_sort else "tăng dần"
            logger.info(f"Đã sắp xếp theo {sort_key} {order_text}")
        
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