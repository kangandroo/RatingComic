from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                            QTabWidget, QTableWidget, QTableWidgetItem, 
                            QPushButton, QFileDialog, QHeaderView, QProgressBar,
                            QMessageBox, QComboBox, QProgressDialog, QSpinBox, QCheckBox)
from PyQt6.QtCore import Qt, QThreadPool, pyqtSlot
import logging
import time
import os
import traceback
from datetime import datetime
from PyQt6.QtWidgets import QDialog
from PyQt6.QtGui import QColor
from datetime import datetime, timedelta
from analysis.rating_factory import RatingFactory
import pandas as pd
from PyQt6.QtGui import QIcon
import gc

from utils.worker import Worker
from analysis.sentiment_analyzer import SentimentAnalyzer
from analysis.rating_factory import RatingFactory

logger = logging.getLogger(__name__)

class DetailAnalysisTab(QWidget):
    """
    Tab để phân tích đánh giá truyện
    """
    
    def __init__(self, db_manager, crawler_factory, log_widget, config_manager):
        super().__init__()
        
        self.db_manager = db_manager
        self.crawler_factory = crawler_factory
        self.log_widget = log_widget
        self.config_manager = config_manager
        self.selected_comics = []
        self.analysis_results = []
        self.sentiment_analyzer = None  # Khởi tạo khi cần
        self.is_analyzing = False
        
        # Thiết lập UI
        self.init_ui()
        
        # Tạo thư mục xuất kết quả nếu chưa có
        os.makedirs("output", exist_ok=True)
        
        self.load_history_data()
        
        logger.info("Khởi tạo DetailAnalysisTab thành công")
    
    def init_ui(self):
        """Thiết lập giao diện người dùng"""
        
        # Layout chính
        main_layout = QVBoxLayout(self)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        main_layout.addWidget(self.progress_bar)
        
        time_limit_layout = QHBoxLayout()
        time_limit_layout.addWidget(QLabel("Giới hạn comment trong:"))
        
        self.limit_spinbox = QSpinBox() 
        self.limit_spinbox.setRange(1, 30)
        self.limit_spinbox.setValue(7) 
        self.limit_spinbox.setSuffix(" ngày gần nhất")
        time_limit_layout.addWidget(self.limit_spinbox)
        
        self.limit_checkbox = QCheckBox("Áp dụng giới hạn")
        self.limit_checkbox.setChecked(True)
        time_limit_layout.addWidget(self.limit_checkbox)
        
        time_limit_layout.addStretch()
        main_layout.addLayout(time_limit_layout)
            
        # Các tab kết quả
        self.result_tabs = QTabWidget()
        
        # Tab tổng quan
        self.overview_tab = QWidget()
        overview_layout = QVBoxLayout(self.overview_tab)
        
        # Label thông tin
        self.info_label = QLabel("Chưa có truyện nào được chọn để phân tích")
        overview_layout.addWidget(self.info_label)
        
        # Bảng kết quả
        # Bảng kết quả
        self.result_table = QTableWidget()
        self.result_table.setColumnCount(11)
        self.result_table.setHorizontalHeaderLabels([
            "Xếp hạng", "Tên truyện", "Nguồn", "Mô tả", "Số chương", 
            "Lượt xem", "Rating", "Lượt đánh giá", "Điểm cơ bản",
            "Điểm sentiment", "Điểm tổng hợp"
        ])

        # Điều chỉnh header
        header = self.result_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)  # Xếp hạng
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)  # Tên truyện
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)  # Nguồn
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)  # Mô tả
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)  # Số chương
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)  # Lượt xem
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)  
        header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)  
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.ResizeToContents)  
        header.setSectionResizeMode(9, QHeaderView.ResizeMode.ResizeToContents)  
        header.setSectionResizeMode(10, QHeaderView.ResizeMode.ResizeToContents)  

        # Cho phép sắp xếp
        self.result_table.setSortingEnabled(True)
        
        # Thêm vào phần view_layout trong init_ui()

        # Controls cho bộ lọc
        filter_layout = QHBoxLayout()

        sort_label = QLabel("Sắp xếp theo:")
        self.sort_field_combo = QComboBox()
        self.sort_field_combo.addItems([
            "Điểm tổng hợp", "Điểm cơ bản", "Điểm sentiment", 
            "Lượt xem", "Số chương"
        ])

        self.sort_order_combo = QComboBox()
        self.sort_order_combo.addItems(["Cao đến thấp", "Thấp đến cao"])

        self.apply_sort_button = QPushButton("Áp dụng")
        self.apply_sort_button.clicked.connect(self.apply_sorting)

        filter_layout.addWidget(sort_label)
        filter_layout.addWidget(self.sort_field_combo)
        filter_layout.addWidget(self.sort_order_combo)
        filter_layout.addWidget(self.apply_sort_button)
        filter_layout.addStretch()

        # Thêm layout vào overview_layout trước bảng kết quả
        overview_layout.addLayout(filter_layout)
        overview_layout.addWidget(self.result_table)
        
        overview_layout.addWidget(self.result_table)
        
        # Tab chi tiết comment
        self.comment_tab = QWidget()
        comment_layout = QVBoxLayout(self.comment_tab)
        
        # Bảng chi tiết comment
        self.comment_table = QTableWidget()
        self.comment_table.setColumnCount(5)
        self.comment_table.setHorizontalHeaderLabels([
            "Tên truyện", "Người bình luận", "Nội dung", 
            "Sentiment", "Độ tin cậy"
        ])
        
        # Điều chỉnh header
        comment_header = self.comment_table.horizontalHeader()
        comment_header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)  # Tên truyện
        comment_header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)  # Người bình luận
        comment_header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)  # Nội dung
        comment_header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)  # Sentiment
        comment_header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)  # Điểm sentiment
        
        comment_layout.addWidget(self.comment_table)
        
        # Thêm các tab vào tab widget
        self.result_tabs.addTab(self.overview_tab, "Tổng quan")
        self.result_tabs.addTab(self.comment_tab, "Chi tiết comment")
        
        main_layout.addWidget(self.result_tabs)
        
        # Controls
        control_layout = QHBoxLayout()
        
        # self.export_button = QPushButton("Xuất kết quả ra Excel")
        # self.export_button.clicked.connect(self.export_results)
        # self.export_button.setEnabled(False)
        
        self.analyze_button = QPushButton("Bắt đầu phân tích")
        self.analyze_button.clicked.connect(self.start_analysis)
        self.analyze_button.setEnabled(False)
        
        control_layout.addWidget(self.analyze_button)
        # control_layout.addWidget(self.export_button)
        
        main_layout.addLayout(control_layout)


        # Thêm tab lịch sử phân tích
        self.history_tab = QWidget()
        history_layout = QVBoxLayout(self.history_tab)

        # Thêm bộ lọc nguồn dữ liệu
        filter_layout = QHBoxLayout()
        filter_label = QLabel("Nguồn dữ liệu:")
        self.source_combo = QComboBox()
        self.source_combo.addItems(["Tất cả", "TruyenQQ", "NetTruyen", "Manhuavn"])
        self.source_combo.currentTextChanged.connect(self.filter_history_data)

        # Thêm bộ lọc thời gian
        self.time_filter_combo = QComboBox()
        self.time_filter_combo.addItems(["Tất cả", "7 ngày gần đây", "30 ngày gần đây"])
        self.time_filter_combo.currentTextChanged.connect(self.filter_history_data)

        filter_layout.addWidget(filter_label)
        filter_layout.addWidget(self.source_combo)
        filter_layout.addWidget(QLabel("Thời gian:"))
        filter_layout.addWidget(self.time_filter_combo)
        filter_layout.addStretch()

        # Thêm buttons cho các chức năng
        button_layout = QHBoxLayout()
        self.refresh_button = QPushButton("Làm mới dữ liệu")
        self.refresh_button.clicked.connect(self.load_history_data)
        self.export_history_button = QPushButton("Xuất ra Excel")
        self.export_history_button.clicked.connect(self.export_history_to_excel)
        self.export_history_button.setIcon(QIcon.fromTheme("document-save"))

        button_layout.addStretch()
        button_layout.addWidget(self.refresh_button)
        button_layout.addWidget(self.export_history_button)

        # Tạo bảng lịch sử phân tích - KHỞI TẠO TRƯỚC KHI SỬ DỤNG
        self.history_table = QTableWidget()
        self.history_table.setColumnCount(11)
        self.history_table.setHorizontalHeaderLabels([
            "Tên truyện", "Nguồn", "Số comment", "Sentiment tích cực (%)", 
            "Sentiment tiêu cực (%)", "Sentiment trung tính (%)", 
            "Điểm sentiment", "Điểm tổng hợp", "Thời gian phân tích",
            "Chi tiết", "Xóa"
        ])

        # Thiết lập header
        header = self.history_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)  # Tên truyện
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)  # Nguồn
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)  # Số comment
        for i in range(3, 8):
            header.setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.ResizeToContents)  # Thời gian
        header.setSectionResizeMode(9, QHeaderView.ResizeMode.ResizeToContents)  # Chi tiết
        header.setSectionResizeMode(10, QHeaderView.ResizeMode.ResizeToContents)  

        # Thêm các layout và widget theo đúng thứ tự
        history_layout.addLayout(filter_layout)
        history_layout.addLayout(button_layout)
        history_layout.addWidget(self.history_table)

        # # Thêm biểu đồ phân tích sentiment
        # self.sentiment_chart_layout = QVBoxLayout()
        # self.sentiment_chart_label = QLabel("Biểu đồ phân tích sentiment")
        # self.sentiment_chart_layout.addWidget(self.sentiment_chart_label)

        # # Tạo placeholder cho biểu đồ
        # chart_placeholder = QLabel("Biểu đồ sẽ hiển thị ở đây")
        # chart_placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        # chart_placeholder.setStyleSheet("background-color: #f0f0f0; min-height: 200px;")
        # self.sentiment_chart_layout.addWidget(chart_placeholder)

        # history_layout.addLayout(self.sentiment_chart_layout)

        # Thêm tab vào result_tabs
        self.result_tabs.addTab(self.history_tab, "Lịch sử phân tích")
            
    def get_comment_table_for_comic(self, comic_name, source):
        """
        Lấy bảng comment cho một truyện cụ thể
        """
        try:
            self.db_manager.set_source(source)
            
            # Lấy tất cả truyện và tìm truyện cần thiết
            all_comics = self.db_manager.get_all_comics()
            comic = next((c for c in all_comics if c['ten_truyen'] == comic_name), None)
            
            if comic:
                comic_id = comic['id']
                comments = self.db_manager.get_all_comments(comic_id)
                
                # Tạo bảng tạm thời để lưu comments
                temp_table = QTableWidget()
                temp_table.setColumnCount(4)
                temp_table.setHorizontalHeaderLabels([
                    "Người bình luận", "Nội dung", "Sentiment", "Điểm sentiment"
                ])
                
                for comment in comments:
                    row = temp_table.rowCount()
                    temp_table.insertRow(row)
                    
                    # Tên người bình luận
                    ten_nguoi_bl = comment.get("ten_nguoi_binh_luan", "N/A")
                    ten_nguoi_bl_short = (ten_nguoi_bl[:20] + "...") if len(ten_nguoi_bl) > 20 else ten_nguoi_bl
                    name_item = QTableWidgetItem(ten_nguoi_bl_short)
                    if len(ten_nguoi_bl) > 20:
                        name_item.setToolTip(ten_nguoi_bl)
                    temp_table.setItem(row, 0, name_item)
                    
                    # Nội dung comment
                    noi_dung = comment.get("noi_dung", "N/A")
                    noi_dung_short = (noi_dung[:100] + "...") if len(noi_dung) > 100 else noi_dung
                    content_item = QTableWidgetItem(noi_dung_short)
                    content_item.setToolTip(noi_dung)
                    temp_table.setItem(row, 1, content_item)
                    
                    # Sentiment và điểm
                    sentiment = comment.get("sentiment", "neutral")
                    sentiment_score = comment.get("sentiment_score", 0.5)
                    
                    sentiment_item = QTableWidgetItem(sentiment)
                    score_item = QTableWidgetItem(f"{sentiment_score:.2f}")
                    
                    # Màu sắc cho các loại sentiment
                    if sentiment == "positive":
                        sentiment_item.setBackground(QColor(200, 255, 200))
                    elif sentiment == "negative":
                        sentiment_item.setBackground(QColor(255, 200, 200))
                    
                    temp_table.setItem(row, 2, sentiment_item)
                    temp_table.setItem(row, 3, score_item)
                
                return temp_table
                
            return None
                
        except Exception as e:
            logger.error(f"Lỗi khi lấy bảng comment cho truyện {comic_name}: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def export_history_to_excel(self):
        """Xuất dữ liệu lịch sử phân tích ra file Excel"""
        if self.history_table.rowCount() == 0:
            QMessageBox.warning(self, "Cảnh báo", "Không có dữ liệu để xuất!")
            return
        
        try:
            current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
            default_filename = f"output/sentiment_history_{current_datetime}.xlsx"
            
            file_path, _ = QFileDialog.getSaveFileName(
                self, "Lưu file Excel", default_filename, "Excel Files (*.xlsx)"
            )
            
            if not file_path:
                return

            progress_dialog = QProgressDialog("Đang xuất dữ liệu...", "Hủy", 0, self.history_table.rowCount(), self)
            progress_dialog.setWindowTitle("Xuất Excel")
            progress_dialog.setWindowModality(Qt.WindowModality.WindowModal)
            
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                # Sheet 1: Phân tích Sentiment
                data = []
                for row in range(self.history_table.rowCount()):
                    row_data = {
                        "Tên truyện": self.history_table.item(row, 0).text(),
                        "Nguồn": self.history_table.item(row, 1).text(),
                        "Số comment": int(self.history_table.item(row, 2).text()),
                        "Sentiment tích cực (%)": float(self.history_table.item(row, 3).text().replace('%', '')),
                        "Sentiment tiêu cực (%)": float(self.history_table.item(row, 4).text().replace('%', '')),
                        "Sentiment trung tính (%)": float(self.history_table.item(row, 5).text().replace('%', '')),
                        "Điểm sentiment": float(self.history_table.item(row, 6).text()),
                        "Điểm tổng hợp": float(self.history_table.item(row, 7).text()),
                        "Thời gian phân tích": self.history_table.item(row, 8).text()
                    }
                    data.append(row_data)
                
                df = pd.DataFrame(data)
                df.to_excel(writer, sheet_name='Phân tích Sentiment', index=False)
                
                # Sheet 2: Thống kê nguồn
                stats_df = pd.DataFrame(self.create_stats_data(data))
                stats_df.to_excel(writer, sheet_name='Thống kê nguồn', index=False)
                
                # Sheet 3: Chi tiết Comments
                comment_data = []
                total_rows = self.history_table.rowCount()
                
                for row in range(total_rows):
                    if progress_dialog.wasCanceled():
                        break
                        
                    comic_name = self.history_table.item(row, 0).text()
                    source = self.history_table.item(row, 1).text()
                    
                    progress_dialog.setValue(row)
                    progress_dialog.setLabelText(f"Đang xử lý: {comic_name}")
                    
                    logger.info(f"Đang xử lý comments cho truyện {comic_name} ({row + 1}/{total_rows})")
                    
                    comment_table = self.get_comment_table_for_comic(comic_name, source)
                    if comment_table:
                        for comment_row in range(comment_table.rowCount()):
                            comment_data.append({
                                "Tên truyện": comic_name,
                                "Nguồn": source,
                                "Người bình luận": comment_table.item(comment_row, 0).text(),
                                "Nội dung": comment_table.item(comment_row, 1).text(),
                                "Sentiment": comment_table.item(comment_row, 2).text(),
                                "Độ tin cậy": float(comment_table.item(comment_row, 3).text()),
                                "Thời gian xuất": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            })
                
                progress_dialog.setValue(total_rows)
                
                if comment_data:
                    comments_df = pd.DataFrame(comment_data)
                    comments_df.to_excel(writer, sheet_name='Chi tiết Comments', index=False)
                    
                    # Tự động điều chỉnh độ rộng cột
                    worksheet = writer.sheets['Chi tiết Comments']
                    for idx, col in enumerate(comments_df.columns):
                        max_length = max(
                            comments_df[col].astype(str).apply(len).max(),
                            len(col)
                        )
                        worksheet.column_dimensions[chr(65 + idx)].width = min(max_length + 2, 50)

            if not progress_dialog.wasCanceled():
                QMessageBox.information(
                    self, "Thành công", 
                    f"Đã xuất dữ liệu ra file:\n{file_path}"
                )
                
                logger.info(f"Đã xuất dữ liệu lịch sử phân tích ra file: {file_path}")
            
        except Exception as e:
            logger.error(f"Lỗi khi xuất file Excel: {str(e)}")
            logger.error(traceback.format_exc())
            QMessageBox.critical(self, "Lỗi", f"Lỗi khi xuất file Excel: {str(e)}")
        
        
    def create_stats_data(self, data):
        """Tạo dữ liệu thống kê cho từng nguồn"""
        sources = ['TruyenQQ', 'NetTruyen', 'Manhuavn', 'Tất cả']
        stats = {
            'Nguồn': sources,
            'Số lượng truyện': [],
            'Điểm sentiment trung bình': [],
            'Điểm tổng hợp trung bình': [],
            'Sentiment tích cực trung bình (%)': [],
            'Sentiment tiêu cực trung bình (%)': []
        }
        
        for source in sources:
            source_data = [d for d in data if d['Nguồn'] == source] if source != 'Tất cả' else data
            count = len(source_data)
            
            stats['Số lượng truyện'].append(count)
            
            if count > 0:
                stats['Điểm sentiment trung bình'].append(
                    sum(d['Điểm sentiment'] for d in source_data) / count
                )
                stats['Điểm tổng hợp trung bình'].append(
                    sum(d['Điểm tổng hợp'] for d in source_data) / count
                )
                stats['Sentiment tích cực trung bình (%)'].append(
                    sum(d['Sentiment tích cực (%)'] for d in source_data) / count
                )
                stats['Sentiment tiêu cực trung bình (%)'].append(
                    sum(d['Sentiment tiêu cực (%)'] for d in source_data) / count
                )
            else:
                stats['Điểm sentiment trung bình'].append(0)
                stats['Điểm tổng hợp trung bình'].append(0)
                stats['Sentiment tích cực trung bình (%)'].append(0)
                stats['Sentiment tiêu cực trung bình (%)'].append(0)
        
        return stats
    
    def load_history_data(self):
        """Tải dữ liệu lịch sử phân tích từ cơ sở dữ liệu"""
        self.history_table.setRowCount(0)
        
        # Tính toán thời gian lọc nếu có
        time_filter = self.time_filter_combo.currentText()
        days = None
        if time_filter == "7 ngày gần đây":
            days = 7
        elif time_filter == "30 ngày gần đây":
            days = 30
        
        # Lấy nguồn dữ liệu đã chọn
        source = self.source_combo.currentText()
        sources = []
        if source == "Tất cả":
            sources = ["TruyenQQ", "NetTruyen", "Manhuavn"]
        else:
            sources = [source]
        
        analyzed_comics = []
        
        # Lấy dữ liệu từ mỗi nguồn
        for source_name in sources:
            # Đặt nguồn
            self.db_manager.set_source(source_name)
            
            # Lấy tất cả truyện từ nguồn này
            comics = self.db_manager.get_all_comics()
            
            for comic in comics:
                # Lấy tất cả comments cho truyện này
                comments = self.db_manager.get_all_comments(comic["id"])
                
                # Chỉ xử lý truyện có comments đã phân tích sentiment
                sentiment_comments = [c for c in comments if c.get("sentiment") is not None]
                
                if sentiment_comments:
                    # Tính toán thống kê sentiment
                    positive = [c for c in sentiment_comments if c.get("sentiment") == "positive"]
                    negative = [c for c in sentiment_comments if c.get("sentiment") == "negative"]
                    neutral = [c for c in sentiment_comments if c.get("sentiment") == "neutral"]
                    
                    total = len(sentiment_comments)
                    positive_percent = len(positive) / total * 100 if total > 0 else 0
                    negative_percent = len(negative) / total * 100 if total > 0 else 0
                    neutral_percent = len(neutral) / total * 100 if total > 0 else 0
                    
                    # Tính điểm sentiment dựa trên công thức hiện tại
                    sentiment_score = (positive_percent * 8/100 ) - (negative_percent * 5/100) + (neutral_percent * 6/100)
                    sentiment_score = max(0, min(10, sentiment_score * 2))
                    
                    # Tính điểm tổng hợp
                    rating_calculator = RatingFactory.get_calculator(source_name)
                    base_rating = rating_calculator.calculate(comic)
                    comprehensive_rating = base_rating * 0.6 + sentiment_score * 0.4
                    
                    # Thêm thời gian phân tích (sử dụng thời gian cập nhật của comment gần nhất)
                    if sentiment_comments:
                        analysis_time = max(c.get("thoi_gian_cap_nhat", "") for c in sentiment_comments)
                    else:
                        analysis_time = "Không rõ"
                    
                    # Thêm vào danh sách
                    analyzed_comics.append({
                        "comic": comic,
                        "source": source_name,
                        "comments": sentiment_comments,
                        "positive_percent": positive_percent,
                        "negative_percent": negative_percent,
                        "neutral_percent": neutral_percent,
                        "sentiment_score": sentiment_score,
                        "base_rating": base_rating,
                        "comprehensive_rating": comprehensive_rating,
                        "analysis_time": analysis_time
                    })
        
        # Sắp xếp theo thời gian phân tích, mới nhất lên đầu
        analyzed_comics.sort(key=lambda x: x["analysis_time"], reverse=True)
        
        # Hiển thị dữ liệu trong bảng
        for comic_data in analyzed_comics:
            row = self.history_table.rowCount()
            self.history_table.insertRow(row)
            
            comic = comic_data["comic"]
            
            self.history_table.setItem(row, 0, QTableWidgetItem(comic.get("ten_truyen", "")))
            self.history_table.setItem(row, 1, QTableWidgetItem(comic_data["source"]))
            self.history_table.setItem(row, 2, QTableWidgetItem(str(len(comic_data["comments"]))))
            
            # Format phần trăm
            self.history_table.setItem(row, 3, QTableWidgetItem(f"{comic_data['positive_percent']:.1f}%"))
            self.history_table.setItem(row, 4, QTableWidgetItem(f"{comic_data['negative_percent']:.1f}%"))
            self.history_table.setItem(row, 5, QTableWidgetItem(f"{comic_data['neutral_percent']:.1f}%"))
            
            self.history_table.setItem(row, 6, QTableWidgetItem(f"{comic_data['sentiment_score']:.2f}"))
            self.history_table.setItem(row, 7, QTableWidgetItem(f"{comic_data['comprehensive_rating']:.2f}"))
            self.history_table.setItem(row, 8, QTableWidgetItem(str(comic_data["analysis_time"])))
            
            # Nút Chi tiết
            detail_button = QPushButton("Chi tiết")
            detail_button.clicked.connect(lambda _, c=comic: self.show_sentiment_details(c))
            self.history_table.setCellWidget(row, 9, detail_button)
            
            # Nút Xóa (thay thế cho Phân tích lại)
            delete_button = QPushButton("Xóa")
            delete_button.setIcon(QIcon.fromTheme("edit-delete"))  # Thêm icon nếu có thể
            delete_button.setStyleSheet("background-color: #ffcccc;")  # Màu đỏ nhạt cho nút xóa
            delete_button.clicked.connect(lambda _, c=comic: self.delete_analysis(c))
            self.history_table.setCellWidget(row, 10, delete_button)
        

    def filter_history_data(self):
        """Lọc dữ liệu lịch sử theo bộ lọc đã chọn"""
        # Gọi lại load_history_data sẽ áp dụng các bộ lọc hiện tại
        self.load_history_data()

    def show_sentiment_details(self, comic):
        """Hiển thị chi tiết phân tích sentiment của một truyện"""
        # Tạo dialog hiển thị chi tiết
        dialog = QDialog(self)
        dialog.setWindowTitle(f"Chi tiết sentiment: {comic.get('ten_truyen', '')}")
        dialog.setMinimumSize(800, 600)
        
        layout = QVBoxLayout(dialog)
        
        # Tạo bảng comments
        comment_table = QTableWidget()
        comment_table.setColumnCount(4)
        comment_table.setHorizontalHeaderLabels([
            "Người bình luận", "Nội dung", "Sentiment", "Điểm"
        ])
        
        header = comment_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        
        # Lấy comments cho truyện này
        self.db_manager.set_source(comic.get("nguon", "TruyenQQ"))
        comments = self.db_manager.get_all_comments(comic["id"])
        
        # Thêm comments vào bảng
        for comment in comments:
            row = comment_table.rowCount()
            comment_table.insertRow(row)
            
            ten_nguoi_bl = comment.get("ten_nguoi_binh_luan", "")
            ten_nguoi_bl_short = ten_nguoi_bl[:20] + "..." if len(ten_nguoi_bl) > 20 else ten_nguoi_bl
            name_item = QTableWidgetItem(ten_nguoi_bl_short)
            if len(ten_nguoi_bl) > 20:
                name_item.setToolTip(ten_nguoi_bl)  # Hiển thị đầy đủ khi hover
            comment_table.setItem(row, 0, name_item)
            
            # Cải thiện xem nội dung comment
            noi_dung = comment.get("noi_dung", "")
            noi_dung_short = noi_dung[:100] + "..." if len(noi_dung) > 100 else noi_dung
            content_item = QTableWidgetItem(noi_dung_short)
            content_item.setToolTip(noi_dung)  # Hiển thị đầy đủ khi hover
            comment_table.setItem(row, 1, content_item)
                
            sentiment = comment.get("sentiment", "neutral")
            sentiment_item = QTableWidgetItem(sentiment)
            
            # Tô màu theo sentiment
            if sentiment == "positive":
                sentiment_item.setBackground(QColor(200, 255, 200))  # Xanh nhạt
            elif sentiment == "negative":
                sentiment_item.setBackground(QColor(255, 200, 200))  # Đỏ nhạt
            
            comment_table.setItem(row, 2, sentiment_item)
            comment_table.setItem(row, 3, QTableWidgetItem(f"{comment.get('sentiment_score', 0.5):.2f}"))
        
        layout.addWidget(comment_table)
        
        # Thêm nút đóng
        close_button = QPushButton("Đóng")
        close_button.clicked.connect(dialog.accept)
        layout.addWidget(close_button)
        
        dialog.exec()

    
    def set_selected_comics(self, selected_comics):
        """
        Thiết lập danh sách truyện đã chọn để phân tích
        
        Args:
            selected_comics: Danh sách truyện đã chọn
        """
        self.selected_comics = selected_comics
        self.analyze_button.setEnabled(len(selected_comics) > 0)
        
        # Reset trạng thái
        self.progress_bar.setValue(0)
        # self.export_button.setEnabled(False)
        self.result_table.setRowCount(0)
        self.comment_table.setRowCount(0)
        
        # Cập nhật thông tin
        self.info_label.setText(f"Đã chọn {len(selected_comics)} truyện để phân tích")
        
        logger.info(f"Đã chọn {len(selected_comics)} truyện để phân tích")
    
    def start_analysis(self):
        """Bắt đầu quá trình phân tích"""
        if self.is_analyzing:
            logger.warning("Đang trong quá trình phân tích, vui lòng đợi")
            return
            
        if not self.selected_comics:
            logger.warning("Chưa có truyện nào được chọn để phân tích")
            return
        
        # Cập nhật trạng thái
        self.is_analyzing = True
        self.analyze_button.setEnabled(False)
        # self.export_button.setEnabled(False)
        self.progress_bar.setValue(0)
        
        # Khởi tạo SentimentAnalyzer (lazy loading)
        if not self.sentiment_analyzer:
            logger.info("Khởi tạo SentimentAnalyzer...")
            self.sentiment_analyzer = SentimentAnalyzer()
        
        # Tạo worker để chạy phân tích trong thread riêng
        worker = Worker(self.analyze_comics)
        worker.signals.progress.connect(self.update_progress)
        worker.signals.result.connect(self.on_analysis_complete)
        worker.signals.error.connect(self.on_analysis_error)
        
        # Bắt đầu phân tích
        logger.info(f"Bắt đầu phân tích {len(self.selected_comics)} truyện")
        QThreadPool.globalInstance().start(worker)
    
    def apply_sorting(self):
        """Áp dụng sắp xếp theo trường đã chọn"""
        field_index = self.sort_field_combo.currentIndex()
        
        # Chuyển đổi index combo sang index cột thực tế
        if field_index == 0:  # Điểm tổng hợp
            column = 10
        elif field_index == 1:  # Điểm cơ bản
            column = 8
        elif field_index == 2:  # Điểm sentiment
            column = 9
        elif field_index == 3:  # Lượt xem
            column = 5
        elif field_index == 4:  # Số chương
            column = 4
        else:
            column = 10
    
        # Xác định thứ tự sắp xếp
        order = Qt.SortOrder.DescendingOrder if self.sort_order_combo.currentIndex() == 0 else Qt.SortOrder.AscendingOrder
    
        # Áp dụng sắp xếp
        self.result_table.sortByColumn(column, order)
    
    def analyze_comics(self, progress_callback):
        """
        Phân tích dữ liệu cho tất cả truyện đã chọn với xử lý batch
        """
        BATCH_SIZE = 1
        
        try:
            results = []
            total_comics = len(self.selected_comics)
            processed_count = 0
            start_time = time.time()
            
            # Phân chia truyện thành các batch
            for batch_start in range(0, total_comics, BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, total_comics)
                current_batch = self.selected_comics[batch_start:batch_end]
                
                logger.info(f"\n{'='*50}")
                logger.info(f"Đang xử lý batch {batch_start//BATCH_SIZE + 1}/{(total_comics-1)//BATCH_SIZE + 1}")
                logger.info(f"Batch truyện {batch_start+1} đến {batch_end}/{total_comics}")
                
                # Xử lý batch hiện tại
                batch_results = self.process_comic_batch(
                    current_batch,
                    processed_count,
                    total_comics,
                    progress_callback
                )
                
                results.extend(batch_results)
                processed_count += len(current_batch)
                
                # Log thông tin về batch
                batch_time = time.time() - start_time
                avg_time_per_comic = batch_time / (processed_count or 1)
                remaining_comics = total_comics - processed_count
                estimated_remaining_time = avg_time_per_comic * remaining_comics
                
                logger.info(f"Tiến độ: {processed_count}/{total_comics} truyện")
                logger.info(f"Thời gian xử lý trung bình/truyện: {avg_time_per_comic:.2f} giây")
                logger.info(f"Ước tính thời gian còn lại: {estimated_remaining_time:.2f} giây")
                
                # Dọn dẹp tài nguyên sau mỗi batch
                self.cleanup_batch_resources()
                
                # Tạm dừng ngắn giữa các batch để tránh quá tải
                time.sleep(2)
            
            # Sắp xếp kết quả cuối cùng
            results.sort(key=lambda x: x.get("comprehensive_rating", 0), reverse=True)
            return results
            
        except Exception as e:
            logger.error(f"Lỗi trong quá trình phân tích: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def process_comic_batch(self, comics_batch, processed_count, total_comics, progress_callback):
        """Xử lý một batch truyện"""
        batch_results = []
        
        time_limit = None
        days_limit = None
        if self.limit_checkbox.isChecked():
            days_limit = self.limit_spinbox.value()
            time_limit = datetime.now() - timedelta(days=days_limit)
            logger.info(f"Giới hạn crawl comment {days_limit} ngày gần đây")
        
        for i, comic in enumerate(comics_batch):
            try:
                comic_id = comic.get("id")
                logger.info(f"\n[{processed_count + i + 1}/{total_comics}] "
                        f"Đang phân tích: {comic['ten_truyen']}")
                
                # Debug info
                logger.info(f"Link: {comic.get('link_truyen', 'N/A')}")
                # logger.info(f"Nguồn: {comic.get('nguon', 'N/A')}")
                
                # Khởi tạo crawler cho nguồn dữ liệu
                nguon = comic.get("nguon", "TruyenQQ")
                crawler = self.crawler_factory.create_crawler(
                    nguon,
                    self.db_manager,
                    self.config_manager
                )
                
                # Crawl và xử lý comments
                start_time = time.time()
                comments = crawler.crawl_comments(comic, time_limit=time_limit, days_limit=days_limit)
                crawl_time = time.time() - start_time
                
                logger.info(f"Đã crawl được {len(comments)} comment trong {crawl_time:.2f} giây")
                
                if not comments:
                    logger.warning(f"Không tìm thấy comment cho truyện {comic['ten_truyen']}")
                    batch_results.append(self.create_basic_result(comic))
                    continue
                
                # Phân tích sentiment cho comments
                sentiment_stats = {"positive": 0, "negative": 0, "neutral": 0}
                processed_comments = []
                
                start_time = time.time()
                for comment in comments:
                    try:
                        content = comment.get("noi_dung", "").strip()
                        if content and len(content) > 3:
                            sentiment_result = self.sentiment_analyzer.analyze(content)
                            comment["sentiment"] = sentiment_result["sentiment"]
                            comment["sentiment_score"] = sentiment_result["score"]
                            sentiment_stats[sentiment_result["sentiment"]] += 1
                        else:
                            comment["sentiment"] = "neutral"
                            comment["sentiment_score"] = 0.5
                            sentiment_stats["neutral"] += 1
                    except Exception as e:
                        logger.error(f"Lỗi khi phân tích sentiment: {str(e)}")
                        comment["sentiment"] = "neutral"
                        comment["sentiment_score"] = 0.5
                        sentiment_stats["neutral"] += 1
                    
                    processed_comments.append(comment)
                
                sentiment_time = time.time() - start_time
                logger.info(f"Phân tích sentiment hoàn tất trong {sentiment_time:.2f} giây")
                
                # Lưu comments đã xử lý
                self.db_manager.save_comments(comic_id, processed_comments)
                
                # Tính toán điểm số
                rating_calculator = RatingFactory.get_calculator(nguon)
                base_rating = rating_calculator.calculate(comic)
                
                # Tính điểm sentiment
                sentiment_rating = self.calculate_sentiment_rating(
                    sentiment_stats["positive"],
                    sentiment_stats["negative"],
                    sentiment_stats["neutral"]
                )
                
                # Tính điểm tổng hợp
                comprehensive_rating = base_rating * 0.6 + sentiment_rating * 0.4
                
                # Tạo kết quả cho truyện
                comic_result = {
                    **comic.copy(),
                    "base_rating": base_rating,
                    "sentiment_rating": sentiment_rating,
                    "comprehensive_rating": comprehensive_rating,
                    "comments": processed_comments,
                    "positive_count": sentiment_stats["positive"],
                    "negative_count": sentiment_stats["negative"],
                    "neutral_count": sentiment_stats["neutral"]
                }
                
                batch_results.append(comic_result)
                
                # Log kết quả
                logger.info(
                    f"Kết quả phân tích sentiment: "
                    f"Positive={sentiment_stats['positive']}, "
                    f"Negative={sentiment_stats['negative']}, "
                    f"Neutral={sentiment_stats['neutral']}"
                )
                logger.info(
                    f"Điểm số: Base={base_rating:.2f}, "
                    f"Sentiment={sentiment_rating:.2f}, "
                    f"Tổng hợp={comprehensive_rating:.2f}"
                )
                
            except Exception as e:
                logger.error(f"Lỗi khi xử lý truyện {comic.get('ten_truyen')}: {str(e)}")
                batch_results.append(self.create_error_result(comic))
            
            # Cập nhật tiến độ
            progress = ((processed_count + i + 1) / total_comics) * 100
            progress_callback.emit(int(progress))
        
        return batch_results

    def calculate_sentiment_rating(self, positive, negative, neutral):
        """Tính điểm sentiment dựa trên số lượng các loại comment"""
        total = positive + negative + neutral
        if total == 0:
            return 5.0
        
        positive_ratio = positive / total
        negative_ratio = negative / total
        neutral_ratio = neutral / total
        
        sentiment_rating = (positive_ratio * 8) - (negative_ratio * 5) + (neutral_ratio * 6)
        return max(0, min(10, sentiment_rating * 2))

    def create_basic_result(self, comic):
        """Tạo kết quả cơ bản khi không có comment"""
        rating_calculator = RatingFactory.get_calculator(comic.get('nguon', 'TruyenQQ'))
        base_rating = rating_calculator.calculate(comic)
        return {
            **comic.copy(),
            "base_rating": base_rating,
            "sentiment_rating": 5.0,
            "comprehensive_rating": base_rating * 0.6 + 5.0 * 0.4,
            "comments": [],
            "positive_count": 0,
            "negative_count": 0,
            "neutral_count": 0
        }

    def create_error_result(self, comic):
        """Tạo kết quả khi có lỗi"""
        rating_calculator = RatingFactory.get_calculator(comic.get('nguon', 'TruyenQQ'))
        base_rating = rating_calculator.calculate(comic)
        return {
            **comic.copy(),
            "base_rating": base_rating,
            "sentiment_rating": 0.0,
            "comprehensive_rating": base_rating * 0.6,
            "comments": [],
            "positive_count": 0,
            "negative_count": 0,
            "neutral_count": 0,
            "error": "Lỗi khi xử lý truyện này"
        }

    def cleanup_batch_resources(self):
        """Dọn dẹp tài nguyên sau khi xử lý batch"""
        gc.collect()
    
    @pyqtSlot(int)
    def update_progress(self, progress):
        """Cập nhật thanh tiến trình"""
        self.progress_bar.setValue(progress)
    
    @pyqtSlot(object)
    def on_analysis_complete(self, results):
        """Xử lý khi phân tích hoàn tất"""
        self.is_analyzing = False
        self.analyze_button.setEnabled(True)
        # self.export_button.setEnabled(True)
        
        # Lưu kết quả cho việc xuất file sau này
        self.analysis_results = results
        
        # Tạm thời tắt tính năng sắp xếp
        self.result_table.setSortingEnabled(False)
        
        # Hiển thị kết quả trong bảng
        self.result_table.setRowCount(0)
        for i, result in enumerate(results):
            row = self.result_table.rowCount()
            self.result_table.insertRow(row)
            
            self.result_table.setItem(row, 0, QTableWidgetItem(str(i + 1)))
            self.result_table.setItem(row, 1, QTableWidgetItem(result["ten_truyen"]))
            self.result_table.setItem(row, 2, QTableWidgetItem(result.get("nguon", "N/A")))
            
            # Mô tả - hiện chỉ một phần nhỏ
            mo_ta_full = result.get("mo_ta", "")
            mo_ta = mo_ta_full[:100] + "..." if len(mo_ta_full) > 100 else mo_ta_full
            self.result_table.setItem(row, 3, QTableWidgetItem(mo_ta))
            
            # Thông tin số lượng
            so_chuong_item = QTableWidgetItem()
            so_chuong_item.setData(Qt.ItemDataRole.DisplayRole, int(result.get("so_chuong", 0)))
            self.result_table.setItem(row, 4, so_chuong_item)
            
            luot_xem_item = QTableWidgetItem()
            luot_xem_item.setData(Qt.ItemDataRole.DisplayRole, int(result.get("luot_xem", 0)))
            self.result_table.setItem(row, 5, luot_xem_item)
            
            # Tùy chỉnh theo nguồn
            if result.get("nguon") == "TruyenQQ":
                self.result_table.setItem(row, 6, QTableWidgetItem("N/A"))
                self.result_table.setItem(row, 7, QTableWidgetItem("N/A"))
            elif result.get("nguon") == "NetTruyen":
                self.result_table.setItem(row, 6, QTableWidgetItem(result.get("rating", "N/A")))
                
                danh_gia_item = QTableWidgetItem()
                danh_gia_item.setData(Qt.ItemDataRole.DisplayRole, int(result.get("luot_danh_gia", 0)))
                self.result_table.setItem(row, 7, danh_gia_item)
            elif result.get("nguon") == "Manhuavn":
                self.result_table.setItem(row, 6, QTableWidgetItem(result.get("danh_gia", "N/A")))
                
                danh_gia_item = QTableWidgetItem()
                danh_gia_item.setData(Qt.ItemDataRole.DisplayRole, int(result.get("luot_danh_gia", 0)))
                self.result_table.setItem(row, 7, danh_gia_item)
            
            # Thông tin điểm số
            base_rating_item = QTableWidgetItem()
            base_rating_item.setData(Qt.ItemDataRole.DisplayRole, float(result.get("base_rating", 0)))
            self.result_table.setItem(row, 8, base_rating_item)
            
            sentiment_rating_item = QTableWidgetItem()
            sentiment_rating_item.setData(Qt.ItemDataRole.DisplayRole, float(result.get("sentiment_rating", 0)))
            self.result_table.setItem(row, 9, sentiment_rating_item)
            
            comprehensive_rating_item = QTableWidgetItem()
            comprehensive_rating_item.setData(Qt.ItemDataRole.DisplayRole, float(result.get("comprehensive_rating", 0)))
            self.result_table.setItem(row, 10, comprehensive_rating_item)
            
            if i % 2 == 1:  
                for col in range(11):
                    item = self.result_table.item(row, col)
                    item.setBackground(QColor(240, 240, 240)) 
        
        self.load_history_data()
        # Bật lại tính năng sắp xếp
        self.result_table.setSortingEnabled(True)
        
        self.comment_table.setRowCount(0)
        for result in results:
            comments = result.get("comments", [])
            for comment in comments:
                row = self.comment_table.rowCount()
                self.comment_table.insertRow(row)
                
                self.comment_table.setItem(row, 0, QTableWidgetItem(result.get("ten_truyen", "")))
                
                # Giới hạn tên người bình luận
                ten_nguoi_bl = comment.get("ten_nguoi_binh_luan", "N/A")
                ten_nguoi_bl_short = ten_nguoi_bl[:20] + "..." if len(ten_nguoi_bl) > 20 else ten_nguoi_bl
                name_item = QTableWidgetItem(ten_nguoi_bl_short)
                if len(ten_nguoi_bl) > 20:
                    name_item.setToolTip(ten_nguoi_bl)
                self.comment_table.setItem(row, 1, name_item)
                
                # Cải thiện xem nội dung
                noi_dung = comment.get("noi_dung", "N/A")
                noi_dung_short = noi_dung[:100] + "..." if len(noi_dung) > 100 else noi_dung
                content_item = QTableWidgetItem(noi_dung_short)
                content_item.setToolTip(noi_dung)
                self.comment_table.setItem(row, 2, content_item)
        
                sentiment = comment.get("sentiment", "neutral")
                sentiment_score = comment.get("sentiment_score", 0.5)
                
                sentiment_item = QTableWidgetItem(sentiment)
                score_item = QTableWidgetItem(f"{sentiment_score:.2f}")
                
                # Màu sắc cho các loại sentiment
                if sentiment == "positive":
                    sentiment_item.setBackground(Qt.GlobalColor.green)
                    score_item.setBackground(Qt.GlobalColor.green)
                elif sentiment == "negative":
                    sentiment_item.setBackground(Qt.GlobalColor.red)
                    score_item.setBackground(Qt.GlobalColor.red)
                
                self.comment_table.setItem(row, 3, sentiment_item)
                self.comment_table.setItem(row, 4, score_item)
        
        # Cập nhật thông tin
        self.info_label.setText(f"Đã phân tích xong {len(results)} truyện")
        
        logger.info(f"Đã hoàn thành phân tích {len(results)} truyện")
        
    @pyqtSlot(str)
    def on_analysis_error(self, error):
        """Xử lý khi có lỗi trong quá trình phân tích"""
        self.is_analyzing = False
        self.analyze_button.setEnabled(True)
        
        # Cập nhật thông tin
        self.info_label.setText(f"Lỗi khi phân tích: {error}")
        
        logger.error(f"Lỗi khi phân tích: {error}")
    
    def delete_analysis(self, comic):
        """Xóa phân tích sentiment cho một truyện"""
        reply = QMessageBox.question(
            self, "Xác nhận", 
            f"Bạn có chắc chắn muốn xóa phân tích sentiment cho truyện '{comic.get('ten_truyen', '')}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                # Đặt nguồn dữ liệu
                self.db_manager.set_source(comic.get("nguon", "TruyenQQ"))
                
                # Xóa dữ liệu phân tích sentiment từ database
                # Chú ý: chỉ xóa thông tin sentiment, không xóa comment
                self.db_manager.delete_sentiment_analysis(comic["id"])
                
                # Làm mới dữ liệu
                self.load_history_data()
                
                QMessageBox.information(
                    self, "Thành công", 
                    f"Đã xóa phân tích sentiment cho truyện '{comic.get('ten_truyen', '')}'."
                )
                
                logger.info(f"Đã xóa phân tích sentiment cho truyện: {comic.get('ten_truyen', '')}")
                
            except Exception as e:
                logger.error(f"Lỗi khi xóa phân tích sentiment: {str(e)}")
                QMessageBox.critical(
                    self, "Lỗi", 
                    f"Lỗi khi xóa phân tích sentiment: {str(e)}"
                )