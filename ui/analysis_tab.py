from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                            QTabWidget, QTableWidget, QTableWidgetItem, 
                            QPushButton, QFileDialog, QHeaderView, QProgressBar,
                            QMessageBox, QComboBox)
from PyQt6.QtCore import Qt, QThreadPool, pyqtSlot
import logging
import time
import os
import traceback
from datetime import datetime

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
        self.result_table.setColumnCount(12)
        self.result_table.setHorizontalHeaderLabels([
            "Xếp hạng", "Tên truyện", "Nguồn", "Mô tả", "Số chương", 
            "Lượt xem", "Lượt thích/theo dõi", "Rating", "Lượt đánh giá",
            "Điểm cơ bản", "Điểm sentiment", "Điểm tổng hợp"
        ])

        # Điều chỉnh header
        header = self.result_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)  # Xếp hạng
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)  # Tên truyện
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)  # Nguồn
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)  # Mô tả
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)  # Số chương
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)  # Lượt xem
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)  # Lượt thích/theo dõi
        header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)  # Rating
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.ResizeToContents)  # Lượt đánh giá
        header.setSectionResizeMode(9, QHeaderView.ResizeMode.ResizeToContents)  # Điểm cơ bản
        header.setSectionResizeMode(10, QHeaderView.ResizeMode.ResizeToContents)  # Điểm sentiment
        header.setSectionResizeMode(11, QHeaderView.ResizeMode.ResizeToContents)  # Điểm tổng hợp

        # Cho phép sắp xếp
        self.result_table.setSortingEnabled(True)
        
        # Thêm vào phần view_layout trong init_ui()

        # Controls cho bộ lọc
        filter_layout = QHBoxLayout()

        sort_label = QLabel("Sắp xếp theo:")
        self.sort_field_combo = QComboBox()
        self.sort_field_combo.addItems([
            "Điểm tổng hợp", "Điểm cơ bản", "Điểm sentiment", 
            "Lượt xem", "Lượt thích/theo dõi", "Số chương"
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
        
        self.export_button = QPushButton("Xuất kết quả ra Excel")
        self.export_button.clicked.connect(self.export_results)
        self.export_button.setEnabled(False)
        
        self.analyze_button = QPushButton("Bắt đầu phân tích")
        self.analyze_button.clicked.connect(self.start_analysis)
        self.analyze_button.setEnabled(False)
        
        control_layout.addWidget(self.analyze_button)
        control_layout.addWidget(self.export_button)
        
        main_layout.addLayout(control_layout)
    
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
        self.export_button.setEnabled(False)
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
        self.export_button.setEnabled(False)
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
            column = 11
        elif field_index == 1:  # Điểm cơ bản
            column = 9
        elif field_index == 2:  # Điểm sentiment
            column = 10
        elif field_index == 3:  # Lượt xem
            column = 5
        elif field_index == 4:  # Lượt thích/theo dõi
            column = 6
        elif field_index == 5:  # Số chương
            column = 4
        else:
            column = 11
    
        # Xác định thứ tự sắp xếp
        order = Qt.SortOrder.DescendingOrder if self.sort_order_combo.currentIndex() == 0 else Qt.SortOrder.AscendingOrder
    
        # Áp dụng sắp xếp
        self.result_table.sortByColumn(column, order)
    
    def analyze_comics(self, progress_callback):
        """
        Phân tích dữ liệu cho tất cả truyện đã chọn
        
        Args:
            progress_callback: Callback để cập nhật tiến trình
            
        Returns:
            List kết quả phân tích
        """
        try:
            results = []
            total_comics = len(self.selected_comics)
            
            for i, comic in enumerate(self.selected_comics):
                try:
                    comic_id = comic.get("id")
                    logger.info(f"[{i+1}/{total_comics}] Đang phân tích truyện: {comic['ten_truyen']}")
                    
                    # Hiển thị thông tin debug về link truyện
                    link_truyen = comic.get("link_truyen", "Không có link")
                    logger.info(f"Đường link truyện: {link_truyen}")
                    logger.info(f"Nguồn: {comic.get('nguon', 'Không xác định')}")
                    
                    # Step 1: Lấy crawler phù hợp với nguồn dữ liệu
                    nguon = comic.get("nguon", "TruyenQQ")
                    crawler = self.crawler_factory.create_crawler(
                        nguon, 
                        self.db_manager, 
                        self.config_manager
                    )
                    
                    # Step 2: Crawl comments từ trang web
                    logger.info(f"Bắt đầu crawl comment cho truyện {comic['ten_truyen']}...")
                    comments = crawler.crawl_comments(comic)
                    logger.info(f"Đã crawl được {len(comments)} comment cho truyện {comic['ten_truyen']}")
                    
                    if not comments:
                        logger.warning(f"Không tìm thấy comment nào cho truyện {comic['ten_truyen']}")
                    
                    # Step 3: Phân tích sentiment cho mỗi comment
                    logger.info(f"Bắt đầu phân tích sentiment cho {len(comments)} comment...")
                    
                    positive_count = 0
                    negative_count = 0
                    neutral_count = 0
                    
                    for comment in comments:
                        try:
                            content = comment.get("noi_dung", "")
                            if content and len(content.strip()) > 3:
                                sentiment_result = self.sentiment_analyzer.analyze(content)
                                comment["sentiment"] = sentiment_result["sentiment"]
                                comment["sentiment_score"] = sentiment_result["score"]
                                
                                # Đếm theo loại sentiment
                                if sentiment_result["sentiment"] == "positive":
                                    positive_count += 1
                                elif sentiment_result["sentiment"] == "negative":
                                    negative_count += 1
                                else:
                                    neutral_count += 1
                                    
                            else:
                                comment["sentiment"] = "neutral"
                                comment["sentiment_score"] = 0.5
                                neutral_count += 1
                                
                        except Exception as e:
                            logger.error(f"Lỗi khi phân tích sentiment: {str(e)}")
                            comment["sentiment"] = "neutral"
                            comment["sentiment_score"] = 0.5
                            neutral_count += 1
                    
                    # Lưu comment đã phân tích vào database
                    self.db_manager.save_comments(comic_id, comments)
                    
                    # Step 4: Tính các chỉ số đánh giá
                    # Lấy RatingCalculator phù hợp với nguồn dữ liệu
                    rating_calculator = RatingFactory.get_calculator(nguon)
                    
                    # Tính điểm cơ bản
                    base_rating = rating_calculator.calculate(comic)
                    
                    # Tính điểm sentiment (0-10)
                    sentiment_rating = 5.0  # Giá trị mặc định
                    if comments:
                        total = positive_count + negative_count + neutral_count
                        if total > 0:
                            positive_ratio = positive_count / total
                            negative_ratio = negative_count / total
                            neutral_ratio = neutral_count / total
                            
                            # Công thức tính điểm sentiment
                            sentiment_rating = (positive_ratio * 8) - (negative_ratio * 5) + (neutral_ratio * 6)
                            sentiment_rating = max(0, min(10, sentiment_rating * 2))
                    
                    # Tính điểm tổng hợp: 70% điểm cơ bản + 30% điểm sentiment
                    comprehensive_rating = base_rating * 0.6
                    if comments:
                        comprehensive_rating += sentiment_rating * 0.4
                    
                    # Thêm kết quả phân tích vào comic
                    comic_result = comic.copy()
                    comic_result["base_rating"] = base_rating
                    comic_result["sentiment_rating"] = sentiment_rating
                    comic_result["comprehensive_rating"] = comprehensive_rating
                    comic_result["comments"] = comments
                    comic_result["positive_count"] = positive_count
                    comic_result["negative_count"] = negative_count
                    comic_result["neutral_count"] = neutral_count
                    
                    results.append(comic_result)
                    
                    # Log thông tin phân tích
                    logger.info(f"Kết quả phân tích sentiment: Positive={positive_count}, Negative={negative_count}, Neutral={neutral_count}")
                    logger.info(f"Điểm cơ bản: {base_rating:.2f}, Điểm sentiment: {sentiment_rating:.2f}, Điểm tổng hợp: {comprehensive_rating:.2f}")
                    
                    # Cập nhật tiến trình
                    progress = ((i + 1) / total_comics) * 100
                    progress_callback.emit(int(progress))
                    
                except Exception as e:
                    logger.error(f"Lỗi khi phân tích truyện {comic.get('ten_truyen')}: {str(e)}")
                    logger.error(traceback.format_exc())
                    
                    # Vẫn thêm vào kết quả nhưng với thông tin lỗi
                    comic_result = comic.copy()
                    rating_calculator = RatingFactory.get_calculator(comic.get('nguon', 'TruyenQQ'))
                    comic_result["base_rating"] = rating_calculator.calculate(comic)
                    comic_result["sentiment_rating"] = 0.0
                    comic_result["comprehensive_rating"] = comic_result["base_rating"] * 0.6
                    comic_result["comments"] = []
                    comic_result["error"] = str(e)
                    
                    results.append(comic_result)
                    
                    # Cập nhật tiến trình
                    progress = ((i + 1) / total_comics) * 100
                    progress_callback.emit(int(progress))
            
            # Sắp xếp kết quả theo điểm tổng hợp giảm dần
            results.sort(key=lambda x: x.get("comprehensive_rating", 0), reverse=True)
            
            return results
            
        except Exception as e:
            logger.error(f"Lỗi trong quá trình phân tích: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    @pyqtSlot(int)
    def update_progress(self, progress):
        """Cập nhật thanh tiến trình"""
        self.progress_bar.setValue(progress)
    
    @pyqtSlot(object)
    def on_analysis_complete(self, results):
        """Xử lý khi phân tích hoàn tất"""
        self.is_analyzing = False
        self.analyze_button.setEnabled(True)
        self.export_button.setEnabled(True)
        
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
                like_follow = f"{result.get('luot_thich', 0)} / {result.get('luot_theo_doi', 0)}"
                self.result_table.setItem(row, 6, QTableWidgetItem(like_follow))
                self.result_table.setItem(row, 7, QTableWidgetItem("N/A"))
                self.result_table.setItem(row, 8, QTableWidgetItem("N/A"))
            elif result.get("nguon") == "NetTruyen":
                like_follow = f"{result.get('luot_thich', 0)} / {result.get('luot_theo_doi', 0)}"
                self.result_table.setItem(row, 6, QTableWidgetItem(like_follow))
                self.result_table.setItem(row, 7, QTableWidgetItem(result.get("rating", "N/A")))
                
                danh_gia_item = QTableWidgetItem()
                danh_gia_item.setData(Qt.ItemDataRole.DisplayRole, int(result.get("luot_danh_gia", 0)))
                self.result_table.setItem(row, 8, danh_gia_item)
            elif result.get("nguon") == "Manhuavn":
                follow = f"N/A / {result.get('luot_theo_doi', 0)}"
                self.result_table.setItem(row, 6, QTableWidgetItem(follow))
                self.result_table.setItem(row, 7, QTableWidgetItem(result.get("danh_gia", "N/A")))
                
                danh_gia_item = QTableWidgetItem()
                danh_gia_item.setData(Qt.ItemDataRole.DisplayRole, int(result.get("luot_danh_gia", 0)))
                self.result_table.setItem(row, 8, danh_gia_item)
            
            # Thông tin điểm số
            base_rating_item = QTableWidgetItem()
            base_rating_item.setData(Qt.ItemDataRole.DisplayRole, float(result.get("base_rating", 0)))
            self.result_table.setItem(row, 9, base_rating_item)
            
            sentiment_rating_item = QTableWidgetItem()
            sentiment_rating_item.setData(Qt.ItemDataRole.DisplayRole, float(result.get("sentiment_rating", 0)))
            self.result_table.setItem(row, 10, sentiment_rating_item)
            
            comprehensive_rating_item = QTableWidgetItem()
            comprehensive_rating_item.setData(Qt.ItemDataRole.DisplayRole, float(result.get("comprehensive_rating", 0)))
            self.result_table.setItem(row, 11, comprehensive_rating_item)
            
            # Highlight top 3
            if i < 3:
                for col in range(12):
                    item = self.result_table.item(row, col)
                    item.setBackground(Qt.GlobalColor.yellow)
        
        # Bật lại tính năng sắp xếp
        self.result_table.setSortingEnabled(True)
        
        # [phần code hiển thị comment không thay đổi...]        
        # Hiển thị chi tiết comment
        self.comment_table.setRowCount(0)
        for result in results:
            comments = result.get("comments", [])
            for comment in comments:
                row = self.comment_table.rowCount()
                self.comment_table.insertRow(row)
                
                self.comment_table.setItem(row, 0, QTableWidgetItem(result["ten_truyen"]))
                self.comment_table.setItem(row, 1, QTableWidgetItem(comment.get("ten_nguoi_binh_luan", "N/A")))
                self.comment_table.setItem(row, 2, QTableWidgetItem(comment.get("noi_dung", "N/A")))
                
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
    
    def export_results(self):
        """Xuất kết quả phân tích ra file Excel"""
        if not self.analysis_results:
            logger.warning("Không có kết quả phân tích nào để xuất")
            return
        
        # Mở dialog chọn file
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Lưu kết quả", "output/analysis_results.xlsx", "Excel Files (*.xlsx)"
        )
        
        if file_path:
            try:
                logger.info(f"Đang xuất kết quả ra file: {file_path}")
                self.db_manager.export_results_to_excel(self.analysis_results, file_path)
                logger.info("Đã xuất kết quả thành công")
                
                # Hiển thị thông báo
                QMessageBox.information(self, "Thông báo", f"Đã xuất kết quả ra file {file_path}")
            except Exception as e:
                logger.error(f"Lỗi khi xuất kết quả: {str(e)}")
                QMessageBox.critical(self, "Lỗi", f"Lỗi khi xuất kết quả: {str(e)}")