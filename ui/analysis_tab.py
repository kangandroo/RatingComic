from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                            QTableWidget, QTableWidgetItem, QPushButton, 
                            QTabWidget, QHeaderView, QProgressBar)
from PyQt6.QtCore import Qt, QThreadPool
import logging
import os
from datetime import datetime

from utils.worker import Worker
from crawlers.crawler_factory import CrawlerFactory
from analysis.sentiment_analyzer import SentimentAnalyzer

logger = logging.getLogger(__name__)

class DetailAnalysisTab(QWidget):
    def __init__(self, db_manager, log_widget):
        super().__init__()
        self.db_manager = db_manager
        self.log_widget = log_widget
        self.selected_comics = []
        self.analysis_results = []
        self.is_analyzing = False
        
        # Thiết lập UI
        self.init_ui()
    
    def init_ui(self):
        # Layout chính
        main_layout = QVBoxLayout(self)
        
        # Tab widget để phân chia các view
        self.tabs = QTabWidget()
        
        # Tab tổng quan
        self.overview_tab = QWidget()
        overview_layout = QVBoxLayout(self.overview_tab)
        
        # Label thông tin
        self.info_label = QLabel("Chưa có truyện nào được chọn để phân tích")
        overview_layout.addWidget(self.info_label)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        overview_layout.addWidget(self.progress_bar)
        
        # Bảng kết quả
        self.results_table = QTableWidget()
        self.results_table.setColumnCount(11)
        self.results_table.setHorizontalHeaderLabels([
            "Xếp hạng", "Tên truyện", "Tác giả", "Thể loại", 
            "Số chương", "Lượt xem", "Lượt thích", 
            "Lượt theo dõi", "Điểm cơ bản", "Điểm sentiment", "Điểm tổng hợp"
        ])
        
        # Điều chỉnh header
        header = self.results_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        
        overview_layout.addWidget(self.results_table)
        
        # Tab chi tiết comment
        self.comment_tab = QWidget()
        comment_layout = QVBoxLayout(self.comment_tab)
        
        # Bảng chi tiết comment
        self.comment_table = QTableWidget()
        self.comment_table.setColumnCount(5)
        self.comment_table.setHorizontalHeaderLabels([
            "Tên truyện", "Người bình luận", "Nội dung bình luận", 
            "Cảm xúc", "Điểm cảm xúc"
        ])
        
        # Điều chỉnh header
        header = self.comment_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        
        comment_layout.addWidget(self.comment_table)
        
        # Thêm các tab vào TabWidget
        self.tabs.addTab(self.overview_tab, "Tổng quan")
        self.tabs.addTab(self.comment_tab, "Chi tiết bình luận")
        
        # Thêm TabWidget vào layout chính
        main_layout.addWidget(self.tabs)
        
        # Controls dưới cùng
        bottom_layout = QHBoxLayout()
        
        # Nút bắt đầu phân tích
        self.analyze_button = QPushButton("Bắt đầu phân tích")
        self.analyze_button.clicked.connect(self.start_analysis)
        self.analyze_button.setEnabled(False)
        
        # Nút xuất Excel
        self.export_button = QPushButton("Xuất kết quả Excel")
        self.export_button.clicked.connect(self.export_results)
        self.export_button.setEnabled(False)
        
        # Thêm vào layout
        bottom_layout.addStretch()
        bottom_layout.addWidget(self.analyze_button)
        bottom_layout.addWidget(self.export_button)
        
        main_layout.addLayout(bottom_layout)
    
    def set_selected_comics(self, comics):
        """Cập nhật danh sách truyện đã chọn"""
        self.selected_comics = comics
        
        # Cập nhật thông tin
        self.info_label.setText(f"Có {len(comics)} truyện được chọn để phân tích")
        
        # Cập nhật trạng thái nút
        self.analyze_button.setEnabled(len(comics) > 0)
        
        # Reset thanh tiến trình
        self.progress_bar.setValue(0)
    
    def start_analysis(self):
        """Bắt đầu quá trình phân tích chi tiết"""
        if self.is_analyzing:
            logger.warning("Đang trong quá trình phân tích, vui lòng đợi")
            return
        
        if not self.selected_comics:
            logger.warning("Chưa có truyện nào được chọn để phân tích")
            return
        
        # Cập nhật UI
        self.is_analyzing = True
        self.analyze_button.setEnabled(False)
        self.export_button.setEnabled(False)
        self.progress_bar.setValue(0)
        
        logger.info(f"Bắt đầu phân tích chi tiết {len(self.selected_comics)} truyện")
        
        # Tạo worker để chạy trong thread riêng
        worker = Worker(self.analyze_comics)
        worker.signals.progress.connect(self.update_progress)
        worker.signals.result.connect(self.on_analysis_finished)
        worker.signals.error.connect(self.on_analysis_error)
        
        # Chạy worker
        QThreadPool.globalInstance().start(worker)
    
    def analyze_comics(self, progress_callback):
        """Thực hiện phân tích chi tiết từng truyện"""
        results = []
        total_comics = len(self.selected_comics)
        
        # Khởi tạo sentiment analyzer
        sentiment_analyzer = SentimentAnalyzer()
        
        for i, comic in enumerate(self.selected_comics):
            logger.info(f"Đang phân tích truyện {i+1}/{total_comics}: {comic['ten_truyen']}")
            
            # Cập nhật tiến trình
            progress = (i / total_comics) * 100
            progress_callback.emit(progress)
            
            try:
                # Lấy crawler phù hợp
                crawler = CrawlerFactory.create_crawler(
                    comic.get('nguon', 'TruyenQQ'), 
                    self.db_manager
                )
                
                # Crawl comment cho truyện này
                logger.info(f"Đang crawl comment cho truyện: {comic['ten_truyen']}")
                comments = crawler.crawl_comments(comic)
                
                logger.info(f"Đã crawl được {len(comments)} comment")
                
                # Phân tích sentiment cho các comment
                if comments:
                    logger.info("Đang phân tích sentiment...")
                    
                    # Phân tích từng comment
                    for comment in comments:
                        sentiment_result = sentiment_analyzer.analyze(comment['noi_dung'])
                        comment['sentiment'] = sentiment_result['sentiment']
                        comment['sentiment_score'] = sentiment_result['score']
                    
                    # Tính điểm sentiment tổng hợp
                    positive = len([c for c in comments if c['sentiment'] == 'positive'])
                    negative = len([c for c in comments if c['sentiment'] == 'negative'])
                    neutral = len([c for c in comments if c['sentiment'] == 'neutral'])
                    
                    total = len(comments)
                    positive_ratio = positive / total if total > 0 else 0
                    negative_ratio = negative / total if total > 0 else 0
                    
                    # Công thức tính điểm sentiment (0-10)
                    sentiment_rating = (positive_ratio * 10) - (negative_ratio * 5)
                    sentiment_rating = max(0, min(10, sentiment_rating))
                    
                else:
                    sentiment_rating = 5  # Điểm mặc định nếu không có comment
                    
                # Lưu comment vào database
                self.db_manager.save_comments(comic['id'], comments)
                
                # Cập nhật giá trị sentiment vào comic
                comic['sentiment_rating'] = sentiment_rating
                comic['comments'] = comments
                
                # Tính điểm tổng hợp
                base_rating = comic.get('base_rating', 5)
                comprehensive_rating = (base_rating * 0.6) + (sentiment_rating * 0.4)
                
                comic['comprehensive_rating'] = comprehensive_rating
                
                # Thêm vào kết quả
                results.append(comic)
                
                logger.info(f"Đã hoàn thành phân tích truyện: {comic['ten_truyen']}")
                logger.info(f"Điểm tổng hợp: {comprehensive_rating:.2f}")
                
            except Exception as e:
                logger.error(f"Lỗi khi phân tích truyện {comic['ten_truyen']}: {str(e)}")
        
        # Cập nhật tiến trình cuối cùng
        progress_callback.emit(100)
        
        # Sắp xếp kết quả theo điểm tổng hợp giảm dần
        results.sort(key=lambda x: x.get('comprehensive_rating', 0), reverse=True)
        
        return results
    
    def update_progress(self, progress):
        """Cập nhật thanh tiến trình"""
        self.progress_bar.setValue(int(progress))
    
    def on_analysis_finished(self, results):
        """Xử lý khi phân tích hoàn tất"""
        self.is_analyzing = False
        self.analyze_button.setEnabled(True)
        
        # Lưu kết quả
        self.analysis_results = results
        
        # Hiển thị kết quả
        self.display_results()
        
        # Cập nhật UI
        self.export_button.setEnabled(True)
        
        logger.info(f"Đã hoàn thành phân tích {len(results)} truyện")
    
    def on_analysis_error(self, error):
        """Xử lý khi có lỗi xảy ra"""
        self.is_analyzing = False
        self.analyze_button.setEnabled(True)
        
        logger.error(f"Lỗi khi phân tích: {error}")
    
    def display_results(self):
        """Hiển thị kết quả phân tích"""
        if not self.analysis_results:
            return
        
        # Hiển thị bảng xếp hạng
        self.display_ranking_table()
        
        # Hiển thị chi tiết comment
        self.display_comment_table()
    
    def display_ranking_table(self):
        """Hiển thị bảng xếp hạng tổng hợp"""
        # Xóa tất cả hàng
        self.results_table.setRowCount(0)
        
        # Thêm dữ liệu
        for i, comic in enumerate(self.analysis_results):
            self.results_table.insertRow(i)
            
            # Xếp hạng
            self.results_table.setItem(i, 0, QTableWidgetItem(str(i + 1)))
            
            # Thông tin truyện
            self.results_table.setItem(i, 1, QTableWidgetItem(comic["ten_truyen"]))
            self.results_table.setItem(i, 2, QTableWidgetItem(comic["tac_gia"]))
            self.results_table.setItem(i, 3, QTableWidgetItem(comic["the_loai"]))
            self.results_table.setItem(i, 4, QTableWidgetItem(str(comic["so_chuong"])))
            self.results_table.setItem(i, 5, QTableWidgetItem(str(comic["luot_xem"])))
            self.results_table.setItem(i, 6, QTableWidgetItem(str(comic["luot_thich"])))
            self.results_table.setItem(i, 7, QTableWidgetItem(str(comic["luot_theo_doi"])))
            
            # Điểm đánh giá
            base_rating = comic.get("base_rating", 0)
            sentiment_rating = comic.get("sentiment_rating", 0)
            comprehensive_rating = comic.get("comprehensive_rating", 0)
            
            self.results_table.setItem(i, 8, QTableWidgetItem(f"{base_rating:.2f}"))
            self.results_table.setItem(i, 9, QTableWidgetItem(f"{sentiment_rating:.2f}"))
            self.results_table.setItem(i, 10, QTableWidgetItem(f"{comprehensive_rating:.2f}"))
            
            # Style cho xếp hạng
            if i < 3:  # Top 3
                for col in range(11):
                    item = self.results_table.item(i, col)
                    item.setBackground(Qt.GlobalColor.yellow)
    
    def display_comment_table(self):
        """Hiển thị bảng chi tiết comment"""
        # Xóa tất cả hàng
        self.comment_table.setRowCount(0)
        
        # Tạo danh sách tất cả comment
        all_comments = []
        
        for comic in self.analysis_results:
            comments = comic.get("comments", [])
            
            for comment in comments:
                all_comments.append({
                    "ten_truyen": comic["ten_truyen"],
                    "nguoi_binh_luan": comment["ten_nguoi_binh_luan"],
                    "noi_dung": comment["noi_dung"],
                    "sentiment": comment["sentiment"],
                    "score": comment["sentiment_score"]
                })
        
        # Thêm dữ liệu
        for i, comment in enumerate(all_comments):
            self.comment_table.insertRow(i)
            
            self.comment_table.setItem(i, 0, QTableWidgetItem(comment["ten_truyen"]))
            self.comment_table.setItem(i, 1, QTableWidgetItem(comment["nguoi_binh_luan"]))
            self.comment_table.setItem(i, 2, QTableWidgetItem(comment["noi_dung"]))
            self.comment_table.setItem(i, 3, QTableWidgetItem(comment["sentiment"]))
            self.comment_table.setItem(i, 4, QTableWidgetItem(f"{comment['score']:.2f}"))
            
            # Style cho sentiment
            sentiment_item = self.comment_table.item(i, 3)
            score_item = self.comment_table.item(i, 4)
            
            if comment["sentiment"] == "positive":
                sentiment_item.setBackground(Qt.GlobalColor.green)
                score_item.setBackground(Qt.GlobalColor.green)
            elif comment["sentiment"] == "negative":
                sentiment_item.setBackground(Qt.GlobalColor.red)
                score_item.setBackground(Qt.GlobalColor.red)
    
    def export_results(self):
        """Xuất kết quả ra file Excel"""
        if not self.analysis_results:
            logger.warning("Chưa có kết quả để xuất")
            return
        
        try:
            # Tạo thư mục output nếu chưa có
            os.makedirs("output", exist_ok=True)
            
            # Tạo tên file với timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"output/comic_analysis_{timestamp}.xlsx"
            
            # Xuất ra Excel
            self.db_manager.export_results_to_excel(
                self.analysis_results, 
                filename
            )
            
            logger.info(f"Đã xuất kết quả ra file: {filename}")
            
        except Exception as e:
            logger.error(f"Lỗi khi xuất kết quả: {str(e)}")