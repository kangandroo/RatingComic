from PyQt6.QtCore import QThread, pyqtSignal
import concurrent.futures
import threading
import logging

from analysis.rating_factory import RatingFactory

logger = logging.getLogger(__name__)

class RatingCalculationThread(QThread):
    progress_updated = pyqtSignal(int)
    calculation_finished = pyqtSignal(list)
    
    def __init__(self, comics, max_workers=None):
        super().__init__()
        self.comics = comics
        self.max_workers = max_workers or min(32, len(comics))
        
        # Lấy calculators từ factory (đã được cải tiến với singleton pattern)
        self.calculators = {
            "TruyenQQ": RatingFactory.get_calculator("TruyenQQ"),
            "NetTruyen": RatingFactory.get_calculator("NetTruyen"),
            "Manhuavn": RatingFactory.get_calculator("Manhuavn")
        }
    
    def run(self):
        try:
            results = []
        
            # Sử dụng ThreadPoolExecutor để xử lý đa luồng
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Tạo future cho mỗi comic
                future_to_comic = {
                    executor.submit(self.calculate_base_rating, comic, index): (comic, index)
                    for index, comic in enumerate(self.comics)
                }
                
                # Xử lý kết quả khi hoàn thành
                completed = 0
                for future in concurrent.futures.as_completed(future_to_comic):
                    comic, index = future_to_comic[future]
                    try:
                        rating_result = future.result()
                        results.append(rating_result)
                    except Exception as e:
                        logger.error(f"Lỗi khi tính rating cho truyện {comic.get('ten_truyen')}: {str(e)}")
                    
                    # Cập nhật tiến độ
                    completed += 1
                    progress = int(completed / len(self.comics) * 100)
                    self.progress_updated.emit(progress)
        
            # Sắp xếp kết quả theo thứ tự ban đầu
            results.sort(key=lambda x: x["index"])
            
            # Hoàn thành tính toán
            self.calculation_finished.emit(results)            
        except Exception as e:
            logger.error(f"Lỗi khi tính toán rating: {str(e)}")
            self.calculation_finished.emit([])
    
    def calculate_base_rating(self, comic, index):
        """Tính điểm cơ bản cho một truyện"""
        nguon = comic.get("nguon", "NetTruyen")
        calculator = self.calculators.get(nguon, self.calculators["NetTruyen"])
        
        # Tính điểm cơ bản
        base_rating = calculator.calculate(comic)
        
        # Trả về kết quả với index để có thể sắp xếp lại
        return {
            "index": index,
            "id": comic.get("id"),
            "base_rating": base_rating
        }