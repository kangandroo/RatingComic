import logging
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import torch
import os

logger = logging.getLogger(__name__)

class SentimentAnalyzer:
    """
    Phân tích tình cảm từ văn bản sử dụng mô hình Transformer pre-trained
    """
    
    def __init__(self, model_name="cardiffnlp/twitter-xlm-roberta-base-sentiment", cache_dir="models"):
        """
        Khởi tạo SentimentAnalyzer
        
        Args:
            model_name: Tên mô hình huấn luyện trước (pre-trained)
            cache_dir: Thư mục cache cho mô hình
        """
        logger.info(f"Khởi tạo SentimentAnalyzer với mô hình: {model_name}")
        
        # Tạo thư mục cache nếu chưa tồn tại
        os.makedirs(cache_dir, exist_ok=True)
        
        try:
            # Sử dụng transformer pipeline cho phân tích cảm xúc
            self.tokenizer = AutoTokenizer.from_pretrained(model_name, cache_dir=cache_dir)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_name, cache_dir=cache_dir)
            self.analyzer = pipeline("sentiment-analysis", model=self.model, tokenizer=self.tokenizer)
            
            # Lưu tên mô hình để biết cách xử lý kết quả
            self.model_name = model_name
            
            logger.info("Đã khởi tạo SentimentAnalyzer thành công")
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo SentimentAnalyzer: {str(e)}")
            
            # Fallback: sử dụng phương pháp đơn giản
            logger.info("Sử dụng analyzer đơn giản làm fallback")
            self.analyzer = None
            self.model_name = "simple"
    
    def analyze(self, text):
        """
        Phân tích tình cảm của văn bản
        
        Args:
            text: Văn bản cần phân tích
            
        Returns:
            dict: Kết quả phân tích (sentiment và score)
        """
        try:
            if not text or len(text.strip()) == 0:
                return {"sentiment": "neutral", "score": 0.5}
                
            # Nếu có mô hình transformer
            if self.analyzer:
                # Giới hạn độ dài văn bản để tránh lỗi
                max_length = 512
                truncated_text = text[:max_length]
                
                # Phân tích cảm xúc
                result = self.analyzer(truncated_text)[0]
                
                # Xử lý kết quả khác nhau tùy theo mô hình
                if "cardiffnlp/twitter-xlm-roberta-base-sentiment" in self.model_name:
                    # Mô hình Twitter XLM RoBERTa có 3 nhãn: positive, neutral, negative
                    label = result["label"].lower()
                    
                    # Map nhãn mô hình Twitter XLM RoBERTa
                    mapping = {
                        "positive": "positive",
                        "neutral": "neutral", 
                        "negative": "negative"
                    }
                    sentiment = mapping.get(label, "neutral")
                    
                    return {
                        "sentiment": sentiment,
                        "score": result["score"]
                    }
                else:
                    # Xử lý cho các mô hình khác
                    label = result["label"].lower()
                    
                    # Map nhãn của mô hình sang positive/neutral/negative
                    if "pos" in label:
                        sentiment = "positive"
                    elif "neg" in label:
                        sentiment = "negative"
                    else:
                        sentiment = "neutral"
                    
                    return {
                        "sentiment": sentiment,
                        "score": result["score"]
                    }
            # else:
            #     # Phương pháp đơn giản dựa trên từ khóa nếu không có mô hình
            #     return self._simple_sentiment_analysis(text)
                
        except Exception as e:
            logger.error(f"Lỗi khi phân tích sentiment: {str(e)}")
            return {"sentiment": "neutral", "score": 0.5}
    
    # def _simple_sentiment_analysis(self, text):
    #     """
    #     Phân tích tình cảm đơn giản dựa trên từ khóa (fallback)
        
    #     Args:
    #         text: Văn bản cần phân tích
            
    #     Returns:
    #         dict: Kết quả phân tích (sentiment và score)
    #     """
    #     # Từ khóa tích cực
    #     positive_words = [
    #         "hay", "tốt", "thích", "tuyệt vời", "đỉnh", "tuyệt", "xuất sắc", "đẹp", 
    #         "hay quá", "cực hay", "siêu phẩm", "nhất", "đáng đọc", "nên đọc", "tuyệt đỉnh",
    #         "đáng xem", "nên xem", "yêu thích", "mê", "chất", "cảm động", "ấn tượng"
    #     ]
        
    #     # Từ khóa tiêu cực
    #     negative_words = [
    #         "dở", "tệ", "chán", "không hay", "tầm thường", "không thích", "kém", "nhàm chán",
    #         "thất vọng", "buồn ngủ", "vô lý", "nhảm", "rác", "phí thời gian", "lãng phí",
    #         "vứt", "bỏ đi", "không nên đọc", "xàm", "trash", "nhạt", "kém hay"
    #     ]
        
    #     # Chuyển văn bản về chữ thường
    #     text = text.lower()
        
    #     # Đếm từ khóa
    #     positive_count = sum(1 for word in positive_words if word in text)
    #     negative_count = sum(1 for word in negative_words if word in text)
        
    #     # Tính điểm
    #     if positive_count > negative_count:
    #         return {"sentiment": "positive", "score": min(0.9, 0.5 + 0.1 * positive_count)}
    #     elif negative_count > positive_count:
    #         return {"sentiment": "negative", "score": max(0, 0.5 - 0.1 * negative_count)}
    #     else:
    #         return {"sentiment": "neutral", "score": 0.5}