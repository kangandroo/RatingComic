import logging
import torch
import numpy as np
from transformers import AutoTokenizer, AutoModelForSequenceClassification

logger = logging.getLogger(__name__)

class SentimentAnalyzer:
    """Phân tích cảm xúc từ dữ liệu comment"""
    
    def __init__(self, model_name="cardiffnlp/twitter-xlm-roberta-base-sentiment"):
        """
        Khởi tạo Sentiment Analyzer với mô hình từ Hugging Face
        
        Args:
            model_name: Tên mô hình dùng để phân tích cảm xúc
        """
        try:
            logger.info(f"Đang tải mô hình {model_name}...")
            self.model_name = model_name
            
            # Sử dụng stub mode cho testing nếu không có GPU
            self.stub_mode = False
            
            try:
                # Tải tokenizer và model
                self.tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=False)
                self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
                
                # Kiểm tra GPU
                self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
                self.model.to(self.device)
                
                logger.info(f"Đã tải mô hình thành công! Sử dụng thiết bị: {self.device}")
            except Exception as e:
                logger.error(f"Lỗi khi tải mô hình: {e}")
                logger.warning("Chuyển sang chế độ stub cho việc test")
                self.stub_mode = True
                
        except Exception as e:
            logger.error(f"Lỗi khởi tạo SentimentAnalyzer: {e}")
            self.stub_mode = True
    
    def analyze(self, text):
        """
        Phân tích cảm xúc của một văn bản
        
        Args:
            text: Nội dung văn bản cần phân tích
            
        Returns:
            Dict chứa kết quả phân tích
        """
        if not text or len(str(text).strip()) < 3:
            return {"sentiment": "neutral", "score": 0.5}
        
        # Sử dụng stub mode nếu không thể tải mô hình
        if self.stub_mode:
            return self.analyze_by_keywords(text)
        
        try:
            # Chuẩn bị input
            inputs = self.tokenizer(str(text), return_tensors="pt", truncation=True, max_length=512)
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            # Thực hiện dự đoán
            with torch.no_grad():
                outputs = self.model(**inputs)
                logits = outputs.logits
                
            # Lấy xác suất cho từng lớp
            probabilities = torch.nn.functional.softmax(logits, dim=1)
            predicted_class = torch.argmax(logits).item()
            confidence = probabilities[0][predicted_class].item()
            
            # Chuyển sang định dạng kết quả
            sentiment_labels = ["negative", "neutral", "positive"]
            sentiment = sentiment_labels[predicted_class]
            
            # Chuyển đổi sang thang điểm 0-1
            if sentiment == "positive":
                score = 0.5 + (confidence / 2)  # 0.5-1.0
            elif sentiment == "negative":
                score = 0.5 - (confidence / 2)  # 0-0.5
            else:
                score = 0.5  # Trung tính
                
            return {
                "sentiment": sentiment,
                "score": score,
                "confidence": confidence
            }
        except Exception as e:
            logger.error(f"Lỗi khi phân tích cảm xúc: {e}")
            # Fallback sang phương pháp từ khóa trong trường hợp lỗi
            return self.analyze_by_keywords(text)
    
    def analyze_by_keywords(self, text):
        """
        Phân tích cảm xúc dựa trên từ khóa (phương pháp dự phòng)
        
        Args:
            text: Nội dung văn bản cần phân tích
            
        Returns:
            Dict chứa kết quả phân tích
        """
        text = str(text).lower()
        
        positive_words = [
            "hay", "thích", "tuyệt", "đỉnh", "vui", "xuất sắc", "chất lượng", 
            "cảm động", "yêu", "tốt", "giỏi", "thú vị", "hấp dẫn", "đẹp", 
            "tuyệt vời", "cười", "hài", "đáng xem", "ngầu", "đỉnh cao", "chất",
            "đỉnh", "ngon", "quá hay", "số 1", "đáng đọc", "đáng theo dõi",
            "hấp dẫn", "thích thú", "mê", "yêu thích", "ủng hộ"
        ]
        
        negative_words = [
            "tệ", "dở", "chán", "kém", "nhạt", "thất vọng", "buồn", "lỗi",
            "thiếu", "không hay", "không thích", "nhàm", "vớ vẩn", 
            "không đáng", "phí", "hụt hẫng", "mất thời gian", "ngu", "vô lý",
            "dở tệ", "thất vọng", "bỏ đi", "lãng phí", "kém chất lượng",
            "nhạt nhẽo", "xàm", "chán ngắt", "tệ hại"
        ]
        
        positive_score = sum(1 for word in positive_words if word in text)
        negative_score = sum(1 for word in negative_words if word in text)
        
        # Tính điểm dựa trên số từ tích cực và tiêu cực
        if positive_score > 0 or negative_score > 0:
            total = positive_score + negative_score
            raw_score = positive_score / total if total > 0 else 0.5
        else:
            raw_score = 0.5  # Trung tính nếu không có từ khóa nào
        
        # Xác định sentiment dựa trên score
        if raw_score > 0.6:
            sentiment = "positive"
        elif raw_score < 0.4:
            sentiment = "negative"
        else:
            sentiment = "neutral"
            
        return {"sentiment": sentiment, "score": raw_score}