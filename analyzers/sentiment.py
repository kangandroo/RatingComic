import torch
import re
from transformers import AutoTokenizer, AutoModelForSequenceClassification

class SentimentAnalyzer:
    """Phân tích cảm xúc từ bình luận tiếng Việt"""
    
    def __init__(self, model_name="cardiffnlp/twitter-xlm-roberta-base-sentiment", logger=None):
        """Khởi tạo Sentiment Analyzer với mô hình từ Hugging Face"""
        self.logger = logger
        self.model_name = model_name
        self.log("Đang tải mô hình sentiment analysis...")
        
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=False)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
            
            # Kiểm tra GPU
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            self.log(f"Sử dụng thiết bị: {self.device}")
            self.model.to(self.device)
            
            # Thiết lập từ điển từ khóa cho phương pháp dự phòng
            self._setup_keyword_dictionaries()
            
            self.log("Đã tải mô hình thành công!")
        except Exception as e:
            self.log(f"Lỗi khi tải mô hình: {e}", level="error")
            # Fallback to keyword analysis
            self._setup_keyword_dictionaries()
            self.model = None
            self.tokenizer = None
    
    def log(self, message, level="info"):
        """Ghi log nếu logger được cung cấp"""
        if self.logger:
            if level == "info":
                self.logger.info(message)
            elif level == "error":
                self.logger.error(message)
            elif level == "warning":
                self.logger.warning(message)
    
    def _setup_keyword_dictionaries(self):
        """Thiết lập từ điển từ khóa cho phân tích sentiment dự phòng"""
        # Từ tích cực tiếng Việt
        self.positive_words = {
            "hay", "thích", "tuyệt", "đỉnh", "vui", "xuất sắc", "chất lượng", 
            "cảm động", "yêu", "tốt", "giỏi", "thú vị", "hấp dẫn", "đẹp", 
            "tuyệt vời", "cười", "hài", "đáng xem", "ngầu", "đỉnh cao", "chất",
            "đỉnh", "ngon", "quá hay", "số 1", "đáng đọc", "đáng theo dõi",
            "hấp dẫn", "thích thú", "mê", "yêu thích", "ủng hộ", "hóng", "mong",
            "cưng", "dễ thương", "cool", "hay quá", "quá đã", "tuyệt quá",
            "đỉnh quá", "xúc động", "ấn tượng", "đáng giá"
        }
        
        # Từ tiêu cực tiếng Việt
        self.negative_words = {
            "tệ", "dở", "chán", "kém", "nhạt", "thất vọng", "buồn", "lỗi",
            "thiếu", "không hay", "không thích", "nhàm", "vớ vẩn", 
            "không đáng", "phí", "hụt hẫng", "mất thời gian", "ngu", "vô lý",
            "dở tệ", "thất vọng", "bỏ đi", "lãng phí", "kém chất lượng",
            "nhạt nhẽo", "xàm", "chán ngắt", "tệ hại", "thất hứa", "chậm", 
            "lê thê", "rối rắm", "không hiểu", "rác rưởi", "thất hứa", "không nên xem"
        }
        
        # Từ phủ định
        self.negation_words = {
            "không", "chẳng", "đừng", "chưa", "không phải", "chả",
            "không thể", "không còn", "không được", "thiếu", "không nên",
            "đéo", "éo", "đ", "k", "ko"
        }
    
    def analyze_by_keywords(self, text):
        """Phân tích cảm xúc dựa trên từ khóa (phương pháp dự phòng)"""
        if not text or len(str(text).strip()) < 3:
            return {"sentiment": "neutral", "score": 0.5}
            
        text = str(text).lower()
        
        # Đếm từ tích cực và tiêu cực
        positive_score = sum(1 for word in self.positive_words if word in text)
        negative_score = sum(1 for word in self.negative_words if word in text)
        
        # Kiểm tra từ phủ định
        for neg_word in self.negation_words:
            if neg_word in text:
                for pos_word in self.positive_words:
                    if f"{neg_word} {pos_word}" in text or f"{neg_word}{pos_word}" in text:
                        positive_score -= 1
                        negative_score += 1
                        
                for neg_word2 in self.negative_words:
                    if f"{neg_word} {neg_word2}" in text or f"{neg_word}{neg_word2}" in text:
                        negative_score -= 1
                        positive_score += 1
        
        # Tính điểm dựa trên số từ tích cực và tiêu cực
        total = positive_score + negative_score
        if total > 0:
            sentiment_score = positive_score / total
        else:
            sentiment_score = 0.5  # Trung tính nếu không có từ khóa
        
        # Xác định sentiment dựa trên score
        if sentiment_score > 0.6:
            sentiment = "positive"
        elif sentiment_score < 0.4:
            sentiment = "negative"
        else:
            sentiment = "neutral"
            
        return {
            "sentiment": sentiment,
            "score": sentiment_score,
            "method": "keyword"
        }
    
    def analyze(self, text):
        """Phân tích cảm xúc của một văn bản"""
        if not text or len(str(text).strip()) < 3:
            return {"sentiment": "neutral", "score": 0.5, "method": "default"}
        
        # Nếu mô hình không có sẵn, sử dụng phương pháp dự phòng
        if self.model is None or self.tokenizer is None:
            return self.analyze_by_keywords(text)
        
        try:
            # Chuẩn bị input
            inputs = self.tokenizer(str(text), return_tensors="pt", truncation=True, max_length=512)
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            # Thực hiện dự đoán
            with torch.no_grad():
                outputs = self.model(**inputs)
                logits = outputs.logits
                
            # Lấy xác suất cho từng lớp (softmax)
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
                "confidence": confidence,
                "method": "model"
            }
        except Exception as e:
            self.log(f"Lỗi khi phân tích sentiment với mô hình: {e}", level="error")
            # Fallback sang phương pháp từ khóa
            return self.analyze_by_keywords(text)
    
    def analyze_batch(self, texts, batch_size=16):
        """Phân tích cảm xúc cho một danh sách văn bản"""
        results = []
        
        # Xử lý theo batch để tiết kiệm bộ nhớ
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i+batch_size]
            batch_results = [self.analyze(text) for text in batch]
            results.extend(batch_results)
            
        return results