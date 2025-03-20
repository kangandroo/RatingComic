import os
import time
import sqlite3
import logging
import torch
import numpy as np
import pandas as pd
import traceback
from datetime import datetime
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from tqdm import tqdm

# Thiết lập logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Đường dẫn đến database
DB_FILE = 'truyenqq_data.db'

# Tạo thư mục output với timestamp
current_time = datetime.now().strftime("%Y-%m-%d-%H%M%S")
OUTPUT_DIR = f"output_truyenqqto_{current_time}"
os.makedirs(OUTPUT_DIR, exist_ok=True)

class SentimentAnalyzer:
    def __init__(self, model_name="cardiffnlp/twitter-xlm-roberta-base-sentiment"):
        """
        Khởi tạo Sentiment Analyzer với mô hình từ Hugging Face
        """
        logger.info(f"Đang tải mô hình {model_name}...")
        self.model_name = model_name
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=False)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
            
            # Kiểm tra xem có GPU không
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            logger.info(f"Sử dụng thiết bị: {self.device}")
            self.model.to(self.device)
            
            logger.info("Đã tải mô hình thành công!")
        except Exception as e:
            logger.error(f"Lỗi khi tải mô hình: {e}")
            raise
            
    def analyze(self, text):
        """Phân tích cảm xúc của một văn bản"""
        if not text or len(str(text).strip()) < 3:
            return {"sentiment": "neutral", "score": 0.5}
        
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
                "confidence": confidence
            }
        except Exception as e:
            logger.error(f"Lỗi khi phân tích cảm xúc: {e}")
            # Fallback sang phương pháp từ khóa trong trường hợp lỗi
            return self.analyze_by_keywords(text)
    
    def analyze_batch(self, texts, batch_size=16):
        """Phân tích cảm xúc cho một danh sách văn bản"""
        results = []
        
        # Xử lý theo batch để tiết kiệm bộ nhớ
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i+batch_size]
            
            # Xử lý từng văn bản trong batch
            batch_results = [self.analyze(text) for text in batch]
            results.extend(batch_results)
            
        return results
    
    def analyze_by_keywords(self, text):
        """
        Phân tích cảm xúc dựa trên từ khóa (phương pháp dự phòng)
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

def update_database_with_sentiment(db_path=DB_FILE, batch_size=32):
    """
    Phân tích cảm xúc cho tất cả bình luận trong database và lưu kết quả
    """
    try:
        # Khởi tạo analyzer
        analyzer = SentimentAnalyzer()
        
        # Kết nối database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Kiểm tra và thêm cột sentiment nếu chưa có
        try:
            cursor.execute("ALTER TABLE comments ADD COLUMN sentiment TEXT")
            cursor.execute("ALTER TABLE comments ADD COLUMN sentiment_score REAL")
        except sqlite3.OperationalError:
            logger.info("Cột sentiment đã tồn tại hoặc có lỗi khi thêm cột")
        
        # Đếm tổng số bình luận
        cursor.execute("SELECT COUNT(*) FROM comments")
        total_comments = cursor.fetchone()[0]
        logger.info(f"Tổng số bình luận: {total_comments}")
        
        # Lấy các bình luận chưa có sentiment
        cursor.execute("SELECT id, noi_dung_binh_luan FROM comments WHERE sentiment IS NULL OR sentiment = ''")
        comments_to_analyze = cursor.fetchall()
        
        logger.info(f"Số bình luận cần phân tích: {len(comments_to_analyze)}")
        
        if not comments_to_analyze:
            logger.info("Tất cả bình luận đã được phân tích!")
            conn.close()
            return True
        
        # Xử lý theo batch
        total_batches = (len(comments_to_analyze) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, len(comments_to_analyze))
            batch = comments_to_analyze[start_idx:end_idx]
            
            logger.info(f"Đang phân tích batch {batch_idx + 1}/{total_batches}...")
            
            # Tách ID và nội dung
            ids = [item[0] for item in batch]
            contents = [item[1] for item in batch]
            
            # Phân tích cảm xúc
            results = analyzer.analyze_batch(contents)
            
            # Cập nhật database
            for i, result in enumerate(results):
                comment_id = ids[i]
                sentiment = result["sentiment"]
                score = result["score"]
                
                cursor.execute("UPDATE comments SET sentiment = ?, sentiment_score = ? WHERE id = ?", 
                              (sentiment, score, comment_id))
            
            # Commit mỗi batch
            conn.commit()
            logger.info(f"Đã cập nhật {len(batch)} bình luận vào database")
            
            # Nghỉ ngắn để không làm quá tải hệ thống
            time.sleep(0.1)
        
        conn.close()
        logger.info("Hoàn thành phân tích cảm xúc và cập nhật vào database!")
        return True
        
    except Exception as e:
        logger.error(f"Lỗi khi cập nhật database: {e}")
        traceback.print_exc()
        return False

def extract_number(text_value):
    """
    Trích xuất số từ các chuỗi với nhiều định dạng như:
    - '1,234' -> 1234
    - '2.345.737' -> 2345737
    - '5K' -> 5000
    - '3.2M' -> 3200000
    """
    try:
        # Xử lý trường hợp đã là số
        if isinstance(text_value, (int, float)):
            return int(text_value)
            
        # Xử lý giá trị trống hoặc không có
        if not text_value or text_value == 'N/A':
            return 0
            
        text_value = str(text_value).strip()
        
        # Xử lý hậu tố K và M
        if 'K' in text_value.upper():
            num_part = text_value.upper().replace('K', '')
            # Làm sạch và chuyển đổi
            if num_part.count('.') == 1:
                return int(float(num_part) * 1000)
            else:
                cleaned = num_part.replace('.', '').replace(',', '')
                return int(float(cleaned) * 1000)
            
        elif 'M' in text_value.upper():
            num_part = text_value.upper().replace('M', '')
            # Làm sạch và chuyển đổi
            if num_part.count('.') == 1:
                return int(float(num_part) * 1000000)
            else:
                cleaned = num_part.replace('.', '').replace(',', '')
                return int(float(cleaned) * 1000000)
        else:
            # Xử lý số có nhiều dấu chấm là dấu phân cách hàng nghìn
            if text_value.count('.') > 1:
                text_value = text_value.replace('.', '')
            
            # Xử lý dấu phẩy là dấu phân cách hàng nghìn
            text_value = text_value.replace(',', '')
            
            return int(float(text_value))
    except Exception as e:
        logger.error(f"Lỗi khi trích xuất số từ '{text_value}': {e}")
        return 0

def calculate_comprehensive_rating(story_data, sentiment_weight=0.4):
    """
    Tính điểm đánh giá tổng hợp cho truyện với công thức cân bằng
    """
    try:
        # Trích xuất các chỉ số cần thiết
        views = extract_number(story_data.get('luot_xem', 0))
        likes = extract_number(story_data.get('luot_thich', 0))
        followers = extract_number(story_data.get('luot_theo_doi', 0))
        chapter_count = extract_number(story_data.get('so_chuong', 0))
        
        # === CÔNG THỨC MỚI ===
        
        # 1. Tính các chỉ số hiệu quả
        views_per_chapter = views / max(1, chapter_count)  # Lượt xem/chương
        likes_per_chapter = likes / max(1, chapter_count)  # Lượt thích/chương
        followers_per_chapter = followers / max(1, chapter_count)  # Lượt theo dõi/chương
        
        # 2. Chuẩn hóa chỉ số hiệu quả (thang 0-1)
        norm_views_efficiency = min(1.0, np.log10(views_per_chapter + 1) / np.log10(100000)) if views_per_chapter > 0 else 0
        norm_likes_efficiency = min(1.0, np.log10(likes_per_chapter + 1) / np.log10(80)) if likes_per_chapter > 0 else 0
        norm_followers_efficiency = min(1.0, np.log10(followers_per_chapter + 1) / np.log10(500)) if followers_per_chapter > 0 else 0
        
        # 3. Chuẩn hóa chỉ số tổng (thang 0-1) - giữ lại nhưng giảm trọng số
        norm_views_total = min(1.0, np.log10(views + 1) / np.log10(2000000)) if views > 0 else 0
        norm_likes_total = min(1.0, np.log10(likes + 1) / np.log10(30000)) if likes > 0 else 0
        norm_followers_total = min(1.0, np.log10(followers + 1) / np.log10(60000)) if followers > 0 else 0
        
        # 4. Chuẩn hóa số chương - giảm ảnh hưởng bằng cách sử dụng logarit
        norm_chapters = min(1.0, np.log10(chapter_count + 1) / np.log10(500)) if chapter_count > 0 else 0
        
        # 5. Tính điểm từ các thành phần
        view_score = (norm_views_total * 1.0) + (norm_views_efficiency * 3.0)  # Tổng: 4 điểm, ưu tiên hiệu quả
        like_score = (norm_likes_total * 0.5) + (norm_likes_efficiency * 2.5)   # Tổng: 3 điểm, ưu tiên hiệu quả
        follower_score = (norm_followers_total * 0.5) + (norm_followers_efficiency * 2.5)  # Tổng: 3 điểm, ưu tiên hiệu quả
        chapter_score = norm_chapters * 0   # Giảm xuống 0.5 điểm
        
        # 6. Điểm cơ bản: thành phần định lượng
        base_rating = view_score + like_score + follower_score + chapter_score  # Tối đa 7.5 điểm
        
        # 7. Điểm sentiment: từ phân tích cảm xúc
        sentiment_rating = story_data.get('sentiment_rating', 5.0)
        
        # 8. Điểm tổng hợp
        comment_count = story_data.get('so_binh_luan', 0)
        if comment_count > 0:
            # Nếu có bình luận, áp dụng sentiment_weight
            final_rating = (base_rating * (1 - sentiment_weight)) + (sentiment_rating * sentiment_weight)
        else:
            # Nếu không có bình luận, chỉ dùng base_rating
            final_rating = base_rating
        
        # Đảm bảo điểm nằm trong thang 0-10
        # Điều chỉnh tỉ lệ để điểm tối đa là 10
        final_rating = min(10.0, max(0.0, final_rating * (1 / 1)))
        
        return {
            'views_per_chapter': views_per_chapter,
            'likes_per_chapter': likes_per_chapter,
            'followers_per_chapter': followers_per_chapter,
            'view_score': view_score,
            'like_score': like_score,
            'follower_score': follower_score,
            'chapter_score': chapter_score,
            'base_rating': base_rating,
            'sentiment_rating': sentiment_rating,
            'comprehensive_rating': final_rating
        }
        
    except Exception as e:
        logger.error(f"Lỗi khi tính điểm tổng hợp: {e}")
        return {
            'views_per_chapter': 0,
            'likes_per_chapter': 0,
            'followers_per_chapter': 0,
            'view_score': 0,
            'like_score': 0,
            'follower_score': 0,
            'chapter_score': 0,
            'base_rating': 0,
            'sentiment_rating': 5.0,
            'comprehensive_rating': 5.0
        }

def analyze_comments_for_stories(db_path=DB_FILE, max_comments_per_story=None):
    """
    Phân tích cảm xúc cho các truyện từ database
    
    Parameters:
    - db_path: Đường dẫn đến database
    - max_comments_per_story: Số lượng bình luận tối đa phân tích cho mỗi truyện (None để phân tích tất cả)
    
    Returns:
    - Tuple (story_ratings, comment_sentiments)
    """
    try:
        # Kết nối database
        conn = sqlite3.connect(db_path)
        
        # Đọc thông tin truyện
        stories_df = pd.read_sql_query('''
            SELECT 
                id, ten_truyen, ten_khac, tac_gia, trang_thai, 
                luot_thich, luot_theo_doi, luot_xem, mo_ta, 
                so_chuong, so_binh_luan
            FROM stories
        ''', conn)
        
        logger.info(f"Đã đọc thông tin của {len(stories_df)} truyện từ database")
        
        # Danh sách kết quả
        all_story_ratings = []
        all_comment_sentiments = []
        
        # Xử lý từng truyện
        for i, (_, story) in enumerate(tqdm(list(stories_df.iterrows()), desc="Phân tích truyện")):
            story_id = story['id']
            story_title = story['ten_truyen']
            comment_count = story['so_binh_luan']
            
            logger.info(f"[{i+1}/{len(stories_df)}] Đang xử lý truyện: {story_title} ({comment_count} bình luận)")
            
            # Bỏ qua truyện không có bình luận
            if comment_count == 0:
                logger.info(f"Bỏ qua truyện {story_title}: Không có bình luận")
                continue
            
            # Lấy bình luận cho truyện hiện tại cùng với kết quả phân tích
            comments_df = pd.read_sql_query('''
                SELECT id, ten_nguoi_binh_luan, noi_dung_binh_luan, sentiment, sentiment_score
                FROM comments
                WHERE story_id = ?
            ''', conn, params=(story_id,))
            
            # Giới hạn số lượng bình luận phân tích
            if max_comments_per_story is not None and len(comments_df) > max_comments_per_story:
                comments_to_analyze = comments_df.sample(n=max_comments_per_story, random_state=42)
            else:
                comments_to_analyze = comments_df
            
            # Xử lý các comment chưa có kết quả phân tích
            if comments_to_analyze['sentiment'].isna().any():
                logger.warning(f"Có bình luận chưa được phân tích cho truyện {story_title}!")
                continue
                
            # Chuyển đổi các giá trị sentiment thành tên viết thường phù hợp với định dạng cũ
            sentiment_map = {"Tích cực": "positive", "Tiêu cực": "negative", "Trung tính": "neutral"}
            comments_to_analyze['sentiment'] = comments_to_analyze['sentiment'].map(
                lambda x: sentiment_map.get(x, x.lower()) if isinstance(x, str) else "neutral"
            )
            
            # Tạo danh sách kết quả phân tích cảm xúc
            comment_sentiments = []
            for _, comment_row in comments_to_analyze.iterrows():
                sentiment_data = {
                    'story_id': story_id,
                    'story_title': story_title,
                    'comment_id': comment_row['id'],
                    'user_name': comment_row['ten_nguoi_binh_luan'],
                    'content': comment_row['noi_dung_binh_luan'],
                    'sentiment': comment_row['sentiment'],
                    'score': comment_row.get('sentiment_score', 0.5)
                }
                
                comment_sentiments.append(sentiment_data)
                all_comment_sentiments.append(sentiment_data)
            
            # Tính toán các chỉ số sentiment của truyện
            sentiment_counts = comments_to_analyze['sentiment'].value_counts()
            positive_count = sentiment_counts.get('positive', 0)
            negative_count = sentiment_counts.get('negative', 0)
            neutral_count = sentiment_counts.get('neutral', 0)
            total = len(comments_to_analyze)
            
            positive_ratio = positive_count / total if total > 0 else 0
            negative_ratio = negative_count / total if total > 0 else 0
            neutral_ratio = neutral_count / total if total > 0 else 0
            
            # Tính điểm sentiment trung bình
            avg_score = comments_to_analyze['sentiment_score'].mean() if 'sentiment_score' in comments_to_analyze else 0.5
            
            # Tính điểm sentiment tổng hợp
            sentiment_score = (positive_ratio * 8) - (negative_ratio * 5) + (neutral_ratio * 6)
            sentiment_score = min(10, max(0, sentiment_score * 2))
            
            # Lưu kết quả đánh giá truyện
            story_rating = {
                'id': story_id,
                'ten_truyen': story_title,
                'ten_khac': story['ten_khac'],
                'tac_gia': story['tac_gia'],
                'trang_thai': story['trang_thai'],
                'luot_xem': story['luot_xem'],
                'luot_thich': story['luot_thich'],
                'luot_theo_doi': story['luot_theo_doi'],
                'so_chuong': story['so_chuong'],
                'so_binh_luan': comment_count,
                'comments_analyzed': total,
                'positive_count': positive_count,
                'negative_count': negative_count,
                'neutral_count': neutral_count,
                'positive_ratio': positive_ratio,
                'negative_ratio': negative_ratio,
                'neutral_ratio': neutral_ratio,
                'avg_sentiment_score': avg_score,
                'sentiment_rating': sentiment_score
            }
            
            all_story_ratings.append(story_rating)
            logger.info(f"Đã hoàn thành phân tích truyện {story_title}: " +
                       f"Điểm sentiment={sentiment_score:.2f}, " +
                       f"Tỷ lệ tích cực={positive_ratio*100:.1f}%, " +
                       f"Tỷ lệ tiêu cực={negative_ratio*100:.1f}%")
        
        # Đóng kết nối DB
        conn.close()
        
        # Chuyển kết quả sang DataFrame
        story_ratings_df = pd.DataFrame(all_story_ratings)
        comment_sentiments_df = pd.DataFrame(all_comment_sentiments)
        
        return story_ratings_df, comment_sentiments_df
        
    except Exception as e:
        logger.error(f"Lỗi khi phân tích cảm xúc: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame(), pd.DataFrame()

def generate_comic_ratings(db_path=DB_FILE, max_comments_per_story=None, hide_analysis_method=True):
    """
    Tạo đánh giá tổng hợp cho các truyện
    
    Parameters:
    - db_path: Đường dẫn đến database
    - max_comments_per_story: Số lượng bình luận tối đa phân tích cho mỗi truyện (None để phân tích tất cả)
    - hide_analysis_method: Ẩn cột "Phương pháp phân tích" trong kết quả
    
    Returns:
    - Tuple (success, comic_file, sentiment_file)
    """
    try:
        logger.info("Bắt đầu quy trình đánh giá truyện...")
        
        # Tạo thư mục output với timestamp
        current_time = datetime.now().strftime("%Y-%m-%d-%H%M%S")
        output_dir = f"output_truyenqqto_{current_time}"
        os.makedirs(output_dir, exist_ok=True)
        
        # Cập nhật database trước khi bắt đầu
        update_database_with_sentiment(db_path)
        
        # Phân tích cảm xúc cho các truyện
        story_ratings_df, comment_sentiments_df = analyze_comments_for_stories(
            db_path=db_path,
            max_comments_per_story=max_comments_per_story
        )
        
        # Kiểm tra kết quả
        if story_ratings_df.empty:
            logger.error("Không có dữ liệu đánh giá truyện!")
            return False, None, None
            
        # Tính điểm đánh giá cơ bản và điểm tổng hợp cho mỗi truyện
        # Tạo một list để lưu thông tin đánh giá chi tiết
        detailed_ratings = []
        
        for _, row in story_ratings_df.iterrows():
            # Tính điểm đánh giá
            ratings = calculate_comprehensive_rating(row)
            
            # Lưu các thông số chi tiết
            detailed_ratings.append({
                'id': row['id'],
                'ten_truyen': row['ten_truyen'],
                'ten_khac': row['ten_khac'],
                'tac_gia': row['tac_gia'],
                'trang_thai': row['trang_thai'],
                'luot_xem': row['luot_xem'],
                                'luot_xem': row['luot_xem'],
                'luot_thich': row['luot_thich'],
                'luot_theo_doi': row['luot_theo_doi'],
                'so_chuong': row['so_chuong'],
                'so_binh_luan': row['so_binh_luan'],
                'comments_analyzed': row['comments_analyzed'],
                'positive_count': row['positive_count'],
                'negative_count': row['negative_count'],
                'neutral_count': row['neutral_count'],
                'positive_ratio': row['positive_ratio'],
                'negative_ratio': row['negative_ratio'],
                'neutral_ratio': row['neutral_ratio'],
                'avg_sentiment_score': row['avg_sentiment_score'],
                'sentiment_rating': row['sentiment_rating'],
                'views_per_chapter': ratings['views_per_chapter'],
                'likes_per_chapter': ratings['likes_per_chapter'],
                'followers_per_chapter': ratings['followers_per_chapter'],
                'view_score': ratings['view_score'],
                'like_score': ratings['like_score'],
                'follower_score': ratings['follower_score'],
                'chapter_score': ratings['chapter_score'],
                'base_rating': ratings['base_rating'],
                'comprehensive_rating': ratings['comprehensive_rating'],
            })
            
        # Tạo DataFrame mới với thông tin chi tiết
        detailed_df = pd.DataFrame(detailed_ratings)
        
        # Tạo DataFrame kết quả cuối cùng
        comic_ratings_df = pd.DataFrame({
            'Tên truyện': detailed_df['ten_truyen'],
            'Tên khác': detailed_df['ten_khac'],
            'Tác giả': detailed_df['tac_gia'],
            'Trạng thái': detailed_df['trang_thai'],
            'Số chương': detailed_df['so_chuong'],
            'Lượt xem': detailed_df['luot_xem'],
            'Lượt thích': detailed_df['luot_thich'],
            'Lượt theo dõi': detailed_df['luot_theo_doi'],
            'Lượt xem/chương': detailed_df['views_per_chapter'].round(1),
            'Lượt thích/chương': detailed_df['likes_per_chapter'].round(2),
            'Theo dõi/chương': detailed_df['followers_per_chapter'].round(2),
            'Số lượng bình luận': detailed_df['so_binh_luan'],
            'Bình luận đã phân tích': detailed_df['comments_analyzed'],
            'Tỷ lệ bình luận tích cực': (detailed_df['positive_ratio'] * 100).round(1),
            'Tỷ lệ bình luận tiêu cực': (detailed_df['negative_ratio'] * 100).round(1),
            'Tỷ lệ bình luận trung tính': (detailed_df['neutral_ratio'] * 100).round(1),
            'Điểm lượt xem': detailed_df['view_score'].round(2),
            'Điểm lượt thích': detailed_df['like_score'].round(2),
            'Điểm lượt theo dõi': detailed_df['follower_score'].round(2),
            'Điểm số chương': detailed_df['chapter_score'].round(2),
            'Điểm cơ bản': detailed_df['base_rating'].round(2),
            'Điểm sentiment': detailed_df['sentiment_rating'].round(2),
            'Điểm đánh giá tổng hợp': detailed_df['comprehensive_rating'].round(2)
        })
        
        # Sắp xếp theo điểm đánh giá giảm dần
        comic_ratings_df = comic_ratings_df.sort_values(by='Điểm đánh giá tổng hợp', ascending=False)
        
        # Thêm cột xếp hạng
        comic_ratings_df.insert(0, 'Xếp hạng', range(1, len(comic_ratings_df) + 1))
        
        # Tạo DataFrame cho phân tích cảm xúc chi tiết
        if not comment_sentiments_df.empty:
            sentiment_detail_df = pd.DataFrame({
                'Tên truyện': comment_sentiments_df['story_title'],
                'Người bình luận': comment_sentiments_df['user_name'],
                'Nội dung bình luận': comment_sentiments_df['content'],
                'Cảm xúc': comment_sentiments_df['sentiment'],
                'Điểm cảm xúc': comment_sentiments_df['score'].round(2)
            })
        else:
            sentiment_detail_df = pd.DataFrame(columns=[
                'Tên truyện', 'Người bình luận', 'Nội dung bình luận', 'Cảm xúc', 'Điểm cảm xúc'
            ])
        
        # Lấy tất cả bình luận từ database (để đảm bảo đầy đủ)
        try:
            conn = sqlite3.connect(db_path)
            query = '''
                SELECT 
                    c.id as 'comment_id',
                    s.ten_truyen as 'Tên truyện', 
                    c.ten_nguoi_binh_luan as 'Người bình luận', 
                    c.noi_dung_binh_luan as 'Nội dung bình luận',
                    c.sentiment as 'Cảm xúc',
                    c.sentiment_score as 'Điểm cảm xúc'
                FROM comments c
                JOIN stories s ON c.story_id = s.id
            '''
            
            all_comments_df = pd.read_sql_query(query, conn)
            conn.close()
            
            # Xử lý trường hợp sentiment có thể là NULL
            all_comments_df['Cảm xúc'] = all_comments_df['Cảm xúc'].fillna('neutral')
            # Xử lý sentiment_score có thể là NULL
            all_comments_df['Điểm cảm xúc'] = all_comments_df['Điểm cảm xúc'].fillna(0.5)
            
            # Chuẩn hóa giá trị sentiment (đổi từ Tiếng Việt sang tiếng Anh nếu cần)
            sentiment_map = {"Tích cực": "positive", "Tiêu cực": "negative", "Trung tính": "neutral"}
            all_comments_df['Cảm xúc'] = all_comments_df['Cảm xúc'].map(
                lambda x: sentiment_map.get(x, x.lower()) if isinstance(x, str) else "neutral"
            )
            
            # Loại bỏ cột comment_id
            all_comments_df = all_comments_df.drop('comment_id', axis=1)
            
            # Loại bỏ các bản ghi trùng lặp
            all_comments_df = all_comments_df.drop_duplicates()
            
            # Sử dụng all_comments_df thay cho sentiment_detail_df
            sentiment_detail_df = all_comments_df
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy toàn bộ bình luận: {e}")
        
        # Lưu kết quả vào Excel
        comic_file = os.path.join(output_dir, "comic_rating.xlsx")
        sentiment_file = os.path.join(output_dir, "sentiment_rating.xlsx")
        
        # Lưu file đánh giá truyện
        comic_ratings_df.to_excel(comic_file, sheet_name="Đánh giá truyện", index=False)
        logger.info(f"Đã lưu kết quả đánh giá truyện vào file: {comic_file}")
        
        # Lưu file phân tích cảm xúc
        sentiment_detail_df.to_excel(sentiment_file, sheet_name="Phân tích cảm xúc", index=False)
        logger.info(f"Đã lưu kết quả phân tích cảm xúc vào file: {sentiment_file}")
        
        return True, comic_file, sentiment_file
        
    except Exception as e:
        logger.error(f"Lỗi trong quá trình tạo đánh giá truyện: {e}")
        traceback.print_exc()
        return False, None, None

def check_database(db_path=DB_FILE):
    """Kiểm tra database đã sẵn sàng chưa"""
    if not os.path.exists(db_path):
        logger.error(f"Không tìm thấy database tại: {db_path}")
        return False
    
    # Kiểm tra dữ liệu trong database
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Kiểm tra bảng stories
        cursor.execute("SELECT COUNT(*) FROM stories")
        story_count = cursor.fetchone()[0]
        
        # Kiểm tra bảng comments
        cursor.execute("SELECT COUNT(*) FROM comments")
        comment_count = cursor.fetchone()[0]
        
        conn.close()
        
        if story_count == 0:
            logger.error("Không có dữ liệu truyện trong database!")
            return False
            
        logger.info(f"Database sẵn sàng với {story_count} truyện và {comment_count} bình luận")
        return True
        
    except Exception as e:
        logger.error(f"Lỗi khi kiểm tra database: {e}")
        return False

def check_model_availability():
    """Kiểm tra mô hình XLM-RoBERTa có thể tải được không"""
    try:
        # Tạo đối tượng SentimentAnalyzer tạm thời để kiểm tra
        logger.info("Kiểm tra mô hình XLM-RoBERTa...")
        analyzer = SentimentAnalyzer()
        
        # Thử phân tích một bình luận đơn giản
        test_comment = "Truyện này rất hay và thú vị"
        result = analyzer.analyze(test_comment)
        
        logger.info(f"✅ Mô hình hoạt động tốt! Kết quả phân tích: {result['sentiment']} ({result['score']:.2f})")
        return True
    except Exception as e:
        logger.error(f"❌ Lỗi khi tải mô hình: {e}")
        return False