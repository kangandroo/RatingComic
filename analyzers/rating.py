import numpy as np

class ComicRatingCalculator:
    """Tính toán điểm đánh giá cho truyện tranh"""
    
    @staticmethod
    def calculate_comprehensive_rating(story_data, source="generic", sentiment_weight=0.4):
        """
        Tính điểm đánh giá tổng hợp cho truyện với công thức thích hợp theo nguồn
        
        Args:
            story_data: Dict chứa thông tin truyện
            source: Nguồn dữ liệu ('nettruyen', 'truyenqq', 'manhuavn')
            sentiment_weight: Trọng số cho phần sentiment (0.0-1.0)
            
        Returns:
            Dict chứa các thành phần đánh giá và điểm tổng hợp
        """
        if source == "nettruyen":
            return ComicRatingCalculator._nettruyen_rating(story_data, sentiment_weight)
        elif source == "truyenqq":
            return ComicRatingCalculator._truyenqq_rating(story_data, sentiment_weight)
        elif source == "manhuavn":
            return ComicRatingCalculator._manhuavn_rating(story_data, sentiment_weight)
        else:
            return ComicRatingCalculator._generic_rating(story_data, sentiment_weight)
    
    @staticmethod
    def _generic_rating(story_data, sentiment_weight=0.4):
        """Công thức chung cho tất cả các nguồn"""
        # Trích xuất dữ liệu
        views = story_data.get('views', 0)
        likes = story_data.get('likes', 0)
        follows = story_data.get('follows', 0)
        chapter_count = story_data.get('chapter_count', 0)
        
        # Đảm bảo giá trị hợp lệ
        views = max(0, int(views) if views else 0)
        likes = max(0, int(likes) if likes else 0)
        follows = max(0, int(follows) if follows else 0)
        chapter_count = max(0, int(chapter_count) if chapter_count else 0)
        
        # 1. Tính các chỉ số hiệu quả
        views_per_chapter = views / max(1, chapter_count)
        likes_per_chapter = likes / max(1, chapter_count)
        follows_per_chapter = follows / max(1, chapter_count)
        
        # 2. Chuẩn hóa chỉ số (0-1)
        norm_views = min(1.0, np.log10(views + 1) / np.log10(1000000))
        norm_likes = min(1.0, np.log10(likes + 1) / np.log10(20000))
        norm_follows = min(1.0, np.log10(follows + 1) / np.log10(50000))
        norm_chapters = min(1.0, np.log10(chapter_count + 1) / np.log10(500))
        
        # 3. Tính điểm thành phần
        view_score = norm_views * 3.0  # 3 điểm
        like_score = norm_likes * 3.0  # 3 điểm
        follow_score = norm_follows * 3.0  # 3 điểm
        chapter_score = norm_chapters * 1.0  # 1 điểm
        
        # 4. Điểm cơ bản
        base_rating = view_score + like_score + follow_score + chapter_score  # Tối đa 10 điểm
        
        # 5. Điểm sentiment
        sentiment_rating = story_data.get('sentiment_rating', 5.0)
        
        # 6. Điểm tổng hợp
        comment_count = story_data.get('comment_count', 0) or 0
        if comment_count > 0:
            # Điều chỉnh sentiment_weight dựa trên số lượng bình luận
            adjusted_sentiment_weight = sentiment_weight * min(1.0, comment_count / 50)
            final_rating = (base_rating * (1 - adjusted_sentiment_weight)) + (sentiment_rating * adjusted_sentiment_weight)
        else:
            final_rating = base_rating
        
        # Đảm bảo điểm nằm trong thang 0-10
        final_rating = min(10.0, max(0.0, final_rating))
        
        return {
            'views_per_chapter': views_per_chapter,
            'likes_per_chapter': likes_per_chapter,
            'follows_per_chapter': follows_per_chapter,
            'view_score': view_score,
            'like_score': like_score,
            'follow_score': follow_score,
            'chapter_score': chapter_score,
            'base_rating': base_rating,
            'sentiment_rating': sentiment_rating,
            'final_rating': final_rating
        }
    
    @staticmethod
    def _nettruyen_rating(story_data, sentiment_weight=0.4):
        """Công thức đánh giá cho NetTruyen, có tính đến rating"""
        # Trích xuất dữ liệu
        views = story_data.get('views', 0)
        follows = story_data.get('follows', 0) or story_data.get('followers', 0) or 0
        chapter_count = story_data.get('chapter_count', 0)
        
        # Xử lý rating từ chuỗi
        rating_str = story_data.get('rating', '0')
        if rating_str and '/' in str(rating_str):
            parts = str(rating_str).split('/')
            rating_value = float(parts[0]) / float(parts[1]) * 10
        elif rating_str:
            try:
                rating_value = float(rating_str)
                # Dự đoán thang điểm gốc
                if rating_value > 10:
                    rating_value = rating_value / 10
                elif rating_value <= 5:
                    rating_value = rating_value * 2
            except:
                rating_value = 5.0
        else:
            rating_value = 5.0
        
        rating_count = story_data.get('rating_count', 0) or 0
        
        # 1. Tính các chỉ số hiệu quả
        views_per_chapter = views / max(1, chapter_count)
        follows_per_chapter = follows / max(1, chapter_count)
        
        # 2. Chuẩn hóa chỉ số
        norm_views_efficiency = min(1.0, np.log10(views_per_chapter + 1) / np.log10(10000))
        norm_followers_efficiency = min(1.0, np.log10(follows_per_chapter + 1) / np.log10(1000))
        norm_views_total = min(1.0, np.log10(views + 1) / np.log10(10000))
        norm_followers_total = min(1.0, np.log10(follows + 1) / np.log10(1000))
        norm_rating_count = min(1.0, np.log10(rating_count + 1) / np.log10(1000))
        norm_chapters = min(1.0, np.log10(chapter_count + 1) / np.log10(500))
        
        # 3. Tính điểm thành phần
        view_score = (norm_views_total * 1.0) + (norm_views_efficiency * 2.0)  # 3 điểm
        follower_score = (norm_followers_total * 0.5) + (norm_followers_efficiency * 2.0)  # 2.5 điểm
        chapter_score = norm_chapters * 0.5  # 0.5 điểm
        
        # Điểm đánh giá với trọng số từ số lượng đánh giá
        rating_confidence = min(1.0, rating_count / 100) if rating_count > 0 else 0.1
        rating_score = (rating_value / 10.0) * 4.0 * rating_confidence  # 4 điểm
        
        # 4. Điểm cơ bản
        base_rating = view_score + follower_score + chapter_score + rating_score
        
        # 5. Điểm sentiment
        sentiment_rating = story_data.get('sentiment_rating', 5.0)
        
        # 6. Điểm tổng hợp
        comment_count = story_data.get('comment_count', 0) or 0
        if comment_count > 0:
            adjusted_sentiment_weight = sentiment_weight * min(1.0, comment_count / 50)
            final_rating = (base_rating * (1 - adjusted_sentiment_weight)) + (sentiment_rating * adjusted_sentiment_weight)
        else:
            final_rating = base_rating
        
        # Đảm bảo điểm nằm trong thang 0-10
        final_rating = min(10.0, max(0.0, final_rating))
        
        return {
            'views_per_chapter': views_per_chapter,
            'follows_per_chapter': follows_per_chapter,
            'view_score': view_score,
            'follower_score': follower_score,
            'chapter_score': chapter_score,
            'rating_score': rating_score,
            'base_rating': base_rating,
            'sentiment_rating': sentiment_rating,
            'final_rating': final_rating
        }
    
    @staticmethod
    def _truyenqq_rating(story_data, sentiment_weight=0.4):
        """Công thức đánh giá cho TruyenQQ, tập trung vào lượt xem, thích, theo dõi"""
        # Trích xuất chỉ số
        views = story_data.get('views', 0) or story_data.get('luot_xem', 0) or 0
        likes = story_data.get('likes', 0) or story_data.get('luot_thich', 0) or 0
        follows = story_data.get('follows', 0) or story_data.get('luot_theo_doi', 0) or 0
        chapter_count = story_data.get('chapter_count', 0) or story_data.get('so_chuong', 0) or 0
        
        # 1. Tính các chỉ số hiệu quả
        views_per_chapter = views / max(1, chapter_count)
        likes_per_chapter = likes / max(1, chapter_count)
        follows_per_chapter = follows / max(1, chapter_count)
        
        # 2. Chuẩn hóa chỉ số hiệu quả (0-1)
        norm_views_efficiency = min(1.0, np.log10(views_per_chapter + 1) / np.log10(100000))
        norm_likes_efficiency = min(1.0, np.log10(likes_per_chapter + 1) / np.log10(80))
        norm_follows_efficiency = min(1.0, np.log10(follows_per_chapter + 1) / np.log10(500))
        
        # 3. Chuẩn hóa chỉ số tổng (0-1)
        norm_views_total = min(1.0, np.log10(views + 1) / np.log10(2000000))
        norm_likes_total = min(1.0, np.log10(likes + 1) / np.log10(30000))
        norm_follows_total = min(1.0, np.log10(follows + 1) / np.log10(60000))
        norm_chapters = min(1.0, np.log10(chapter_count + 1) / np.log10(500))
        
        # 4. Tính điểm thành phần
        view_score = (norm_views_total * 1.0) + (norm_views_efficiency * 3.0)  # 4 điểm
        like_score = (norm_likes_total * 0.5) + (norm_likes_efficiency * 2.5)  # 3 điểm
        follower_score = (norm_follows_total * 0.5) + (norm_follows_efficiency * 2.5)  # 3 điểm
        chapter_score = norm_chapters * 0  # 0 điểm
        
        # 5. Điểm cơ bản
        base_rating = view_score + like_score + follower_score + chapter_score
        
        # 6. Điểm sentiment
        sentiment_rating = story_data.get('sentiment_rating', 5.0)
        
        # 7. Điểm tổng hợp
        comment_count = story_data.get('comment_count', 0) or story_data.get('so_binh_luan', 0) or 0
        if comment_count > 0:
            adjusted_sentiment_weight = sentiment_weight * min(1.0, comment_count / 50)
            final_rating = (base_rating * (1 - adjusted_sentiment_weight)) + (sentiment_rating * adjusted_sentiment_weight)
        else:
            final_rating = base_rating
            
        # Đảm bảo điểm nằm trong thang 0-10
        final_rating = min(10.0, max(0.0, final_rating))
            
        return {
            'views_per_chapter': views_per_chapter,
            'likes_per_chapter': likes_per_chapter,
            'follows_per_chapter': follows_per_chapter,
            'view_score': view_score,
            'like_score': like_score,
            'follower_score': follower_score,
            'chapter_score': chapter_score,
            'base_rating': base_rating,
            'sentiment_rating': sentiment_rating,
            'final_rating': final_rating
        }
        
    @staticmethod
    def _manhuavn_rating(story_data, sentiment_weight=0.4):
        """Công thức đánh giá cho ManhuaVN, có tính đến rating và lượt theo dõi"""
        # Trích xuất dữ liệu
        views = story_data.get('views', 0)
        follows = story_data.get('follows', 0) or story_data.get('followers', 0) or 0
        chapter_count = story_data.get('chapter_count', 0) or story_data.get('chapters', 0) or 0
        
        # Xử lý rating và rating_count
        rating_str = story_data.get('rating', 'N/A')
        rating_count = story_data.get('rating_count', 0) or 0
        
        # Xử lý rating từ chuỗi
        if rating_str != 'N/A' and '/' in str(rating_str):
            parts = str(rating_str).split('/')
            rating_value = float(parts[0]) / float(parts[1]) * 10
        elif rating_str != 'N/A':
            try:
                rating_value = float(rating_str)
                # Dự đoán thang điểm gốc
                if rating_value > 10:
                    rating_value = rating_value / 10
                elif rating_value > 5:
                    rating_value = rating_value
                else:
                    rating_value = rating_value * 2
            except:
                rating_value = 5.0
        else:
            rating_value = 5.0
        
        # 1. Tính các chỉ số hiệu quả
        views_per_chapter = views / max(1, chapter_count)
        follows_per_chapter = follows / max(1, chapter_count)
        
        # 2. Chuẩn hóa chỉ số hiệu quả (0-1)
        norm_views_efficiency = min(1.0, np.log10(views_per_chapter + 1) / np.log10(1000))
        norm_follows_efficiency = min(1.0, np.log10(follows_per_chapter + 1) / np.log10(100))
        
        # 3. Chuẩn hóa chỉ số tổng (0-1)
        norm_views_total = min(1.0, np.log10(views + 1) / np.log10(1000000))
        norm_follows_total = min(1.0, np.log10(follows + 1) / np.log10(30000))
        norm_rating_count = min(1.0, np.log10(rating_count + 1) / np.log10(1000))
        norm_chapters = min(1.0, np.log10(chapter_count + 1) / np.log10(500))
        
        # 4. Tính điểm thành phần
        view_score = (norm_views_total * 1.0) + (norm_views_efficiency * 2.5)  # 3.5 điểm
        follower_score = (norm_follows_total * 0.5) + (norm_follows_efficiency * 2.0)  # 2.5 điểm
        chapter_score = norm_chapters * 0  # 0 điểm
        
        # Điểm đánh giá với trọng số từ số lượng đánh giá
        rating_confidence = min(1.0, rating_count / 10) if rating_count > 0 else 0.1
        rating_score = (rating_value / 10.0) * 4.0 * rating_confidence  # 4 điểm
        
        # 5. Điểm cơ bản
        base_rating = view_score + follower_score + chapter_score + rating_score
        
        # 6. Điểm sentiment
        sentiment_rating = story_data.get('sentiment_rating', 5.0)
        
        # 7. Điểm tổng hợp
        comment_count = story_data.get('comment_count', 0)
        if comment_count > 0:
            adjusted_sentiment_weight = sentiment_weight * min(1.0, comment_count / 50)
            final_rating = (base_rating * (1 - adjusted_sentiment_weight)) + (sentiment_rating * adjusted_sentiment_weight)
        else:
            final_rating = base_rating
            
        # Đảm bảo điểm nằm trong thang 0-10
        final_rating = min(10.0, max(0.0, final_rating))
            
        return {
            'views_per_chapter': views_per_chapter,
            'follows_per_chapter': follows_per_chapter,
            'view_score': view_score,
            'follower_score': follower_score,
            'chapter_score': chapter_score,
            'rating_score': rating_score,
            'base_rating': base_rating,
            'sentiment_rating': sentiment_rating,
            'final_rating': final_rating
        }
    
    @staticmethod
    def calculate_sentiment_rating(positive_ratio, negative_ratio, neutral_ratio):
        """
        Tính điểm đánh giá sentiment (0-10)
        
        Args:
            positive_ratio: Tỷ lệ bình luận tích cực (0-1)
            negative_ratio: Tỷ lệ bình luận tiêu cực (0-1)
            neutral_ratio: Tỷ lệ bình luận trung tính (0-1)
            
        Returns:
            float: Điểm đánh giá sentiment (0-10)
        """
        # Công thức tính điểm sentiment: ưu tiên bình luận tích cực, penalize bình luận tiêu cực
        sentiment_score = (positive_ratio * 8) - (negative_ratio * 5) + (neutral_ratio * 6)
        # Điều chỉnh thang điểm về 0-10
        sentiment_score = min(10, max(0, sentiment_score * 2))
        return sentiment_score