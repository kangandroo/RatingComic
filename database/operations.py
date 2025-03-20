from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import json
import os

from db.models import db, Website, StoryIndex, AnalysisReport
from utils.logger import default_logger as logger

class DatabaseOperations:
    """Các thao tác database chung cho ứng dụng"""
    
    def __init__(self, app=None, db_manager=None):
        """Khởi tạo với app và database manager"""
        self.app = app
        self.db_manager = db_manager
        
    @staticmethod
    def add_website(name, url, api_name):
        """
        Thêm một website mới hoặc cập nhật nếu đã tồn tại
        
        Args:
            name (str): Tên website
            url (str): URL gốc
            api_name (str): Tên API (nettruyen, truyenqq, manhuavn)
            
        Returns:
            Website: Đối tượng Website đã tạo hoặc cập nhật
        """
        try:
            # Kiểm tra website đã tồn tại chưa
            website = Website.query.filter_by(api_name=api_name).first()
            
            if website:
                # Cập nhật thông tin nếu cần
                if website.name != name or website.url != url:
                    website.name = name
                    website.url = url
                    db.session.commit()
                    logger.info(f"Đã cập nhật website: {name}")
            else:
                # Tạo mới website
                website = Website(name=name, url=url, api_name=api_name)
                db.session.add(website)
                db.session.commit()
                logger.info(f"Đã thêm website mới: {name}")
                
            return website
            
        except SQLAlchemyError as e:
            db.session.rollback()
            logger.error(f"Lỗi khi thêm/cập nhật website: {str(e)}")
            return None
    
    def add_story_index(self, story_data, website_id):
        """
        Thêm một truyện mới vào chỉ mục hoặc cập nhật nếu đã tồn tại
        
        Args:
            story_data (dict): Dữ liệu cơ bản của truyện
            website_id (int): ID của website
            
        Returns:
            StoryIndex: Đối tượng StoryIndex đã tạo hoặc cập nhật
        """
        try:
            # Kiểm tra truyện đã tồn tại chưa
            story = StoryIndex.query.filter_by(url=story_data['url']).first()
            
            if story:
                # Cập nhật thông tin cơ bản
                story.title = story_data.get('title', story.title)
                story.cover_url = story_data.get('cover_url', story.cover_url)
                story.author = story_data.get('author', story.author)
                story.views = story_data.get('views', story.views)
                story.chapter_count = story_data.get('chapter_count', story.chapter_count)
                story.updated_at = datetime.utcnow()
                
                db.session.commit()
                logger.info(f"Đã cập nhật chỉ mục truyện: {story.title}")
            else:
                # Tạo mới chỉ mục truyện
                story = StoryIndex(
                    title=story_data.get('title', ''),
                    url=story_data.get('url', ''),
                    cover_url=story_data.get('cover_url', ''),
                    author=story_data.get('author', 'Đang cập nhật'),
                    views=story_data.get('views', 0),
                    chapter_count=story_data.get('chapter_count', 0),
                    website_id=website_id
                )
                
                db.session.add(story)
                db.session.commit()
                logger.info(f"Đã thêm chỉ mục truyện mới: {story.title}")
                
                # Tạo database riêng cho truyện
                self.db_manager.create_story_db(story.id)
                
            return story
            
        except SQLAlchemyError as e:
            db.session.rollback()
            logger.error(f"Lỗi khi thêm/cập nhật chỉ mục truyện: {str(e)}")
            return None
    
    def add_story_detail(self, story_id, detail_data):
        """
        Lưu thông tin chi tiết của truyện vào database riêng
        
        Args:
            story_id (int): ID của truyện trong chỉ mục
            detail_data (dict): Dữ liệu chi tiết truyện
            
        Returns:
            object: Đối tượng StoryDetail đã tạo hoặc cập nhật
        """
        try:
            with self.db_manager.story_db_session(story_id) as (session, models):
                # Kiểm tra đã có thông tin chi tiết chưa
                story_detail = session.query(models['StoryDetail']).first()
                
                if story_detail:
                    # Cập nhật thông tin
                    story_detail.title = detail_data.get('title', story_detail.title)
                    story_detail.alt_title = detail_data.get('alt_title', story_detail.alt_title)
                    story_detail.url = detail_data.get('url', story_detail.url)
                    story_detail.cover_url = detail_data.get('cover_url', story_detail.cover_url)
                    story_detail.author = detail_data.get('author', story_detail.author)
                    story_detail.status = detail_data.get('status', story_detail.status)
                    story_detail.description = detail_data.get('description', story_detail.description)
                    story_detail.views = detail_data.get('views', story_detail.views)
                    story_detail.likes = detail_data.get('likes', story_detail.likes)
                    story_detail.follows = detail_data.get('follows', story_detail.follows)
                    story_detail.chapter_count = detail_data.get('chapter_count', story_detail.chapter_count)
                    story_detail.rating = detail_data.get('rating', story_detail.rating)
                    story_detail.rating_count = detail_data.get('rating_count', story_detail.rating_count)
                    story_detail.updated_at = datetime.utcnow()
                    
                    # Để tìm kiếm nhanh, cập nhật một số thông tin trong chỉ mục
                    story_index = StoryIndex.query.get(story_id)
                    if story_index:
                        story_index.views = detail_data.get('views', story_index.views)
                        story_index.chapter_count = detail_data.get('chapter_count', story_index.chapter_count)
                        db.session.commit()
                    
                    session.commit()
                    logger.info(f"Đã cập nhật chi tiết truyện ID {story_id}")
                else:
                    # Tạo mới chi tiết truyện
                    # Lấy thông tin website từ chỉ mục
                    story_index = StoryIndex.query.get(story_id)
                    source_website = story_index.website.name if story_index and story_index.website else "Unknown"
                    
                    story_detail = models['StoryDetail'](
                        title=detail_data.get('title', ''),
                        alt_title=detail_data.get('alt_title', ''),
                        url=detail_data.get('url', ''),
                        cover_url=detail_data.get('cover_url', ''),
                        author=detail_data.get('author', 'Đang cập nhật'),
                        status=detail_data.get('status', 'Đang cập nhật'),
                        description=detail_data.get('description', ''),
                        views=detail_data.get('views', 0),
                        likes=detail_data.get('likes', 0),
                        follows=detail_data.get('follows', 0),
                        chapter_count=detail_data.get('chapter_count', 0),
                        rating=detail_data.get('rating', 0),
                        rating_count=detail_data.get('rating_count', 0),
                        source_website=source_website
                    )
                    
                    session.add(story_detail)
                    session.commit()
                    logger.info(f"Đã thêm chi tiết truyện ID {story_id}")
                
                return story_detail
                
        except Exception as e:
            logger.error(f"Lỗi khi thêm/cập nhật chi tiết truyện: {str(e)}")
            return None
    
    def add_comments(self, story_id, comments_data):
        """
        Thêm nhiều bình luận cho một truyện
        
        Args:
            story_id (int): ID của truyện trong chỉ mục
            comments_data (list): Danh sách dữ liệu bình luận
            
        Returns:
            int: Số lượng bình luận đã thêm
        """
        try:
            if not comments_data:
                return 0
                
            with self.db_manager.story_db_session(story_id) as (session, models):
                # Lấy nội dung của bình luận đã tồn tại để tránh trùng lặp
                existing_comments = session.query(models['Comment']).all()
                existing_contents = set(c.content for c in existing_comments)
                
                # Đếm số bình luận đã thêm
                count = 0
                
                for comment_data in comments_data:
                    content = comment_data.get('content', '')
                    if not content or content in existing_contents:
                        continue
                        
                    # Thêm vào set để tránh trùng lặp trong cùng batch
                    existing_contents.add(content)
                    
                    # Tạo bình luận mới
                    comment = models['Comment'](
                        username=comment_data.get('username', 'Ẩn danh'),
                        content=content,
                        date=comment_data.get('date', datetime.utcnow())
                    )
                    
                    session.add(comment)
                    count += 1
                
                session.commit()
                logger.info(f"Đã thêm {count} bình luận cho truyện ID {story_id}")
                return count
                
        except Exception as e:
            logger.error(f"Lỗi khi thêm bình luận: {str(e)}")
            return 0
    
    def update_story_ratings(self, story_id, ratings_data):
        """
        Cập nhật điểm đánh giá cho một truyện
        
        Args:
            story_id (int): ID của truyện trong chỉ mục
            ratings_data (dict): Dữ liệu điểm đánh giá
            
        Returns:
            object: Đối tượng Rating đã cập nhật
        """
        try:
            with self.db_manager.story_db_session(story_id) as (session, models):
                # Kiểm tra đã có đánh giá chưa
                rating = session.query(models['Rating']).first()
                
                if rating:
                    # Cập nhật thông tin
                    rating.view_score = ratings_data.get('view_score', rating.view_score)
                    rating.like_score = ratings_data.get('like_score', rating.like_score)
                    rating.follow_score = ratings_data.get('follow_score', rating.follow_score)
                    rating.chapter_score = ratings_data.get('chapter_score', rating.chapter_score)
                    rating.rating_score = ratings_data.get('rating_score', rating.rating_score)
                    rating.base_rating = ratings_data.get('base_rating', rating.base_rating)
                    rating.sentiment_score = ratings_data.get('sentiment_score', rating.sentiment_score)
                    rating.positive_ratio = ratings_data.get('positive_ratio', rating.positive_ratio)
                    rating.negative_ratio = ratings_data.get('negative_ratio', rating.negative_ratio)
                    rating.neutral_ratio = ratings_data.get('neutral_ratio', rating.neutral_ratio)
                    rating.final_rating = ratings_data.get('final_rating', rating.final_rating)
                    rating.updated_at = datetime.utcnow()
                else:
                    # Tạo mới đánh giá
                    rating = models['Rating'](
                        view_score=ratings_data.get('view_score'),
                        like_score=ratings_data.get('like_score'),
                        follow_score=ratings_data.get('follow_score'),
                        chapter_score=ratings_data.get('chapter_score'),
                        rating_score=ratings_data.get('rating_score'),
                        base_rating=ratings_data.get('base_rating'),
                        sentiment_score=ratings_data.get('sentiment_score'),
                        positive_ratio=ratings_data.get('positive_ratio'),
                        negative_ratio=ratings_data.get('negative_ratio'),
                        neutral_ratio=ratings_data.get('neutral_ratio'),
                        final_rating=ratings_data.get('final_rating')
                    )
                    session.add(rating)
                
                session.commit()
                
                # Cập nhật điểm trong chỉ mục để tìm kiếm nhanh
                story_index = StoryIndex.query.get(story_id)
                if story_index:
                    story_index.final_rating = ratings_data.get('final_rating')
                    story_index.has_analyzed = True
                    db.session.commit()
                
                logger.info(f"Đã cập nhật đánh giá cho truyện ID {story_id}")
                return rating
                
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật đánh giá: {str(e)}")
            return None
    
    def update_comment_sentiment(self, story_id, comment_id, sentiment, score):
        """
        Cập nhật sentiment cho một bình luận
        
        Args:
            story_id (int): ID của truyện
            comment_id (int): ID của bình luận
            sentiment (str): Kết quả sentiment (positive, negative, neutral)
            score (float): Điểm sentiment (0-1)
            
        Returns:
            object: Đối tượng Comment đã cập nhật
        """
        try:
            with self.db_manager.story_db_session(story_id) as (session, models):
                # Tìm bình luận
                comment = session.query(models['Comment']).get(comment_id)
                if not comment:
                    logger.error(f"Không tìm thấy bình luận ID {comment_id} trong truyện ID {story_id}")
                    return None
                    
                # Cập nhật sentiment
                comment.sentiment = sentiment
                comment.sentiment_score = score
                
                session.commit()
                logger.info(f"Đã cập nhật sentiment cho bình luận ID {comment_id} trong truyện ID {story_id}")
                return comment
                
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật sentiment bình luận: {str(e)}")
            return None
    
    def add_custom_field(self, story_id, field_name, field_type, field_value):
        """
        Thêm trường dữ liệu tùy chỉnh cho một truyện
        
        Args:
            story_id (int): ID của truyện
            field_name (str): Tên trường
            field_type (str): Loại dữ liệu (text, number, date, etc.)
            field_value (str): Giá trị trường
            
        Returns:
            object: Đối tượng CustomField đã tạo
        """
        try:
            with self.db_manager.story_db_session(story_id) as (session, models):
                # Kiểm tra trường đã tồn tại chưa
                custom_field = session.query(models['CustomField']).filter_by(field_name=field_name).first()
                
                if custom_field:
                    # Cập nhật giá trị
                    custom_field.field_type = field_type
                    custom_field.field_value = field_value
                else:
                    # Tạo mới
                    custom_field = models['CustomField'](
                        field_name=field_name,
                        field_type=field_type,
                        field_value=field_value
                    )
                    session.add(custom_field)
                
                session.commit()
                logger.info(f"Đã thêm/cập nhật trường {field_name} cho truyện ID {story_id}")
                return custom_field
                
        except Exception as e:
            logger.error(f"Lỗi khi thêm trường tùy chỉnh: {str(e)}")
            return None
    
    @staticmethod
    def add_analysis_report(title, description, story_ids, source, story_count, comment_count, excel_path):
        """
        Thêm một báo cáo phân tích mới
        
        Args:
            title (str): Tiêu đề báo cáo
            description (str): Mô tả báo cáo
            story_ids (list): Danh sách ID truyện phân tích
            source (str): Nguồn dữ liệu
            story_count (int): Số lượng truyện
            comment_count (int): Số lượng bình luận
            excel_path (str): Đường dẫn đến file Excel
            
        Returns:
            AnalysisReport: Đối tượng AnalysisReport đã tạo
        """
        try:
            # Chuyển danh sách ID thành chuỗi JSON
            story_ids_json = json.dumps(story_ids)
            
            report = AnalysisReport(
                title=title,
                description=description,
                story_ids=story_ids_json,
                source=source,
                story_count=story_count,
                comment_count=comment_count,
                excel_path=excel_path
            )
            
            db.session.add(report)
            db.session.commit()
            logger.info(f"Đã thêm báo cáo phân tích: {title}")
            return report
            
        except SQLAlchemyError as e:
            db.session.rollback()
            logger.error(f"Lỗi khi thêm báo cáo phân tích: {str(e)}")
            return None
    
    def get_stories(self, website_id=None, min_views=None, min_rating=None, has_analyzed=None):
        """
        Lấy danh sách truyện với các bộ lọc
        
        Args:
            website_id (int, optional): ID website
            min_views (int, optional): Lượt xem tối thiểu
            min_rating (float, optional): Điểm đánh giá tối thiểu
            has_analyzed (bool, optional): Đã phân tích hay chưa
            
        Returns:
            list: Danh sách các đối tượng StoryIndex
        """
        try:
            query = StoryIndex.query
            
            # Áp dụng bộ lọc
            if website_id:
                query = query.filter_by(website_id=website_id)
                
            if min_views:
                query = query.filter(StoryIndex.views >= min_views)
                
            if min_rating is not None:
                query = query.filter(StoryIndex.final_rating >= min_rating)
            
            if has_analyzed is not None:
                query = query.filter(StoryIndex.has_analyzed == has_analyzed)
            
            return query.all()
            
        except SQLAlchemyError as e:
            logger.error(f"Lỗi khi lấy danh sách truyện: {str(e)}")
            return []
    
    def get_story_details(self, story_id):
        """
        Lấy thông tin chi tiết của truyện từ database riêng
        
        Args:
            story_id (int): ID của truyện
            
        Returns:
            dict: Thông tin chi tiết truyện
        """
        try:
            # Lấy thông tin từ chỉ mục
            story_index = StoryIndex.query.get(story_id)
            if not story_index:
                logger.error(f"Không tìm thấy truyện ID {story_id}")
                return None
            
            with self.db_manager.story_db_session(story_id) as (session, models):
                # Lấy thông tin chi tiết
                story_detail = session.query(models['StoryDetail']).first()
                if not story_detail:
                    logger.error(f"Không tìm thấy chi tiết truyện ID {story_id}")
                    return None
                
                # Lấy đánh giá
                rating = session.query(models['Rating']).first()
                
                # Lấy bình luận
                comments = session.query(models['Comment']).all()
                
                # Lấy các trường tùy chỉnh
                custom_fields = session.query(models['CustomField']).all()
                
                # Tổng hợp thông tin
                result = {
                    'id': story_id,
                    'title': story_detail.title,
                    'alt_title': story_detail.alt_title,
                    'url': story_detail.url,
                    'cover_url': story_detail.cover_url,
                    'author': story_detail.author,
                    'status': story_detail.status,
                    'description': story_detail.description,
                    'views': story_detail.views,
                    'likes': story_detail.likes,
                    'follows': story_detail.follows,
                    'chapter_count': story_detail.chapter_count,
                    'rating': story_detail.rating,
                    'rating_count': story_detail.rating_count,
                    'source_website': story_detail.source_website,
                    'website': story_index.website.name if story_index.website else 'Unknown',
                    'created_at': story_detail.created_at,
                    'updated_at': story_detail.updated_at,
                    'comments': [],
                    'custom_fields': {}
                }
                
                # Thêm thông tin đánh giá
                if rating:
                    result.update({
                        'view_score': rating.view_score,
                        'like_score': rating.like_score,
                        'follow_score': rating.follow_score,
                        'chapter_score': rating.chapter_score,
                        'rating_score': rating.rating_score,
                        'base_rating': rating.base_rating,
                        'sentiment_score': rating.sentiment_score,
                        'positive_ratio': rating.positive_ratio,
                        'negative_ratio': rating.negative_ratio,
                        'neutral_ratio': rating.neutral_ratio,
                        'final_rating': rating.final_rating
                    })
                
                # Thêm bình luận
                for comment in comments:
                    result['comments'].append({
                        'id': comment.id,
                        'username': comment.username,
                        'content': comment.content,
                        'date': comment.date,
                        'sentiment': comment.sentiment,
                        'sentiment_score': comment.sentiment_score
                    })
                
                # Thêm trường tùy chỉnh
                for field in custom_fields:
                    # Chuyển đổi giá trị theo loại dữ liệu
                    if field.field_type == 'number':
                        try:
                            value = float(field.field_value)
                        except (ValueError, TypeError):
                            value = field.field_value
                    elif field.field_type == 'date':
                        try:
                            value = datetime.fromisoformat(field.field_value)
                        except (ValueError, TypeError):
                            value = field.field_value
                    elif field.field_type == 'json':
                        try:
                            value = json.loads(field.field_value)
                        except (json.JSONDecodeError, TypeError):
                            value = field.field_value
                    else:
                        value = field.field_value
                    
                    result['custom_fields'][field.field_name] = value
                
                return result
                
        except Exception as e:
            logger.error(f"Lỗi khi lấy chi tiết truyện: {str(e)}")
            return None
    
    def export_to_excel(self, story_ids, output_dir='exports'):
        """
        Xuất dữ liệu truyện ra Excel
        
        Args:
            story_ids (list): Danh sách ID truyện cần xuất
            output_dir (str): Thư mục xuất file
            
        Returns:
            str: Đường dẫn đến file Excel
        """
        try:
            # Tạo thư mục nếu chưa tồn tại
            os.makedirs(output_dir, exist_ok=True)
            
            # Tạo DataFrame cho truyện và bình luận
            stories_data = []
            comments_data = []
            
            for story_id in story_ids:
                # Lấy thông tin chi tiết
                story_detail = self.get_story_details(story_id)
                if not story_detail:
                    continue
                
                # Thêm vào DataFrame truyện
                stories_data.append({
                    'ID': story_id,
                    'Tên truyện': story_detail['title'],
                    'Tên khác': story_detail.get('alt_title', ''),
                    'Tác giả': story_detail.get('author', ''),
                    'Trạng thái': story_detail.get('status', ''),
                    'Website': story_detail.get('website', ''),
                    'Lượt xem': story_detail.get('views', 0),
                    'Lượt thích': story_detail.get('likes', 0),
                    'Lượt theo dõi': story_detail.get('follows', 0),
                    'Số chương': story_detail.get('chapter_count', 0),
                    'Xếp hạng': story_detail.get('rating', 0),
                    'Số lượng đánh giá': story_detail.get('rating_count', 0),
                    'Điểm lượt xem': story_detail.get('view_score', 0),
                    'Điểm lượt thích': story_detail.get('like_score', 0),
                    'Điểm lượt theo dõi': story_detail.get('follow_score', 0),
                    'Điểm cơ bản': story_detail.get('base_rating', 0),
                    'Tỷ lệ tích cực': story_detail.get('positive_ratio', 0) * 100,
                    'Tỷ lệ tiêu cực': story_detail.get('negative_ratio', 0) * 100,
                    'Tỷ lệ trung tính': story_detail.get('neutral_ratio', 0) * 100,
                    'Điểm sentiment': story_detail.get('sentiment_score', 0),
                    'Điểm tổng hợp': story_detail.get('final_rating', 0),
                    'URL': story_detail.get('url', ''),
                    'Cập nhật lần cuối': story_detail.get('updated_at', '')
                })
                
                # Thêm vào DataFrame bình luận
                for comment in story_detail.get('comments', []):
                    comments_data.append({
                        'ID Truyện': story_id,
                        'Tên truyện': story_detail['title'],
                        'Người bình luận': comment.get('username', ''),
                        'Nội dung': comment.get('content', ''),
                        'Thời gian': comment.get('date', ''),
                        'Cảm xúc': comment.get('sentiment', ''),
                        'Điểm cảm xúc': comment.get('sentiment_score', 0)
                    })
            
            # Tạo DataFrame
            stories_df = pd.DataFrame(stories_data)
            comments_df = pd.DataFrame(comments_data)
            
            # Tạo tên file với timestamp
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            excel_file = os.path.join(output_dir, f"comic_analysis_{timestamp}.xlsx")
            
            # Xuất ra Excel
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                stories_df.to_excel(writer, sheet_name='Truyện', index=False)
                comments_df.to_excel(writer, sheet_name='Bình luận', index=False)
                
                # Tạo sheet thống kê
                stats_data = {
                    'Chỉ số': [
                        'Tổng số truyện',
                        'Tổng số bình luận',
                        'Điểm tổng hợp trung bình',
                        'Tỷ lệ bình luận tích cực trung bình',
                        'Tỷ lệ bình luận tiêu cực trung bình',
                        'Truyện có điểm cao nhất',
                        'Điểm cao nhất',
                        'Truyện có nhiều bình luận nhất',
                        'Thời gian xuất'
                    ],
                    'Giá trị': [
                        len(stories_data),
                        len(comments_data),
                        stories_df['Điểm tổng hợp'].mean() if not stories_df.empty else 0,
                        stories_df['Tỷ lệ tích cực'].mean() if not stories_df.empty else 0,
                        stories_df['Tỷ lệ tiêu cực'].mean() if not stories_df.empty else 0,
                        stories_df.loc[stories_df['Điểm tổng hợp'].idxmax(), 'Tên truyện'] if not stories_df.empty and len(stories_df) > 0 else 'N/A',
                        stories_df['Điểm tổng hợp'].max() if not stories_df.empty else 0,
                        stories_df.iloc[comments_df.groupby('ID Truyện').size().idxmax()]['Tên truyện'] if not comments_df.empty and len(comments_df.groupby('ID Truyện')) > 0 else 'N/A',
                        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    ]
                }
                
                pd.DataFrame(stats_data).to_excel(writer, sheet_name='Thống kê', index=False)
            
            logger.info(f"Đã xuất dữ liệu phân tích vào file: {excel_file}")
            return excel_file
            
        except Exception as e:
            logger.error(f"Lỗi khi xuất dữ liệu ra Excel: {str(e)}")
            return None