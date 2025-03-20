from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import os

db = SQLAlchemy()

# Models cho database chính
class Website(db.Model):
    """Model thông tin website crawl"""
    __tablename__ = 'websites'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)
    url = db.Column(db.String(200), nullable=False)
    api_name = db.Column(db.String(50), nullable=False, unique=True)  # nettruyen, truyenqq, manhuavn
    stories = db.relationship('StoryIndex', backref='website', lazy=True)

class StoryIndex(db.Model):
    """Model chỉ mục truyện - lưu trong database chính"""
    __tablename__ = 'story_index'
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    url = db.Column(db.String(500), nullable=False, unique=True)
    cover_url = db.Column(db.String(500))
    author = db.Column(db.String(100))
    # Không lưu thể loại theo yêu cầu
    website_id = db.Column(db.Integer, db.ForeignKey('websites.id'))
    
    # Đường dẫn tới database riêng của truyện
    db_path = db.Column(db.String(500))
    
    # Thông tin tổng hợp từ db riêng để tìm kiếm nhanh
    views = db.Column(db.Integer, default=0)
    chapter_count = db.Column(db.Integer, default=0)
    final_rating = db.Column(db.Float)
    has_analyzed = db.Column(db.Boolean, default=False)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f'<StoryIndex {self.title}>'

class AnalysisReport(db.Model):
    """Model lưu thông tin báo cáo phân tích"""
    __tablename__ = 'analysis_reports'
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    description = db.Column(db.Text)
    story_ids = db.Column(db.Text)  # JSON string của story ids
    source = db.Column(db.String(50))
    story_count = db.Column(db.Integer, default=0)
    comment_count = db.Column(db.Integer, default=0)
    excel_path = db.Column(db.String(500))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
# Models cho database của từng truyện riêng lẻ
class StoryBaseModel:
    """Lớp cơ sở để tạo models cho từng truyện"""
    @staticmethod
    def get_story_models(db):
        """Tạo các models cho database truyện riêng lẻ"""
        
        class StoryDetail(db.Model):
            """Chi tiết truyện"""
            __tablename__ = 'story_detail'
            id = db.Column(db.Integer, primary_key=True)
            title = db.Column(db.String(200), nullable=False)
            alt_title = db.Column(db.String(200))
            url = db.Column(db.String(500), nullable=False)
            cover_url = db.Column(db.String(500))
            
            # Thông tin chi tiết
            author = db.Column(db.String(100))
            status = db.Column(db.String(50))
            # Không lưu thể loại theo yêu cầu
            description = db.Column(db.Text)
            
            # Chỉ số thống kê
            views = db.Column(db.Integer, default=0)
            likes = db.Column(db.Integer, default=0)
            follows = db.Column(db.Integer, default=0)
            chapter_count = db.Column(db.Integer, default=0)
            rating = db.Column(db.Float, default=0)
            rating_count = db.Column(db.Integer, default=0)
            
            # Các metadata khác
            source_website = db.Column(db.String(100))
            created_at = db.Column(db.DateTime, default=datetime.utcnow)
            updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
        
        class Chapter(db.Model):
            """Thông tin chương của truyện"""
            __tablename__ = 'chapters'
            id = db.Column(db.Integer, primary_key=True)
            number = db.Column(db.Integer)
            title = db.Column(db.String(200))
            url = db.Column(db.String(500))
            views = db.Column(db.Integer, default=0)
            upload_date = db.Column(db.DateTime)
            created_at = db.Column(db.DateTime, default=datetime.utcnow)
        
        class Comment(db.Model):
            """Bình luận về truyện"""
            __tablename__ = 'comments'
            id = db.Column(db.Integer, primary_key=True)
            username = db.Column(db.String(100))
            content = db.Column(db.Text)
            date = db.Column(db.DateTime)
            
            # Phân tích sentiment
            sentiment = db.Column(db.String(20))  # positive, negative, neutral
            sentiment_score = db.Column(db.Float)
            
            created_at = db.Column(db.DateTime, default=datetime.utcnow)
        
        class Rating(db.Model):
            """Đánh giá của truyện"""
            __tablename__ = 'ratings'
            id = db.Column(db.Integer, primary_key=True)
            
            # Điểm thành phần
            view_score = db.Column(db.Float)
            like_score = db.Column(db.Float)
            follow_score = db.Column(db.Float)
            chapter_score = db.Column(db.Float)
            rating_score = db.Column(db.Float)
            
            # Phân tích sentiment
            sentiment_score = db.Column(db.Float)
            positive_ratio = db.Column(db.Float)
            negative_ratio = db.Column(db.Float)
            neutral_ratio = db.Column(db.Float)
            
            # Điểm tổng hợp
            base_rating = db.Column(db.Float)
            final_rating = db.Column(db.Float)
            
            created_at = db.Column(db.DateTime, default=datetime.utcnow)
            updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
        
        class CustomField(db.Model):
            """Trường tùy chỉnh cho từng truyện"""
            __tablename__ = 'custom_fields'
            id = db.Column(db.Integer, primary_key=True)
            field_name = db.Column(db.String(100))
            field_type = db.Column(db.String(50))  # text, number, date, etc.
            field_value = db.Column(db.Text)
            created_at = db.Column(db.DateTime, default=datetime.utcnow)
        
        return {
            'StoryDetail': StoryDetail,
            'Chapter': Chapter,
            'Comment': Comment,
            'Rating': Rating,
            'CustomField': CustomField
        }