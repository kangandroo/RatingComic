from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

class Website(db.Model):
    """Model thông tin website crawl"""
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)
    url = db.Column(db.String(200), nullable=False)
    api_name = db.Column(db.String(50), nullable=False, unique=True)  # nettruyen, truyenqq, manhuavn
    stories = db.relationship('Story', backref='website', lazy=True)
    
    def __repr__(self):
        return f'<Website {self.name}>'

class Story(db.Model):
    """Model thông tin truyện"""
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    alt_title = db.Column(db.String(200))  # Tên khác
    url = db.Column(db.String(500), nullable=False, unique=True)
    cover_url = db.Column(db.String(500))
    
    # Thông tin cơ bản
    author = db.Column(db.String(100))
    status = db.Column(db.String(50))
    genres = db.Column(db.String(300))
    description = db.Column(db.Text)
    
    # Chỉ số định lượng
    views = db.Column(db.Integer, default=0)
    likes = db.Column(db.Integer, default=0)
    follows = db.Column(db.Integer, default=0)
    chapter_count = db.Column(db.Integer, default=0)
    rating = db.Column(db.Float, default=0)  # Điểm rating gốc (thang 0-10)
    rating_count = db.Column(db.Integer, default=0)
    
    # Điểm đánh giá thành phần
    view_score = db.Column(db.Float)
    like_score = db.Column(db.Float)
    follow_score = db.Column(db.Float)
    chapter_score = db.Column(db.Float)
    rating_score = db.Column(db.Float)
    base_rating = db.Column(db.Float)
    
    # Phân tích sentiment
    sentiment_score = db.Column(db.Float)
    positive_ratio = db.Column(db.Float)
    negative_ratio = db.Column(db.Float)
    neutral_ratio = db.Column(db.Float)
    has_analyzed_comments = db.Column(db.Boolean, default=False)
    
    # Điểm tổng hợp cuối cùng
    final_rating = db.Column(db.Float)
    
    # Metadata
    website_id = db.Column(db.Integer, db.ForeignKey('website.id'))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    comments = db.relationship('Comment', backref='story', lazy=True, cascade="all, delete-orphan")
    
    def __repr__(self):
        return f'<Story {self.title}>'

class Comment(db.Model):
    """Model thông tin bình luận"""
    id = db.Column(db.Integer, primary_key=True)
    story_id = db.Column(db.Integer, db.ForeignKey('story.id'), nullable=False)
    username = db.Column(db.String(100))
    content = db.Column(db.Text)
    date = db.Column(db.DateTime)
    
    # Phân tích sentiment
    sentiment = db.Column(db.String(20))  # positive, negative, neutral
    sentiment_score = db.Column(db.Float)
    
    # Metadata
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f'<Comment {self.id} by {self.username}>'

class AnalysisReport(db.Model):
    """Model lưu thông tin báo cáo phân tích"""
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    description = db.Column(db.Text)
    source = db.Column(db.String(50))  # nettruyen, truyenqq, manhuavn hoặc multiple
    story_count = db.Column(db.Integer, default=0)
    comment_count = db.Column(db.Integer, default=0)
    excel_path = db.Column(db.String(500))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f'<AnalysisReport {self.title}>'