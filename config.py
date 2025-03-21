import os
from datetime import timedelta

class Config:
    
    STORY_DB_DIR = 'databases/stories'
    # Flask config
    SECRET_KEY = os.environ.get('SECRET_KEY', 'your-secret-key-goes-here')
    DEBUG = os.environ.get('FLASK_DEBUG', 'True') == 'True'
    
    # Database - Chỉ giữ master db cho thông tin website
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL', 'sqlite:///databases/master.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    WEBSITE_DB_DIR = os.environ.get('WEBSITE_DB_DIR', 'databases/websites')
    
    # File uploads
    UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
    
    # Các thư mục khác
    STORY_DB_DIR = os.environ.get('STORY_DB_DIR', 'databases/stories')
    UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER', 'uploads')
    EXPORT_FOLDER = os.environ.get('EXPORT_FOLDER', 'exports')
        
    # Session config
    SESSION_TYPE = 'filesystem'
    PERMANENT_SESSION_LIFETIME = timedelta(days=7)
    
    # Crawler config
    # Chrome driver
    CHROME_DRIVER_PATH = os.environ.get('CHROME_DRIVER_PATH', 'crawlers/chromedriver.exe')
 
    MAX_PAGES_TO_CRAWL = 5
    
    # Analysis config
    SENTIMENT_MODEL = os.environ.get('SENTIMENT_MODEL', 'xlm-roberta-base')
    SENTIMENT_CACHE_DIR = os.environ.get('SENTIMENT_CACHE_DIR', '.cache/huggingface')

    SENTIMENT_WEIGHT = 0.4  # Trọng số của sentiment trong điểm tổng hợp (0-1)