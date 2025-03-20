import os
from datetime import timedelta

class Config:
    # Flask config
    SECRET_KEY = os.environ.get('SECRET_KEY', 'your-secret-key-goes-here')
    DEBUG = os.environ.get('FLASK_DEBUG', 'True') == 'True'
    
    # SQLAlchemy config
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL', 'sqlite:///comic_analyzer.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # File uploads
    UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
    
    # Session config
    SESSION_TYPE = 'filesystem'
    PERMANENT_SESSION_LIFETIME = timedelta(days=7)
    
    # Crawler config
    CHROMEDRIVER_PATH = os.environ.get('CHROMEDRIVER_PATH', None)
    MAX_PAGES_TO_CRAWL = 5
    
    # Analysis config
    SENTIMENT_MODEL = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
    SENTIMENT_WEIGHT = 0.4  # Trọng số của sentiment trong điểm tổng hợp (0-1)