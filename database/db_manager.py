import os
import sqlite3
import json
from flask import current_app
from flask_sqlalchemy import SQLAlchemy
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.models import StoryBaseModel
from utils.logger import default_logger as logger

class DatabaseManager:
    """Quản lý nhiều database cho từng truyện"""
    
    def __init__(self, app=None, db_dir='databases/stories'):
        self.app = app
        self.db_dir = db_dir
        self.master_db = None
        
        # Tạo thư mục databases nếu chưa tồn tại
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
        
        if app is not None:
            self.init_app(app)
    
    def init_app(self, app):
        """Khởi tạo với Flask app"""
        self.app = app
        self.db_dir = app.config.get('STORY_DB_DIR', 'databases/stories')
        
        # Đảm bảo thư mục tồn tại
        if not os.path.exists(self.db_dir):
            os.makedirs(self.db_dir)
        
        # Thiết lập database chính 
        self.master_db = SQLAlchemy(app)
    
    def get_story_db_path(self, story_id):
        """Lấy đường dẫn đến file database của truyện"""
        return os.path.join(self.db_dir, f"story_{story_id}.db")
    
    def create_story_db(self, story_id):
        """Tạo database mới cho truyện"""
        from db.models import StoryIndex
        
        # Lấy thông tin truyện từ database chính
        story_index = StoryIndex.query.get(story_id)
        if not story_index:
            raise ValueError(f"Không tìm thấy truyện với ID {story_id}")
        
        # Tạo đường dẫn file database
        db_path = self.get_story_db_path(story_id)
        
        # Tạo URI SQLite
        db_uri = f"sqlite:///{db_path}"
        
        # Tạo database và các bảng
        engine = create_engine(db_uri)
        temp_db = SQLAlchemy(metadata=SQLAlchemy.Metadata())
        temp_db.Model.metadata.bind = engine
        
        # Lấy các models
        models = StoryBaseModel.get_story_models(temp_db)
        
        # Tạo tất cả bảng
        temp_db.Model.metadata.create_all(engine)
        
        # Cập nhật đường dẫn trong story_index
        story_index.db_path = db_path
        self.master_db.session.commit()
        
        # Tối ưu database
        self.optimize_sqlite_db(db_path)
        
        logger.info(f"Đã tạo database riêng cho truyện ID {story_id}: {db_path}")
        return db_path
    
    @contextmanager
    def story_db_session(self, story_id):
        """Context manager để làm việc với database của truyện"""
        from db.models import StoryIndex
        
        # Lấy thông tin truyện từ database chính
        story_index = StoryIndex.query.get(story_id)
        if not story_index:
            raise ValueError(f"Không tìm thấy truyện với ID {story_id}")
        
        # Nếu chưa có database, tạo mới
        if not story_index.db_path or not os.path.exists(story_index.db_path):
            self.create_story_db(story_id)
            # Reload story_index
            story_index = StoryIndex.query.get(story_id)
        
        # Tạo engine và session
        engine = create_engine(f"sqlite:///{story_index.db_path}")
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # Lấy models
            temp_db = SQLAlchemy(metadata=SQLAlchemy.Metadata())
            temp_db.Model.metadata.bind = engine
            models = StoryBaseModel.get_story_models(temp_db)
            
            # Trả về session và models
            yield session, models
        finally:
            session.close()
    
    def optimize_sqlite_db(self, db_path):
        """Tối ưu hóa database SQLite"""
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Bật ràng buộc khóa ngoại
            cursor.execute("PRAGMA foreign_keys=ON")
            
            # Tối ưu cấu hình
            cursor.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA cache_size=5000")
            cursor.execute("PRAGMA temp_store=MEMORY")
            cursor.execute("PRAGMA locking_mode=EXCLUSIVE")
            
            # Tạo indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_sentiment ON comments(sentiment)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_chapters_number ON chapters(number)")
            
            # Vacuum để tối ưu kích thước
            cursor.execute("VACUUM")
            
            conn.close()
            logger.info(f"Đã tối ưu hóa database: {db_path}")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi tối ưu database {db_path}: {str(e)}")
            return False
    
    def backup_story_db(self, story_id, backup_dir='backups/stories'):
        """Sao lưu database của một truyện"""
        import shutil
        from datetime import datetime
        
        # Tạo thư mục backup
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        # Lấy đường dẫn database
        db_path = self.get_story_db_path(story_id)
        if not os.path.exists(db_path):
            logger.error(f"Không tìm thấy database truyện ID {story_id}")
            return None
        
        # Tạo tên file backup
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(backup_dir, f"story_{story_id}_{timestamp}.db")
        
        # Sao chép file
        shutil.copy2(db_path, backup_path)
        logger.info(f"Đã sao lưu database truyện ID {story_id}: {backup_path}")
        
        return backup_path