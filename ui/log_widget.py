from PyQt6.QtWidgets import QTextEdit
from PyQt6.QtCore import Qt, pyqtSlot
from PyQt6.QtGui import QColor, QTextCursor
import logging

class LogWidget(QTextEdit):
    """Widget hiển thị log với màu sắc và giới hạn số dòng"""
    
    # Định nghĩa màu cho các loại log
    LOG_COLORS = {
        logging.DEBUG: QColor("#6D8A96"),  # Xám xanh
        logging.INFO: QColor("#FFFFFF"),   # Trắng
        logging.WARNING: QColor("#FFCC00"), # Vàng
        logging.ERROR: QColor("#FF6666"),  # Đỏ
        logging.CRITICAL: QColor("#FF0000") # Đỏ đậm
    }
    
    def __init__(self, max_lines=1000, parent=None):
        super().__init__(parent)
        self.max_lines = max_lines
        
        # Cấu hình TextEdit
        self.setReadOnly(True)
        self.setLineWrapMode(QTextEdit.LineWrapMode.WidgetWidth)
        
        # Thiết lập màu nền tối
        self.setStyleSheet("""
            QTextEdit {
                background-color: #2D2D30;
                color: #FFFFFF;
                font-family: 'Consolas', 'Courier New', monospace;
                font-size: 10pt;
            }
        """)
        
        # Kết nối với logger của Python
        self.handler = LogWidgetHandler(self)
        self.handler.setLevel(logging.DEBUG)
        logger = logging.getLogger()
        logger.addHandler(self.handler)
    
    @pyqtSlot(int, str)
    def append_log(self, level, message):
        """Thêm một dòng log với màu sắc tương ứng"""
        self.moveCursor(QTextCursor.MoveOperation.End)
        
        # Định dạng màu
        color = self.LOG_COLORS.get(level, QColor("#FFFFFF"))
        self.setTextColor(color)
        
        # Thêm nội dung
        self.insertPlainText(message + "\n")
        
        # Giới hạn số dòng
        self.limit_lines()
        
        # Cuộn xuống cuối
        self.moveCursor(QTextCursor.MoveOperation.End)
        self.ensureCursorVisible()
    
    def limit_lines(self):
        """Giới hạn số dòng được hiển thị"""
        document = self.document()
        
        while document.blockCount() > self.max_lines:
            # Xóa block đầu tiên
            cursor = QTextCursor(document)
            cursor.movePosition(QTextCursor.MoveOperation.Start)
            cursor.select(QTextCursor.SelectionType.BlockUnderCursor)
            cursor.removeSelectedText()
            cursor.deleteChar()  # Xóa cả ký tự xuống dòng

class LogWidgetHandler(logging.Handler):
    """Handler để kết nối logging của Python với LogWidget"""
    
    def __init__(self, log_widget):
        super().__init__()
        self.log_widget = log_widget
        self.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s'))
    
    def emit(self, record):
        """Xử lý log record và chuyển đến widget"""
        try:
            level = record.levelno
            msg = self.format(record)
            
            # Emit signal để cập nhật UI trong main thread
            self.log_widget.append_log(level, msg)
            
        except Exception:
            self.handleError(record)