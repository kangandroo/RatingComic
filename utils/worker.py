from PyQt6.QtCore import QObject, QRunnable, pyqtSignal, pyqtSlot

class WorkerSignals(QObject):
    """Đối tượng chứa các signal cho Worker"""
    finished = pyqtSignal()
    error = pyqtSignal(str)
    result = pyqtSignal(object)
    progress = pyqtSignal(int)

class Worker(QRunnable):
    """
    Worker thread để chạy các tác vụ nặng trong thread riêng
    
    Tránh gây đóng băng giao diện người dùng
    """
    
    def __init__(self, fn, *args, **kwargs):
        """
        Khởi tạo Worker
        
        Args:
            fn: Hàm cần chạy
            *args, **kwargs: Tham số cho hàm
        """
        super(Worker, self).__init__()
        
        # Lưu trữ hàm và tham số
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()
        
        # Thêm callback vào kwargs nếu chưa có
        if 'progress_callback' not in kwargs:
            kwargs['progress_callback'] = self.signals.progress
    
    @pyqtSlot()
    def run(self):
        """Chạy worker"""
        try:
            # Gọi hàm với tham số
            result = self.fn(*self.args, **self.kwargs)
            
            # Emit kết quả
            self.signals.result.emit(result)
            
        except Exception as e:
            # Emit lỗi
            self.signals.error.emit(str(e))
            
        finally:
            # Luôn emit signal đã hoàn thành
            self.signals.finished.emit()