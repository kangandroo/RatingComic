from PyQt6.QtCore import QObject, pyqtSignal, QRunnable, pyqtSlot
import traceback
import logging

logger = logging.getLogger(__name__)

class WorkerSignals(QObject):
    """
    Signals cho Worker
    """
    progress = pyqtSignal(int)
    result = pyqtSignal(object)
    error = pyqtSignal(str)
    finished = pyqtSignal()

class Worker(QRunnable):
    """
    Worker thread
    
    Thực thi task trong thread riêng và phát signals
    cho phép cập nhật UI trong main thread
    """
    
    def __init__(self, fn, *args, **kwargs):
        """
        Khởi tạo Worker
        
        Args:
            fn: Function cần thực thi
            *args: Arguments cho function
            **kwargs: Keyword arguments cho function
        """
        super(Worker, self).__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()
        
        # Pass progress callback vào kwargs
        self.kwargs['progress_callback'] = self.signals.progress
    
    @pyqtSlot()
    def run(self):
        """
        Thực thi function trong thread riêng
        """
        try:
            result = self.fn(*self.args, **self.kwargs)
            self.signals.result.emit(result)
        except Exception as e:
            logger.error(f"Lỗi trong worker: {str(e)}")
            logger.error(traceback.format_exc())
            self.signals.error.emit(str(e))
        finally:
            self.signals.finished.emit()