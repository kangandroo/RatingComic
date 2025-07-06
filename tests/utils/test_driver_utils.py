import pytest
import time
import logging
import os
import signal
import multiprocessing
from unittest.mock import Mock, patch, MagicMock, call
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException

# Import the module under test
import sys
sys.path.insert(0, '/home/runner/work/RatingComic/RatingComic')

from utils.driver_utils import (
    retry,
    check_system_resources,
    setup_signal_handlers,
    init_process,
    get_text_safe,
    process_comic_worker,
    MAX_MEMORY_PERCENT,
    MAX_DRIVER_INSTANCES,
    DEFAULT_TIMEOUT,
    MAX_RETRIES,
    driver_semaphore
)


class TestConstants:
    """Test module constants"""
    
    def test_constants_are_defined(self):
        """Test that all required constants are defined with correct types"""
        assert isinstance(MAX_MEMORY_PERCENT, int)
        assert MAX_MEMORY_PERCENT == 80
        
        assert isinstance(MAX_DRIVER_INSTANCES, int)
        assert MAX_DRIVER_INSTANCES == 25
        
        assert isinstance(DEFAULT_TIMEOUT, int)
        assert DEFAULT_TIMEOUT == 30
        
        assert isinstance(MAX_RETRIES, int)
        assert MAX_RETRIES == 3
        
        assert isinstance(driver_semaphore, type(multiprocessing.Semaphore(1)))


class TestRetryDecorator:
    """Test the retry decorator functionality"""
    
    def test_retry_success_on_first_attempt(self):
        """Test that retry decorator works with successful function on first attempt"""
        @retry(max_retries=3, delay=0.1)
        def successful_function():
            return "success"
        
        result = successful_function()
        assert result == "success"
    
    def test_retry_success_after_failures(self, caplog):
        """Test that retry decorator works after some failures"""
        call_count = 0
        
        @retry(max_retries=3, delay=0.1)
        def failing_then_success():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary failure")
            return "success"
        
        with caplog.at_level(logging.WARNING):
            result = failing_then_success()
        
        assert result == "success"
        assert call_count == 3
        assert "Thử lại" in caplog.text
    
    def test_retry_max_retries_exceeded(self, caplog):
        """Test that retry decorator raises exception after max retries exceeded"""
        @retry(max_retries=2, delay=0.1)
        def always_failing():
            raise ValueError("Always fails")
        
        with pytest.raises(ValueError, match="Always fails"):
            with caplog.at_level(logging.ERROR):
                always_failing()
        
        assert "thất bại sau" in caplog.text
    
    def test_retry_with_exponential_backoff(self):
        """Test that retry decorator implements exponential backoff"""
        call_times = []
        
        @retry(max_retries=3, delay=0.1)
        def failing_function():
            call_times.append(time.time())
            raise ValueError("Test failure")
        
        with pytest.raises(ValueError):
            failing_function()
        
        # Should have 3 calls (initial + 2 retries)
        assert len(call_times) == 3
        
        # Check exponential backoff (approximately)
        if len(call_times) >= 2:
            delay1 = call_times[1] - call_times[0]
            assert delay1 >= 0.1  # First retry delay
        
        if len(call_times) >= 3:
            delay2 = call_times[2] - call_times[1]
            assert delay2 >= 0.2  # Second retry delay (exponential backoff)


class TestCheckSystemResources:
    """Test system resource checking functionality"""
    
    @patch('utils.driver_utils.psutil.virtual_memory')
    @patch('utils.driver_utils.gc.collect')
    @patch('utils.driver_utils.time.sleep')
    def test_check_system_resources_memory_ok(self, mock_sleep, mock_gc, mock_memory):
        """Test check_system_resources when memory usage is acceptable"""
        # Mock memory usage below threshold
        mock_memory.return_value.percent = 70
        
        result = check_system_resources()
        
        assert result is True
        mock_memory.assert_called_once()
        mock_gc.assert_not_called()
        mock_sleep.assert_not_called()
    
    @patch('utils.driver_utils.psutil.virtual_memory')
    @patch('utils.driver_utils.gc.collect')
    @patch('utils.driver_utils.time.sleep')
    def test_check_system_resources_memory_high(self, mock_sleep, mock_gc, mock_memory, caplog):
        """Test check_system_resources when memory usage is high"""
        # Mock memory usage above threshold
        mock_memory.return_value.percent = 85
        
        with caplog.at_level(logging.WARNING):
            result = check_system_resources()
        
        assert result is False
        mock_memory.assert_called_once()
        mock_gc.assert_called_once()
        mock_sleep.assert_called_once_with(5)
        assert "Cảnh báo: Sử dụng bộ nhớ cao" in caplog.text
    
    @patch('utils.driver_utils.psutil.virtual_memory')
    def test_check_system_resources_exception_handling(self, mock_memory, caplog):
        """Test check_system_resources handles exceptions gracefully"""
        # Mock psutil raising an exception
        mock_memory.side_effect = Exception("psutil error")
        
        with caplog.at_level(logging.ERROR):
            result = check_system_resources()
        
        assert result is True  # Should return True on error to allow continuation
        assert "Lỗi khi kiểm tra tài nguyên hệ thống" in caplog.text


class TestSetupSignalHandlers:
    """Test signal handler setup functionality"""
    
    @patch('utils.driver_utils.os.name', 'posix')
    @patch('utils.driver_utils.signal.signal')
    def test_setup_signal_handlers_unix(self, mock_signal):
        """Test signal handler setup on Unix systems"""
        setup_signal_handlers()
        
        # Should register SIGTERM handler
        mock_signal.assert_called_once()
        args, kwargs = mock_signal.call_args
        assert args[0] == signal.SIGTERM
        assert callable(args[1])
    
    @patch('utils.driver_utils.os.name', 'nt')
    @patch('utils.driver_utils.signal.signal')
    def test_setup_signal_handlers_windows(self, mock_signal):
        """Test signal handler setup on Windows (should not register handlers)"""
        setup_signal_handlers()
        
        # Should not register any handlers on Windows
        mock_signal.assert_not_called()
    
    @patch('utils.driver_utils.os.name', 'posix')
    @patch('utils.driver_utils.signal.signal')
    @patch('utils.driver_utils.current_process')
    @patch('sys.exit')
    def test_signal_handler_execution(self, mock_exit, mock_current_process, mock_signal, caplog):
        """Test that the signal handler executes correctly"""
        mock_current_process.return_value.name = "TestProcess"
        
        setup_signal_handlers()
        
        # Get the handler function that was registered
        handler_func = mock_signal.call_args[0][1]
        
        with caplog.at_level(logging.INFO):
            handler_func(signal.SIGTERM, None)
        
        assert "Process TestProcess nhận tín hiệu SIGTERM" in caplog.text
        mock_exit.assert_called_once_with(0)


class TestInitProcess:
    """Test process initialization functionality"""
    
    @patch('utils.driver_utils.multiprocessing.current_process')
    @patch('utils.driver_utils.setup_signal_handlers')
    def test_init_process(self, mock_setup_signals, mock_current_process):
        """Test process initialization"""
        mock_process = Mock()
        mock_current_process.return_value = mock_process
        
        init_process()
        
        # Should set daemon to False and setup signal handlers
        assert mock_process.daemon is False
        mock_setup_signals.assert_called_once()


class TestGetTextSafe:
    """Test safe text extraction functionality"""
    
    def test_get_text_safe_with_css_selector_success(self):
        """Test get_text_safe with CSS selector returning text successfully"""
        # Mock element with find_elements method
        mock_element = Mock()
        mock_found_element = Mock()
        mock_found_element.text = "  Test Text  "
        mock_element.find_elements.return_value = [mock_found_element]
        
        result = get_text_safe(mock_element, ".test-selector")
        
        assert result == "Test Text"
        mock_element.find_elements.assert_called_once_with(By.CSS_SELECTOR, ".test-selector")
    
    def test_get_text_safe_with_by_selector_success(self):
        """Test get_text_safe with By object selector"""
        mock_element = Mock()
        mock_found_element = Mock()
        mock_found_element.text = "Test Text"
        mock_element.find_elements.return_value = [mock_found_element]
        
        selector = (By.ID, "test-id")
        result = get_text_safe(mock_element, selector)
        
        assert result == "Test Text"
        mock_element.find_elements.assert_called_once_with(By.ID, "test-id")
    
    def test_get_text_safe_no_elements_found(self):
        """Test get_text_safe when no elements are found"""
        mock_element = Mock()
        mock_element.find_elements.return_value = []
        
        result = get_text_safe(mock_element, ".test-selector", default="Not Found")
        
        assert result == "Not Found"
    
    def test_get_text_safe_empty_text(self):
        """Test get_text_safe when element text is empty"""
        mock_element = Mock()
        mock_found_element = Mock()
        mock_found_element.text = "   "  # Only whitespace
        mock_element.find_elements.return_value = [mock_found_element]
        
        result = get_text_safe(mock_element, ".test-selector", default="Empty")
        
        assert result == "Empty"
    
    def test_get_text_safe_no_such_element_exception(self):
        """Test get_text_safe handles NoSuchElementException"""
        mock_element = Mock()
        mock_element.find_elements.side_effect = NoSuchElementException("Element not found")
        
        result = get_text_safe(mock_element, ".test-selector", default="Exception")
        
        assert result == "Exception"
    
    def test_get_text_safe_stale_element_exception(self):
        """Test get_text_safe handles StaleElementReferenceException"""
        mock_element = Mock()
        mock_element.find_elements.side_effect = StaleElementReferenceException("Element stale")
        
        result = get_text_safe(mock_element, ".test-selector", default="Stale")
        
        assert result == "Stale"
    
    def test_get_text_safe_general_exception(self):
        """Test get_text_safe handles general exceptions"""
        mock_element = Mock()
        mock_element.find_elements.side_effect = Exception("General error")
        
        result = get_text_safe(mock_element, ".test-selector", default="Error")
        
        assert result == "Error"


class TestProcessComicWorker:
    """Test process comic worker functionality"""
    
    @patch('utils.driver_utils.check_system_resources')
    @patch('utils.driver_utils.SQLiteHelper')
    @patch('utils.driver_utils.driver_semaphore')
    def test_process_comic_worker_insufficient_resources(self, mock_semaphore, mock_sqlite_helper, mock_check_resources, caplog):
        """Test process_comic_worker when system resources are insufficient"""
        mock_check_resources.return_value = False
        
        comic = {'Tên truyện': 'Test Comic'}
        params = (comic, '/test/db/path', 'http://example.com', 1)
        
        mock_crawl_function = Mock()
        mock_setup_driver = Mock()
        
        with caplog.at_level(logging.WARNING):
            result = process_comic_worker(params, mock_crawl_function, mock_setup_driver)
        
        assert result is None
        assert "Tài nguyên hệ thống không đủ" in caplog.text
        mock_sqlite_helper.assert_not_called()
    
    @patch('utils.driver_utils.check_system_resources')
    @patch('utils.driver_utils.SQLiteHelper')
    def test_process_comic_worker_database_connection_error(self, mock_sqlite_helper, mock_check_resources, caplog):
        """Test process_comic_worker when database connection fails"""
        mock_check_resources.return_value = True
        mock_sqlite_helper.side_effect = Exception("DB connection failed")
        
        comic = {'Tên truyện': 'Test Comic'}
        params = (comic, '/test/db/path', 'http://example.com', 1)
        
        mock_crawl_function = Mock()
        mock_setup_driver = Mock()
        
        with caplog.at_level(logging.ERROR):
            result = process_comic_worker(params, mock_crawl_function, mock_setup_driver)
        
        assert result is None
        assert "Không thể kết nối đến database" in caplog.text
    
    @patch('utils.driver_utils.check_system_resources')
    @patch('utils.driver_utils.SQLiteHelper')
    @patch('utils.driver_utils.driver_semaphore')
    def test_process_comic_worker_driver_creation_error(self, mock_semaphore, mock_sqlite_helper, mock_check_resources, caplog):
        """Test process_comic_worker when driver creation fails"""
        mock_check_resources.return_value = True
        mock_sqlite_instance = Mock()
        mock_sqlite_helper.return_value = mock_sqlite_instance
        
        # Mock semaphore context manager
        mock_semaphore.__enter__ = Mock(return_value=None)
        mock_semaphore.__exit__ = Mock(return_value=None)
        
        mock_setup_driver = Mock(side_effect=Exception("Driver creation failed"))
        
        comic = {'Tên truyện': 'Test Comic'}
        params = (comic, '/test/db/path', 'http://example.com', 1)
        
        mock_crawl_function = Mock()
        
        with caplog.at_level(logging.ERROR):
            result = process_comic_worker(params, mock_crawl_function, mock_setup_driver)
        
        assert result is None
        assert "Không thể tạo driver" in caplog.text
        mock_sqlite_instance.close_all_connections.assert_called_once()
    
    @patch('utils.driver_utils.check_system_resources')
    @patch('utils.driver_utils.SQLiteHelper')
    @patch('utils.driver_utils.driver_semaphore')
    def test_process_comic_worker_successful_processing(self, mock_semaphore, mock_sqlite_helper, mock_check_resources, caplog):
        """Test process_comic_worker successful processing"""
        mock_check_resources.return_value = True
        mock_sqlite_instance = Mock()
        mock_sqlite_helper.return_value = mock_sqlite_instance
        
        # Mock semaphore context manager
        mock_semaphore.__enter__ = Mock(return_value=None)
        mock_semaphore.__exit__ = Mock(return_value=None)
        
        mock_driver = Mock()
        mock_setup_driver = Mock(return_value=mock_driver)
        
        expected_result = {'title': 'Test Result'}
        mock_crawl_function = Mock(return_value=expected_result)
        
        comic = {'Tên truyện': 'Test Comic'}
        params = (comic, '/test/db/path', 'http://example.com', 1)
        
        with caplog.at_level(logging.DEBUG):
            result = process_comic_worker(params, mock_crawl_function, mock_setup_driver)
        
        assert result == expected_result
        assert "Đã tạo driver thành công" in caplog.text
        assert "Đã đóng driver" in caplog.text
        assert "Đã đóng kết nối database" in caplog.text
        
        # Verify function calls
        mock_crawl_function.assert_called_once_with(comic, mock_driver, mock_sqlite_instance, 'http://example.com', 1)
        mock_driver.quit.assert_called_once()
        mock_sqlite_instance.close_all_connections.assert_called_once()
    
    @patch('utils.driver_utils.check_system_resources')
    @patch('utils.driver_utils.SQLiteHelper')
    @patch('utils.driver_utils.driver_semaphore')
    def test_process_comic_worker_crawl_function_error(self, mock_semaphore, mock_sqlite_helper, mock_check_resources, caplog):
        """Test process_comic_worker when crawl function raises exception"""
        mock_check_resources.return_value = True
        mock_sqlite_instance = Mock()
        mock_sqlite_helper.return_value = mock_sqlite_instance
        
        # Mock semaphore context manager
        mock_semaphore.__enter__ = Mock(return_value=None)
        mock_semaphore.__exit__ = Mock(return_value=None)
        
        mock_driver = Mock()
        mock_setup_driver = Mock(return_value=mock_driver)
        
        mock_crawl_function = Mock(side_effect=Exception("Crawl failed"))
        
        comic = {'Tên truyện': 'Test Comic'}
        params = (comic, '/test/db/path', 'http://example.com', 1)
        
        with caplog.at_level(logging.ERROR):
            result = process_comic_worker(params, mock_crawl_function, mock_setup_driver)
        
        assert result is None
        assert "Lỗi khi xử lý truyện Test Comic" in caplog.text
        
        # Resources should still be cleaned up
        mock_driver.quit.assert_called_once()
        mock_sqlite_instance.close_all_connections.assert_called_once()
    
    @patch('utils.driver_utils.check_system_resources')
    @patch('utils.driver_utils.SQLiteHelper')
    @patch('utils.driver_utils.driver_semaphore')
    def test_process_comic_worker_with_truyenqq_comic_format(self, mock_semaphore, mock_sqlite_helper, mock_check_resources):
        """Test process_comic_worker with TruyenQQ comic format (different key name)"""
        mock_check_resources.return_value = False
        
        # TruyenQQ format uses 'ten_truyen' instead of 'Tên truyện'
        comic = {'ten_truyen': 'TruyenQQ Comic'}
        params = (comic, '/test/db/path', 'http://example.com', 1)
        
        mock_crawl_function = Mock()
        mock_setup_driver = Mock()
        
        result = process_comic_worker(params, mock_crawl_function, mock_setup_driver)
        
        assert result is None
    
    @patch('utils.driver_utils.check_system_resources')
    @patch('utils.driver_utils.SQLiteHelper')
    @patch('utils.driver_utils.driver_semaphore')
    def test_process_comic_worker_resource_cleanup_on_exception(self, mock_semaphore, mock_sqlite_helper, mock_check_resources):
        """Test that resources are cleaned up even when driver.quit() or sqlite.close() fail"""
        mock_check_resources.return_value = True
        mock_sqlite_instance = Mock()
        mock_sqlite_helper.return_value = mock_sqlite_instance
        
        # Mock semaphore context manager
        mock_semaphore.__enter__ = Mock(return_value=None)
        mock_semaphore.__exit__ = Mock(return_value=None)
        
        mock_driver = Mock()
        mock_driver.quit.side_effect = Exception("Quit failed")
        mock_setup_driver = Mock(return_value=mock_driver)
        
        mock_sqlite_instance.close_all_connections.side_effect = Exception("Close failed")
        
        mock_crawl_function = Mock(return_value={'result': 'success'})
        
        comic = {'Tên truyện': 'Test Comic'}
        params = (comic, '/test/db/path', 'http://example.com', 1)
        
        # Should not raise exceptions even if cleanup fails
        result = process_comic_worker(params, mock_crawl_function, mock_setup_driver)
        
        assert result == {'result': 'success'}
        mock_driver.quit.assert_called_once()
        mock_sqlite_instance.close_all_connections.assert_called_once()


class TestIntegration:
    """Integration tests for driver_utils module"""
    
    def test_module_imports_successfully(self):
        """Test that the module can be imported without errors"""
        from utils import driver_utils
        assert hasattr(driver_utils, 'init_process')
        assert hasattr(driver_utils, 'get_text_safe')
        assert hasattr(driver_utils, 'process_comic_worker')
        assert hasattr(driver_utils, 'check_system_resources')
        assert hasattr(driver_utils, 'setup_signal_handlers')
        assert hasattr(driver_utils, 'retry')
    
    def test_logger_is_configured(self):
        """Test that logger is properly configured"""
        from utils.driver_utils import logger
        assert logger.name == 'utils.driver_utils'
    
    @patch('utils.driver_utils.psutil.virtual_memory')
    def test_full_workflow_simulation(self, mock_memory):
        """Test a simplified full workflow"""
        # Mock low memory usage
        mock_memory.return_value.percent = 50
        
        # Test that system resources check passes
        assert check_system_resources() is True
        
        # Test that text extraction works with mocked element
        mock_element = Mock()
        mock_found_element = Mock()
        mock_found_element.text = "Integration Test"
        mock_element.find_elements.return_value = [mock_found_element]
        
        result = get_text_safe(mock_element, ".test")
        assert result == "Integration Test"


if __name__ == "__main__":
    pytest.main([__file__])