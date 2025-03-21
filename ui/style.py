/* Styles cho ứng dụng */
QMainWindow {
    background-color: #f5f5f5;
}

QTabWidget::pane {
    border: 1px solid #cccccc;
    background-color: white;
}

QTabBar::tab {
    background-color: #e0e0e0;
    padding: 8px 20px;
    border: 1px solid #cccccc;
    border-bottom: none;
    border-top-left-radius: 4px;
    border-top-right-radius: 4px;
}

QTabBar::tab:selected {
    background-color: white;
    border-bottom: 1px solid white;
}

QPushButton {
    background-color: #3f51b5;
    color: white;
    border: none;
    padding: 8px 16px;
    border-radius: 4px;
}

QPushButton:hover {
    background-color: #303f9f;
}

QPushButton:pressed {
    background-color: #1a237e;
}

QPushButton:disabled {
    background-color: #bdbdbd;
    color: #757575;
}

QProgressBar {
    border: 1px solid #cccccc;
    border-radius: 3px;
    background-color: #eeeeee;
    text-align: center;
}

QProgressBar::chunk {
    background-color: #3f51b5;
    width: 10px;
}

QTableView {
    border: 1px solid #cccccc;
    gridline-color: #e0e0e0;
    selection-background-color: #e3f2fd;
    selection-color: #212121;
}

QTableView::item {
    padding: 4px;
}

QHeaderView::section {
    background-color: #f5f5f5;
    padding: 4px;
    border: 1px solid #cccccc;
    border-left: none;
}

QComboBox {
    border: 1px solid #cccccc;
    border-radius: 3px;
    padding: 4px 8px;
}

QTextEdit {
    border: 1px solid #cccccc;
    border-radius: 3px;
    background-color: white;
    font-family: 'Consolas', 'Courier New', monospace;
}

QCheckBox {
    spacing: 8px;
}