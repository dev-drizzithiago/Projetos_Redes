
import sys
from PySide6.QtWidgets import QApplication, QWidget, QLabel, QVBoxLayout
from PySide6.QtCore import Qt, QTimer

class PopUp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Aviso Importante")
        self.setWindowFlags(Qt.WindowStaysOnTopHint | Qt.CustomizeWindowHint | Qt.FramelessWindowHint)
        self.setFixedSize(300, 150)

        layout = QVBoxLayout()
        label = QLabel('Alef, você já bateu o seu ponto?.')
        label.setAlignment(Qt.AlignCenter)
        layout.addWidget(label)
        self.setLayout(layout)

        QTimer.singleShot(30_000, self.close)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = PopUp()
    window.show()
    sys.exit(app.exec())
