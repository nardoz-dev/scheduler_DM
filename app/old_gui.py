import sys
from PyQt6.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QListWidget, QLineEdit, QPushButton, QLabel
import networkx as nx
import matplotlib.pyplot as plt

class SchedulerApp(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Scheduler Visualizer")
        self.setGeometry(100, 100, 500, 600)

        central_widget = QWidget(self)
        self.setCentralWidget(central_widget)

        layout = QVBoxLayout()

        self.list_widget = QListWidget()
        layout.addWidget(self.list_widget)

        input_layout = QVBoxLayout()

        self.input_field = QLineEdit()
        self.input_field.returnPressed.connect(self.add_action_enter)  # Connect to Enter key press
        input_layout.addWidget(self.input_field)

        self.add_button = QPushButton("Add Action")
        self.add_button.clicked.connect(self.add_action)
        input_layout.addWidget(self.add_button)

        layout.addLayout(input_layout)

        self.generate_button = QPushButton("Generate Scheduler")
        self.generate_button.clicked.connect(self.generate_scheduler)
        layout.addWidget(self.generate_button)

        self.check_serializable_button = QPushButton("Check Serializable")
        self.check_serializable_button.clicked.connect(self.check_serializable)
        layout.addWidget(self.check_serializable_button)

        self.scheduler_label = QLabel()
        layout.addWidget(self.scheduler_label)

        central_widget.setLayout(layout)

        self.scheduler = []
        self.precedence_graph = None  # Store the precedence graph

    def add_action(self):
        user_input = self.input_field.text()
        self.process_input(user_input)

    def add_action_enter(self):
        user_input = self.input_field.text()
        self.process_input(user_input)

    def process_input(self, user_input):
        parts = user_input.split()
        
        if len(parts) == 3:
            action_type, transaction_id, resource = parts
            transaction_id = int(transaction_id)

            if action_type not in ['read', 'write']:
                self.list_widget.addItem("Invalid action type. Please enter 'read' or 'write'.")
            else:
                self.scheduler.append((action_type, transaction_id, resource))
                self.list_widget.addItem(f"Added: {user_input}")
        else:
            self.list_widget.addItem("Invalid input format. Please enter 'read' or 'write' followed by transaction ID and resource.")
        
        self.input_field.clear()

    def generate_scheduler(self):
        scheduler_str = "S : {"
        for action in self.scheduler:
            action_type, transaction_id, resource = action
            scheduler_str += f" {action_type}{transaction_id}({resource}),"
        scheduler_str = scheduler_str.rstrip(',')  # Remove the trailing comma
        scheduler_str += " }"
        self.scheduler_label.setText(scheduler_str)

    def check_serializable(self):
        # Implement conflict serializability check logic here
        is_serializable, precedence_graph = self.is_conflict_serializable(self.scheduler)

        if is_serializable:
            self.check_serializable_button.setStyleSheet("background-color: green")
        else:
            self.check_serializable_button.setStyleSheet("background-color: red")

        self.precedence_graph = precedence_graph

    def is_conflict_serializable(self, schedule):
        # Create a precedence graph with nodes for each unique transaction
        transactions = set(action[1] for action in schedule)
        graph = {t: set() for t in transactions}

        for i in range(len(schedule)):
            for j in range(i + 1, len(schedule)):
                if self.is_conflict_pair(schedule[i], schedule[j]):
                    graph[schedule[j][1]].add(schedule[i][1])  # Edge from j to i

        # Check for cycles in the precedence graph using depth-first search
        visited = set()
        stack = []

        def has_cycle(node):
            if node in stack:
                return True
            if node in visited:
                return False

            stack.append(node)
            visited.add(node)

            for neighbor in graph[node]:
                if has_cycle(neighbor):
                    return True

            stack.pop()
            return False

        for transaction in transactions:
            if transaction not in visited:
                if has_cycle(transaction):
                    return False, None

        # Convert the graph to networkx format for visualization
        nx_graph = nx.DiGraph(graph)
        return True, nx_graph

    def is_conflict_pair(self, action1, action2):
        _, transaction_id1, resource1 = action1
        _, transaction_id2, resource2 = action2

        return transaction_id1 != transaction_id2 and resource1 == resource2 and ("write" in action1 or "write" in action2)

def main():
    app = QApplication(sys.argv)
    window = SchedulerApp()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
