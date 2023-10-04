import sys
from PyQt6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QHBoxLayout, QWidget, QPushButton, QTextEdit, QLabel, QTextBrowser, QLineEdit, QMessageBox
from PyQt6.QtGui import QColor

class SchedulerWindow(QMainWindow):
    # Set UI 
    def __init__(self):
        super().__init__()
        # Set my Window View
        self.setWindowTitle("myScheduler")
        self.setGeometry(100, 100, 800, 640)
        # Box for the scheduler
        self.scheduler_label = QLabel("Scheduler")
        # self.scheduler_text = QTextEdit()
        self.input_scheduler = QLineEdit()
        self.input_scheduler.returnPressed.connect(self.add_action)
        self.input_scheduler.setMaximumHeight(27)
        # button for handling input and generate scheduler
        self.add_action_button = QPushButton("Add Action")
        self.add_action_button.clicked.connect(self.add_action)
        self.generate_scheduler_button = QPushButton("Generate Scheduler")
        self.generate_scheduler_button.clicked.connect(self.generate_scheduler)    
        # layout for this two button
        button_schedule_layout = QHBoxLayout()
        button_schedule_layout.addWidget(self.add_action_button)
        button_schedule_layout.addWidget(self.generate_scheduler_button)
        # box for displaying generated scheduler
        self.scheduler_output_lab = QLabel("Scheduler generated")
        self.scheduler_output = QTextBrowser() 
        self.scheduler_output.setReadOnly(True) 
        self.scheduler_output.setMaximumHeight(100)
        # Scheduler layout
        scheduler_layout = QVBoxLayout()
        scheduler_layout.addWidget(self.scheduler_label)
        scheduler_layout.addWidget(self.input_scheduler)
        scheduler_layout.addLayout(button_schedule_layout)
        scheduler_layout.addWidget(self.scheduler_output_lab)
        scheduler_layout.addWidget(self.scheduler_output)
        # Scheduler container
        scheduler_container = QWidget()
        scheduler_container.setLayout(scheduler_layout)
        # Box for keep track the status of resources
        self.resources_status_label = QLabel("Status Resources/Transaction at the end")
        self.resources_status_text = QTextEdit()
        # Resource/Transaction Info layout
        resources_status_layout = QVBoxLayout()
        resources_status_layout.addWidget(self.resources_status_label)
        resources_status_layout.addWidget(self.resources_status_text)
        # Resource/Transaction container
        resources_status_container = QWidget()
        resources_status_container.setLayout(resources_status_layout)
        # Box for the actions executed
        self.actions_label = QLabel("Workflow concurrency control")
        self.actions_text = QTextEdit()
        # Layout for the actions executed
        actions_layout = QVBoxLayout()
        actions_layout.addWidget(self.actions_label)
        actions_layout.addWidget(self.actions_text)
        # Container for the action executed
        actions_container = QWidget()
        actions_container.setLayout(actions_layout)
        # Botton to start the concurrency throught timestamp
        self.concurrency_ts_button = QPushButton("Apply concurrency Timestamp")
        self.concurrency_ts_button.clicked.connect(self.apply_timestamp)
        # Bottone for checking serializability
        self.check_serializability_button = QPushButton("Check Serializability")
        self.check_serializability_button.clicked.connect(self.check_serializability)
        # Bottone to delete all
        self.clear_button = QPushButton("Clear All")
        self.clear_button.clicked.connect(self.clear_all)
        # Layout principale
        main_layout = QVBoxLayout()
        main_layout.addWidget(scheduler_container)
        # Second Layout
        second_layout = QHBoxLayout()
        second_layout.addWidget(resources_status_container)
        second_layout.addWidget(actions_container)
        # Functionality Layout
        button_layout = QHBoxLayout()
        button_layout.addWidget(self.concurrency_ts_button)
        button_layout.addWidget(self.check_serializability_button)
        button_layout.addWidget(self.clear_button)
        # Build my window view
        main_layout.addLayout(second_layout)
        main_layout.addLayout(button_layout)
        central_widget = QWidget()
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)

    #-------------------------------------------- FUNCTIONALITY
    def show_error_popup(self, message):
        error_popup = QMessageBox()
        error_popup.setWindowTitle("Error")
        error_popup.setIcon(QMessageBox.Icon.Critical)
        error_popup.setText(message)
        error_popup.exec()

    def clear_all(self):
        # Enable/Disable buttons
        self.add_action_button.setEnabled(True)
        self.input_scheduler.setEnabled(True)
        self.generate_scheduler_button.setEnabled(True)
        # Clear Window GUI
        self.input_scheduler.clear()
        self.resources_status_text.clear()
        self.actions_text.clear()
        self.scheduler_output.clear()
        self.check_serializability_button.setStyleSheet("")
        # Clear variable
        # init_list()

    #--------------------------------------------- INPUT HANDLER
    def add_action(self):
        user_input = self.input_scheduler.text()
        self.process_input(user_input)
        pass

    def display_scheduler(self):
        scheduler_str = "S : {"
        for action in scheduler:
            transaction_id, action_type, resource = action
            if action_type in ["read","write"]:
                scheduler_str += f" {action_type}{transaction_id[1]}({resource}),"
            else : 
                scheduler_str += f" {action_type}{transaction_id[1]},"
        scheduler_str = scheduler_str.rstrip(',')  # Remove the trailing comma
        scheduler_str += " }"
        #print(f"Scheduler Obtained : {scheduler_str}")
        self.scheduler_output.setText(scheduler_str)

    def process_input(self, user_input):
        parts = user_input.split()
        if len(parts) == 2:
            action_type, transaction_id = parts
            if action_type == "commit":
                resource = None
            else: 
                self.show_error_popup("Invalid input format. Please enter 'commit' followed by transaction ID or 'read' or 'write' followed by transaction ID and resource.")
        elif len(parts) == 3 :
            action_type, transaction_id, resource = parts
            if action_type not in ['read', 'write']:
                self.show_error_popup("Invalid action type. Please enter 'read' or 'write'.")
            else:
                transaction_id = "T"+str(transaction_id)
                scheduler.append((transaction_id, action_type, resource))
                #self.list_widget.addItem(f"Added: {user_input}")
                self.display_scheduler()
        else:
            self.show_error_popup("Invalid input format. Please enter 'read' or 'write' followed by transaction ID and resource.")
        
        self.input_scheduler.clear()

    def generate_scheduler(self):
        self.add_action_button.setEnabled(False)
        self.input_scheduler.setEnabled(False)
        self.generate_scheduler_button.setEnabled(False)
        self.display_scheduler()
        #init_variable_for_algorithm()
    
    #---------------------------------------- CONCURRENCY CONTROL
    def apply_timestamp(self):
        # Each time you click on timestamp, reset the usefull list
        pass


    #---------------------------------------- SERIALIZABILITY
    def check_serializability(self):
        # Set our scheduler well.
        scheduler_for_conflict = [elem for elem in scheduler if "commit" not in elem ]
        # Init precedent graph
        precedence_graph = {}
        # Scan the scheduler in order to find the conflict pair for adding edges
        for i in range(len(scheduler_for_conflict)):
            action_i, transaction_i, element_i = scheduler_for_conflict[i]
            if transaction_i not in precedence_graph:
                precedence_graph[transaction_i] = []
            
            for j in range(i + 1, len(scheduler_for_conflict)):
                action_j, transaction_j, element_j = scheduler_for_conflict[j]
                if (
                    transaction_i != transaction_j
                    and element_i == element_j
                    and (action_i == 'write' or action_j == 'write')
                ):
                    if transaction_j not in precedence_graph:
                        precedence_graph[transaction_j] = []
                    precedence_graph[transaction_i].append(transaction_j)

        # Check if the graph is acyclic.
        if self.has_cycle(precedence_graph):
            self.check_serializability_button.setStyleSheet("background-color: red")
        else:
            self.check_serializability_button.setStyleSheet("background-color: green")
    
    # Function for checking if there'are cycle (use DFS search)
    def has_cycle(self,graph):
        visited = set()
        rec_stack = set()
        def dfs(node):
            if node not in visited:
                visited.add(node)
                rec_stack.add(node)
                for neighbor in graph.get(node, []):
                    if neighbor in rec_stack:
                        return True
                    if neighbor not in visited and dfs(neighbor):
                        return True
                rec_stack.remove(node)
            return False

        for node in graph:
            if node not in visited and dfs(node):
                return True

        return False

if __name__ == '__main__':
    # Init GUI
    app = QApplication(sys.argv)
    window = SchedulerWindow()
    
    # Declare variable for algorithm workflow
    scheduler = []

    # Show GUI
    window.show()


    sys.exit(app.exec())
