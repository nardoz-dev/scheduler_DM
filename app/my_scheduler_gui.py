import sys
from PyQt6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QHBoxLayout, QWidget, QPushButton, QTextEdit, QLabel, QTextBrowser, QLineEdit, QMessageBox
from PyQt6.QtGui import QColor

class Transaction:
    def __init__(self, name):
        self.name = name
        self.active = True
        self.rollback = False
        self.waiting = False

class ResourceInfo:
    def __init__(self,name):
        self.name = name
        self.rts = 0
        self.wts = 0
        self.wts_c = 0
        self.cb = True

class SchedulerWindow(QMainWindow):
    # Set UI 
    def __init__(self):
        super().__init__()
        # Set my Window View
        self.setWindowTitle("myScheduler")
        self.setGeometry(100, 100, 910, 630)
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
        self.resources_status_text.setReadOnly(True)
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
        self.actions_text.setReadOnly(True)
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
        self.concurrency_ts_button.setEnabled(False)
        # Bottone for checking serializability
        self.check_serializability_button = QPushButton("Check Serializability")
        self.check_serializability_button.clicked.connect(self.check_serializability)
        self.check_serializability_button.setEnabled(False)
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
        self.concurrency_ts_button.setEnabled(False)
        self.check_serializability_button.setEnabled(False)
        # Clear Window GUI
        self.input_scheduler.clear()
        self.resources_status_text.clear()
        self.actions_text.clear()
        self.scheduler_output.clear()
        self.check_serializability_button.setStyleSheet("")
        # Clear variables
        init_variable()
    
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
                transaction_id = "T"+str(transaction_id)
                scheduler.append((transaction_id, action_type, resource))
                self.display_scheduler()
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
        self.concurrency_ts_button.setEnabled(True)
        self.check_serializability_button.setEnabled(True)
        self.display_scheduler()
        #init_variable_for_algorithm()
    
    #---------------------------------------- CONCURRENCY CONTROL
    def apply_timestamp(self):
        global scheduler

        # Generate a list without duplicates of the needed element
        list_resource = set([tupla[2] for tupla in scheduler if tupla[2] is not None])
        list_transaction = set([tupla[0] for tupla in scheduler])
        for elem in list_resource:
            resource_info.append(ResourceInfo(elem))
        for elem in list_transaction:
            transaction_info.append(Transaction(elem))
        
        # Apply timestamp_rules over schedules
        for elem in scheduler:
            self.apply_rules(elem)

        # Print results
        for elem in resource_info:
            text_to_add = vars(elem)
            self.resources_status_text.append(str(text_to_add))
        for elem in transaction_info:
            text_to_add = vars(elem)
            self.resources_status_text.append(str(text_to_add))

    # Let's see the application of concurrency through ts.
    def apply_rules(self, elem):  
        global ignored_actions
        global rollback_transaction
        global deadlock_detector

        transaction , action_type, resource = elem[0],elem[1],elem[2]
        # Check if we need to ignore some action because transaction is in waiting or rollback
        flag_rollback =  any(transaction in element_2 for element_2 in rollback_transaction)
        if(any(transaction in element for element in deadlock_detector) or flag_rollback): 
            if not flag_rollback:
                ignored_actions.append(elem)
        else:
            # We get the index of the current resource and the current index of transaction of the current action in analisys
            resource_index = None
            for index, elem in enumerate(resource_info):
                if elem.name == resource:
                    resource_index = index
                    break
            transaction_index = None
            for index, elem in enumerate(transaction_info):
                if elem.name == transaction:
                    transaction_index = index
                    break

            # Get the timestamp of the current transaction that belongs to the current action
            transaction_ts = int(transaction[1])
            if action_type == "write":
                if (transaction_ts >= resource_info[resource_index].rts) and (transaction_ts  >= resource_info[resource_index].wts):
                    if resource_info[resource_index].cb == True:
                        resource_info[resource_index].wts = transaction_ts
                        resource_info[resource_index].cb = False
                        text = "Action :"+action_type+" Transaction :"+transaction+" over element :"+resource+" STATUS = OK "
                        self.actions_text.append(text)
                    else:
                        text = "Action :"+action_type+" Transaction :"+transaction+" over element :"+resource+" STATUS = WAITING "
                        self.actions_text.append(text)
                        #add the action that generate waiting into deadlock_list_detector
                        deadlock_detector.append(( transaction , action_type, resource ))
                        transaction_info[transaction_index].waiting = True   
                        ignored_actions.append(( transaction , action_type, resource ))
                        check_deadlock(( transaction , action_type, resource ))

                elif (transaction_ts >= resource_info[resource_index].rts) and (transaction_ts < resource_info[resource_index].wts):
                    if resource_info[resource_index].cb == True:
                        text = "Action :"+action_type+" Transaction :"+transaction+" over element :"+resource+" STATUS = IGNORE - THOMAS RULE "
                        ignored_actions.append(( transaction , action_type, resource ))   
                        self.actions_text.append(text)
                    else:
                        text = "Action :"+action_type+" Transaction :"+transaction+" over element :"+resource+" STATUS = WAITING - THOMAS RULE "
                        self.actions_text.append(text)
                        #add the action that generate waiting into deadlock_list_detector
                        deadlock_detector.append(( transaction , action_type, resource ))
                        transaction_info[transaction_index].waiting = True
                        ignored_actions.append(( transaction , action_type, resource ))   
                        check_deadlock(( transaction , action_type, resource ))

                else:
                    text = "Action :"+action_type+" Transaction :"+transaction+" over element :"+resource+" STATUS = ROLLBACK "
                    self.actions_text.append(text)
                    #add the transaction that need to be rollbacked
                    rollback_transaction.append(transaction)
                    transaction_info[transaction_index].rollback = True 
                    self.rollback(transaction_ts)

            elif action_type == "read":
                if (transaction_ts >= resource_info[resource_index].wts):
                    if (resource_info[resource_index].cb == True) or (transaction_ts  == resource_info[resource_index].wts):
                        text = "Action :"+action_type+" Transaction :"+transaction+" over element :"+resource+" STATUS = OK "
                        self.actions_text.append(text)
                        resource_info[resource_index].rts = max(transaction_ts, resource_info[resource_index].rts)
                    else:
                        text = "Action :"+action_type+" Transaction :"+transaction+" over element :"+resource+" STATUS = WAITING "
                        self.actions_text.append(text)
                        #add the action that generate waiting into deadlock_list_detector
                        deadlock_detector.append(( transaction , action_type, resource ))
                        transaction_info[transaction_index].waiting = True
                        ignored_actions.append(elem)   
                        check_deadlock(( transaction , action_type, resource ))

                else:
                    text = "Action :"+action_type+" Transaction :"+transaction+" over element :"+resource+" STATUS = ROLLBACK "
                    self.actions_text.append(text)
                    #add the transaction that need to be rollbacked
                    rollback_transaction.append(transaction)
                    transaction_info[transaction_index].rollback = True 
                    self.rollback(transaction_ts)

            elif action_type == "commit":
                text = "Action :"+action_type+" Transaction :"+transaction+" STATUS = ROLLBACK "
                self.actions_text.append(text)
                # We can check in the array of the datainfo objects, if there's resources where the last write have the ts = to the ts of the commit, it means is the last transaction that wrote in this element,
                # so we need to set it to true
                resource_to_check = None
                for index, elem in enumerate(resource_info):
                    if elem.wts == transaction_ts :
                        elem.cb = True
                        elem.wts_c = transaction_ts
                        resource_to_check = elem.name
                # Now we need to check if some other actions are in waiting for this commit.
                self.check_waiting(resource_to_check)

    def check_deadlock(self,elem):
        #Invoked whenever an action is added to the deadlock_list transactions in waiting.
        transaction_w, action_w, resource_w = elem[0],elem[1],elem[2]
        # Get the transaction ID in which the current action is waiting for
        transactionID_in_conflit = None
        for index, elem in enumerate(resource_info):
            if elem.name == resource_w:
                transactionID_in_conflit = elem.wts
                resource_index = index
                break
        if transactionID_in_conflit != None : 
            transactionID_in_conflit = "T"+str(transactionID_in_conflit)
            if(any(transactionID_in_conflit in tupla for tupla in deadlock_detector)):
                #print("Action that has generated waiting :",transaction_w,"is waiting for :",transactionID_in_conflit)
                #print("It's a match, see if also :",transactionID_in_conflit,"is waiting for :",transaction_w)
                transactionInDeadLockList_ID = transactionID_in_conflit
                if(any(transactionInDeadLockList_ID in elem for elem in deadlock_detector)):
                    #print("DEADLOCK")
                    setDeadLock()
                    message_error += f"DeadLock Detected over the transaction {transactionID_in_conflit} and {transaction_w} "
                    self.show_error_popup("Message Error")
                else:
                    print("No, the other transaction is waiting for the commit fo the transaction : ",transactionInDeadLockList_ID)

    def setDeadLock(self):
        global deadlock_f
        deadlock_f = True

    # function used for the transaction that are in waiting for cb("Any resource") = True
    def check_waiting(self,resource_to_check):
        global deadlock_detector
        print("La risorsa che è stata trasformata in True è : ", resource_to_check)

        # Remember that in order to check if the action is in waiting for the right resource we saved the action that caused waiting
        # in this special list . (In fact is also used for check if there's deadlock)
        #if any(resource_to_check in elem for elem in deadlock_detector): 

        # Get the Transaction that is in waiting list in which the resource is set to True
        transaction = None
        for elem in deadlock_detector:
            resource = elem[2]
            if resource == resource_to_check:
                transaction = elem[0]

        if transaction is not None :
            # Create a new list that contain all the tuple of the Transaction in the waiting list .
            new_schedule = [tupla for tupla in ignored_actions if transaction in tupla]
            # Remove the element in the deadlock_list because now it is processed
            deadlock_detector = [tupla for tupla in deadlock_detector if resource_to_check not in tupla]
            # Remove waiting status from the transaction 
            for index, elem in enumerate(transaction_info):
                if elem.name == transaction:
                    elem.waiting = False

            # Process the related action 
            for elem in new_schedule:
                #print(elem)
                self.apply_rules(elem)
                ignored_actions.remove(elem)

    #Function for rollback, we need to know the ts of the transaction that generate rollback
    #In order to use the same trick of the commit to update the value wts with wts_c i.e., the
    #timestamp of the transaction Tj that wrote the element before the current one, and set cb(X) to true (indeed, Tj has surely committed)
    def rollback(self,transaction_ts):
        resource_to_check = None
        for index, elem in enumerate(resource_info):
            if elem.wts == transaction_ts :
                elem.wts = elem.wts_c
                elem.cb = True
                resource_to_check = elem.name
                # Now we need to check if some other transaction is in waiting , each time we put to True a new variable
                self.check_waiting(resource_to_check)


    #---------------------------------------- SERIALIZABILITY
    def check_serializability(self):
        # Set our scheduler well.
        scheduler_for_conflict = [elem for elem in scheduler if "commit" not in elem ]
        # Init precedent graph
        precedence_graph = {}
        # Scan the scheduler in order to find the conflict pair for adding edges
        for i in range(len(scheduler_for_conflict)):
            transaction_i, action_i, element_i = scheduler_for_conflict[i]
            if transaction_i not in precedence_graph:
                precedence_graph[transaction_i] = []

            for j in range(i + 1, len(scheduler_for_conflict)):
                transaction_j, action_j, element_j = scheduler_for_conflict[j]
                # check conflict pair in order to add edges
                if ( transaction_i != transaction_j and element_i == element_j and (action_i == 'write' or action_j == 'write') ):
                    if transaction_j not in precedence_graph:
                        precedence_graph[transaction_j] = []
                    precedence_graph[transaction_i].append(transaction_j)
       
        #for transaction, adjacents in precedence_graph.items():
        #    print(f"{transaction}: {adjacents}")

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



def init_variable():
    global scheduler
    global ignored_actions
    global rollback_transaction 
    global deadlock_detector
    global resource_info 
    global transaction_info
    global
    scheduler = []
    ignored_actions = []
    rollback_transaction = []
    deadlock_detector = []
    resource_info = []
    transaction_info = []
    deadlock_f = False

if __name__ == '__main__':
    # Init GUI
    app = QApplication(sys.argv)
    window = SchedulerWindow()
    
    # Declare variable for algorithm workflow
    init_variable()


    # Show GUI
    window.show()
    sys.exit(app.exec())
