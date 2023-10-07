import sys
from PyQt6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QHBoxLayout, QWidget, QPushButton, QTextEdit, QLabel, QTextBrowser, QLineEdit, QMessageBox, QCheckBox
from PyQt6.QtGui import QColor
import itertools,random


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
        
        # Box for the scheduler  ------------------------------- GUI INPUT
        self.scheduler_label = QLabel("Insert actions")
        # Bottone to delete all
        self.clear_button = QPushButton("Clear All")
        self.clear_button.clicked.connect(self.clear_all)
        header_layout = QHBoxLayout()
        header_layout.addWidget(self.scheduler_label)
        header_layout.addStretch(1)
        header_layout.addWidget(self.clear_button)
        # Space for input handler
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
        # Layout for the input scheduler
        scheduler_layout = QVBoxLayout()
        scheduler_layout.addLayout(header_layout)
        scheduler_layout.addWidget(self.input_scheduler)
        scheduler_layout.addLayout(button_schedule_layout)
        scheduler_layout.addWidget(self.scheduler_output_lab)
        scheduler_layout.addWidget(self.scheduler_output)
        # Container for the input scheduler
        input_container = QWidget()
        input_container.setLayout(scheduler_layout)

        # Botton to start the concurrency throught timestamp ------------------------------- GUI CONCURRENCY CONTROL
        self.concurrency_ts_button = QPushButton("Apply Concurrency Control Strategy")
        self.concurrency_ts_button.clicked.connect(self.apply_timestamp_from_bt)
        self.concurrency_ts_button.setFixedWidth(250)
        self.concurrency_ts_button.setEnabled(False)
        self.deadlock_solution_switch = QCheckBox("Deadlock Solution On/Off")
        self.deadlock_solution_switch.toggled.connect(self.toggle_deadlock_solution) 
        self.deadlock_solution_switch.setChecked(False)
        # Layout concurrency button functionality
        concurrency_buttons_layout = QHBoxLayout()
        concurrency_buttons_layout.addWidget(self.concurrency_ts_button)
        concurrency_buttons_layout.addWidget(self.deadlock_solution_switch)

        # Box for keep track the status of resources
        self.resources_status_label = QLabel("Status Resources/Transaction at the end")
        self.resources_status_text = QTextEdit()
        self.resources_status_text.setReadOnly(True)
        # Resource/Transaction Info layout
        resources_status_layout = QVBoxLayout()
        resources_status_layout.addWidget(self.resources_status_label)
        resources_status_layout.addWidget(self.resources_status_text)
        # Resource/Transaction container
        # resources_status_container = QWidget()
        # resources_status_container.setLayout(resources_status_layout)
        # Box for the actions executed
        self.actions_label = QLabel("Workflow concurrency control")
        self.actions_text = QTextEdit()
        self.actions_text.setReadOnly(True)
        # Layout for the actions executed
        actions_layout = QVBoxLayout()
        actions_layout.addWidget(self.actions_label)
        actions_layout.addWidget(self.actions_text)
        # Layout for the output
        concurrency_output_layout = QHBoxLayout()
        concurrency_output_layout.addLayout(resources_status_layout)
        concurrency_output_layout.addLayout(actions_layout)
        # Layout for the concurrency section
        concurrency_layout = QVBoxLayout()
        concurrency_layout.addLayout(concurrency_buttons_layout)
        concurrency_layout.addLayout(concurrency_output_layout)

        # Container for the action
        # actions_container = QWidget()
        # actions_container.setLayout(actions_layout)
        # Container for the concurrency section
        concurrency_container = QWidget()
        concurrency_container.setLayout(concurrency_layout)

        # Bottone for checking conflict serializability ------------------------------- GUI SERIALIZABILITY
        self.check_conflict_s_button = QPushButton("Check Conflict Serializability")
        self.check_conflict_s_button.clicked.connect(self.check_serializability_bt)
        self.check_conflict_s_button.setEnabled(False)
        # Botton for checking view serializability
        self.check_view_s_button = QPushButton("Check View Serializability")
        self.check_view_s_button.clicked.connect(self.check_view_serializability)
        self.check_view_s_button.setEnabled(False)
        # Layout for the bottom
        serializability_b_layout = QHBoxLayout()
        serializability_b_layout.addWidget(self.check_conflict_s_button)
        serializability_b_layout.addWidget(self.check_view_s_button)
        # Display the output of serializability
        self.conflict_label = QLabel("Conflict Equivalent Scheduler")
        self.conflict_text = QTextEdit()
        self.conflict_text.setMaximumHeight(40)
        self.conflict_text.setReadOnly(True)
        self.view_label = QLabel("View Equivalent Scheduler")
        self.view_text = QTextEdit()
        self.view_text.setMaximumHeight(40)
        self.view_text.setReadOnly(True)
        # Layout of the output both for conflict and view-serializability
        conflict_output_layout = QVBoxLayout()
        conflict_output_layout.addWidget(self.conflict_label)
        conflict_output_layout.addWidget(self.conflict_text)
        view_output_layout = QVBoxLayout()
        view_output_layout.addWidget(self.view_label)
        view_output_layout.addWidget(self.view_text)
        serializability_output_layout = QHBoxLayout()
        serializability_output_layout.addLayout(conflict_output_layout)
        serializability_output_layout.addLayout(view_output_layout)
        # Layout della serializability section
        serializability_layout = QVBoxLayout()
        serializability_layout.addLayout(serializability_b_layout)
        serializability_layout.addLayout(serializability_output_layout)

        # Container per la serializability sectionß
        serializability_container = QWidget()
        serializability_container.setLayout(serializability_layout)

        # Assembly layout principale
        main_layout = QVBoxLayout()
        main_layout.addWidget(input_container)
        main_layout.addWidget(concurrency_container)
        main_layout.addWidget(serializability_container)
        #Final container
        central_widget = QWidget()
        central_widget.setLayout(main_layout)

        self.setCentralWidget(central_widget)

    #-------------------------------------------- UI FUNCTIONALITY
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
        self.check_conflict_s_button.setEnabled(False)
        self.check_view_s_button.setEnabled(False)
        # Clear Window GUI
        self.input_scheduler.clear()
        self.resources_status_text.clear()
        self.actions_text.clear()
        self.scheduler_output.clear()
        self.check_conflict_s_button.setStyleSheet("")
        self.check_view_s_button.setStyleSheet("")
        self.conflict_text.clear()
        self.view_text.clear()
        # Clear variables
        init_variable()

    def toggle_deadlock_solution(self):
        global flag_deadlock_solution
        flag_deadlock_solution = not flag_deadlock_solution
    
    def apply_timestamp_from_bt(self):
        self.concurrency_ts_button.setEnabled(False)
        self.apply_timestamp(scheduler)
  
    def check_serializability_bt(self):
        is_conflict, serial_schedule = self.check_serializability()
        print(is_conflict,serial_schedule)
        # Check if the graph is acyclic.
        if not is_conflict:
            self.check_conflict_s_button.setStyleSheet("background-color: red")
            self.check_conflict_s_button.setEnabled(False)
            self.conflict_text.setText("No conflict-serializable")
        else:
            self.check_conflict_s_button.setStyleSheet("background-color: green")
            self.check_view_s_button.setStyleSheet("background-color: green")
            self.check_conflict_s_button.setEnabled(False)
            self.check_view_s_button.setEnabled(False)
            inverted_list = serial_schedule[::-1]
            self.conflict_text.setText(str(inverted_list))
        
    #--------------------------------------------- INPUT HANDLER
    def add_action(self):
        user_input = self.input_scheduler.text()
        self.process_input(user_input)
        pass

    def display_scheduler(self,input_scheduler):
        scheduler_for_output = list(input_scheduler)   
        scheduler_str = "S : {"
        for action in scheduler_for_output:
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
            if action_type not in ['commit','rollback']:
                self.show_error_popup("Invalid input format. Please enter 'commit' followed by transaction ID or 'read' or 'write' followed by transaction ID and resource.")
            else: 
                resource = None
                transaction_id = "T"+str(transaction_id)
                scheduler.append((transaction_id, action_type, resource))
                self.display_scheduler(scheduler)
            
        elif len(parts) == 3 :
            action_type, transaction_id, resource = parts
            if action_type not in ['read', 'write']:
                self.show_error_popup("Invalid action type. Please enter 'read' or 'write'.")
            else:
                transaction_id = "T"+str(transaction_id)
                scheduler.append((transaction_id, action_type, resource))
                #self.list_widget.addItem(f"Added: {user_input}")
                self.display_scheduler(scheduler)
        else:
            self.show_error_popup("Invalid input format. Please enter 'read' or 'write' followed by transaction ID and resource.")
        
        self.input_scheduler.clear()

    def generate_scheduler(self):
        self.add_action_button.setEnabled(False)
        self.input_scheduler.setEnabled(False)
        self.generate_scheduler_button.setEnabled(False)
        self.concurrency_ts_button.setEnabled(True)
        self.check_conflict_s_button.setEnabled(True)
        self.check_view_s_button.setEnabled(True)
        self.display_scheduler(scheduler)
    
    #---------------------------------------- CONCURRENCY CONTROL
    def setDeadLock(self):
        global deadlock_f
        deadlock_f = True
        
    def deadlock_solution(self,transactionID_1, transactionID_2):
        global deadlock_s_exec
        global deadlock_solution
        global ignored_actions
        global rollback_transaction 
        global deadlock_detector
        global resource_info 
        global transaction_info
        ignored_actions = []
        rollback_transaction = []
        deadlock_detector = []
        resource_info = []
        transaction_info = []
        # For each transaction generate ad assign a priority number.
        priority_dictionary = {}
        generated_numbers = set()

        list_transaction = set([tupla[0] for tupla in scheduler])
        for elem in list_transaction:
            priority_dictionary[elem] = random.randint(1, 100)
            while True:
                random_num = random.randint(1, 100)
                # we are sure that the generated number is not equal to others
                if random_num not in generated_numbers:
                    generated_numbers.add(random_num)
                    priority_dictionary[elem] = random_num
                    break  

        # Remove the text from UI
        self.resources_status_text.clear()
        self.actions_text.clear()
        self.scheduler_output.clear()
       
        if( priority_dictionary[transactionID_1] > priority_dictionary[transactionID_2] ):
            # print("Kill transaction :",trans1)
            deadlock_solution = [tupla for tupla in scheduler if transactionID_1 not in tupla]
            deadlock_s_exec = True
            self.display_scheduler(deadlock_solution)
            self.apply_timestamp(deadlock_solution)
        else:
            # print("Kill transaction :",transactionID_2)
            deadlock_solution = [tupla for tupla in scheduler if transactionID_2 not in tupla]
            deadlock_s_exec = True
            self.display_scheduler(deadlock_solution)
            self.apply_timestamp(deadlock_solution)
        

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
                    self.setDeadLock()
                    text = "DEADLOCK"
                    self.actions_text.append(text)
                    if flag_deadlock_solution == True:
                        self.deadlock_solution(transactionInDeadLockList_ID,transaction_w)
                    else:
                        message_error = ""
                        message_error += f"DeadLock Detected over the transaction {transactionID_in_conflit} and {transaction_w} "
                        self.show_error_popup(message_error)
                else:
                    print("No, the other transaction is waiting for the commit fo the transaction : ",transactionInDeadLockList_ID)

    def apply_timestamp(self,input_scheduler):
        scheduler_to_use = list(input_scheduler)
        # Generate a list without duplicates of the needed element
        list_resource = set([tupla[2] for tupla in scheduler_to_use if tupla[2] is not None])
        list_transaction = set([tupla[0] for tupla in scheduler_to_use])
        # Create list of object class.
        for elem in list_resource:
            resource_info.append(ResourceInfo(elem))
        for elem in list_transaction:
            transaction_info.append(Transaction(elem))

        if not deadlock_s_exec :
            # Apply timestamp_rules over the new scheduler
            for elem in scheduler_to_use:
                if not deadlock_f:
                    self.apply_rules(elem)
                    if deadlock_f == False:
                        for elem in resource_info:
                            text_to_add = vars(elem)
                            self.resources_status_text.append(str(text_to_add))
                        self.resources_status_text.append("-----------------------------")
            if not deadlock_f:
                for elem in transaction_info:
                    text_to_add = vars(elem)
                    self.resources_status_text.append(str(text_to_add))
        else:
            # Apply timestamp_rules over the new scheduler
            for elem in scheduler_to_use:
                self.apply_rules(elem)
                for elem in resource_info:
                    text_to_add = vars(elem)
                    self.resources_status_text.append(str(text_to_add))
                self.resources_status_text.append("-----------------------------")
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
                        self.check_deadlock(( transaction , action_type, resource ))

                elif (transaction_ts >= resource_info[resource_index].rts) and (transaction_ts < resource_info[resource_index].wts):
                    if resource_info[resource_index].cb == True:
                        text = "Action :"+action_type+" Transaction :"+transaction+" over element :"+resource+" STATUS = IGNORE - THOMAS RULE "   
                        self.actions_text.append(text)
                    else:
                        text = "Action :"+action_type+" Transaction :"+transaction+" over element :"+resource+" STATUS = WAITING - THOMAS RULE "
                        self.actions_text.append(text)
                        #add the action that generate waiting into deadlock_list_detector
                        deadlock_detector.append(( transaction , action_type, resource ))
                        transaction_info[transaction_index].waiting = True
                        ignored_actions.append(( transaction , action_type, resource ))   
                        self.check_deadlock(( transaction , action_type, resource ))

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
                        self.check_deadlock(( transaction , action_type, resource ))

                else:
                    text = "Action :"+action_type+" Transaction :"+transaction+" over element :"+resource+" STATUS = ROLLBACK "
                    self.actions_text.append(text)
                    #add the transaction that need to be rollbacked
                    rollback_transaction.append(transaction)
                    transaction_info[transaction_index].rollback = True 
                    self.rollback(transaction_ts)

            elif action_type == "commit":
                text = "Action :"+action_type+" Transaction :"+transaction+" COMMIT EVENT "
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

            elif action_type == "rollback":
                text = "Action :"+action_type+" Transaction :"+transaction+" ROLLBACK EVENT "
                self.actions_text.append(text)
                # Now we need to check if some other actions are in waiting for this commit.
                self.rollback(transaction_ts)

    
    # function used for the transaction that are in waiting for cb("Any resource") = True
    def check_waiting(self,resource_to_check):
        global deadlock_detector
        #print("La risorsa che è stata trasformata in True è : ", resource_to_check)
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
        scheduler_for_conflict = [elem for elem in scheduler if "commit" not in elem and "rollback" not in elem ]
        # Init precedent graph
        precedence_graph = {}
        # Scan the scheduler in order to find the conflict pair for adding edges
        for i in range(len(scheduler_for_conflict)):
            transaction_i, action_i, element_i = scheduler_for_conflict[i]
            if transaction_i not in precedence_graph:
                precedence_graph[transaction_i] = set()

            for j in range(i + 1, len(scheduler_for_conflict)):
                transaction_j, action_j, element_j = scheduler_for_conflict[j]
                # check conflict pair in order to add edges
                if (transaction_i != transaction_j and element_i == element_j and (action_i == "write" or action_j =="write")):
                    #if transaction_j not in precedence_graph:
                    #precedence_graph[transaction_j] = []
                    precedence_graph[transaction_i].add(transaction_j)
        # Function for checking if there'are cycle (use DFS search)
        def has_cycle(node, visited, rec_stack, order):
            visited[node] = True
            rec_stack[node] = True

            for neighbor in precedence_graph.get(node, []):
                if not visited[neighbor]:
                    if has_cycle(neighbor, visited, rec_stack,order):
                        return True
                elif rec_stack[neighbor]:
                    return True

            rec_stack[node] = False
            order.append(node)
            return False

        # Verifica se il grafo di precedenza ha cicli
        visited = {transaction: False for transaction in precedence_graph}
        rec_stack = {transaction: False for transaction in precedence_graph}
        topological_order = []
        for transaction, adjacents in precedence_graph.items():
            print(f"{transaction}: {adjacents}")
        for transaction in precedence_graph:
            if not visited[transaction]:
                if has_cycle(transaction, visited, rec_stack,topological_order):
                    return False, [] # Lo schedule non è conflict-serializable

        return True,topological_order  # Lo schedule è conflict-serializable
        

    def check_view_serializability(self):
        # Set our scheduler well.
        scheduler_for_conflict = [elem for elem in scheduler if "commit" not in elem and "rollback" not in elem ]
        # Calculate the set of the current scheduler
        read_from, final_write = self.extract_read_from_final_write(scheduler_for_conflict)
        # Create a set without duplicate of all the transaction_id in the scheduler
        list_transaction = set([tupla[0] for tupla in scheduler_for_conflict])
        # Generate all permutation possible with the different transaction_id present in the schedule
        all_permutations = itertools.permutations(list_transaction)

        # iterate over all the permutation, we will stop at the first occurency
        for permuted_transactions in all_permutations:
            permuted_schedule = []
            # Create new scheduler in base of the current permutation of transaction
            for t_id in permuted_transactions:
                t_actions = [action for action in scheduler_for_conflict if action[0] == t_id]
                permuted_schedule.extend(t_actions)
            
            #calculate the set of the read from and final write serial scheduler obtained from the combination of permutation
            permuted_read_from, permuted_final_write = self.extract_read_from_final_write(permuted_schedule)

            read_from_to_check = set(read_from)
            final_write_to_check = set(final_write)
            read_from_permuted = set(permuted_read_from)
            final_Write_permuted = set(final_write)
            
            if read_from_to_check == read_from_permuted and final_write_to_check == final_Write_permuted:
                self.check_view_s_button.setStyleSheet("background-color: green")
                serial_scheduler = ""
                serial_scheduler += f"{permuted_transactions}"
                self.view_text.setText(serial_scheduler)
                self.check_view_s_button.setEnabled(False)
                break
            else:
                self.check_view_s_button.setStyleSheet("background-color: red")
                self.view_text.setText("No view-serializable")
                self.check_view_s_button.setEnabled(False)
                break

    def extract_read_from_final_write(self,schedule):
        read_from = []
        final_write = []

        for i, (transaction_id, action_name, resource) in enumerate(schedule):
            if action_name == "read":
                j = i - 1
                for j in range(i - 1, -1, -1):
                    #print("Nostra azione : ",(transaction_id, action_name, resource))
                    #print("Azione dello scheduler :",(schedule[j][0],schedule[j][1],schedule[j][2]))
                    if (schedule[j][2] == resource and schedule[j][1] == "write" and schedule[j][0] != transaction_id ):
                        read_from.append((transaction_id,action_name,resource,schedule[j][0],schedule[j][1],schedule[j][2]))
            if action_name == "write":
                j = i - 1
                if(schedule[j][2] == resource):
                    #create a new list without the tupla with the resource == schedule[j][2]
                    final_write = [(resource, transaction_id) for resource, _ in final_write if resource != schedule[j][2]]
                    #add element
                    final_write.append((resource,schedule[j][0]))
                else:
                    final_write.append((resource,transaction_id))
        return read_from, final_write


def init_variable():
    global scheduler
    global ignored_actions
    global rollback_transaction 
    global deadlock_detector
    global resource_info 
    global transaction_info
    global deadlock_f
    scheduler = []
    ignored_actions = []
    rollback_transaction = []
    deadlock_detector = []
    resource_info = []
    transaction_info = []
    deadlock_f = False

    #Deadlock Solution
    global scheduler_solution
    global deadlock_s_exec
    deadlock_s_exec = False
    scheduler_solution = []

# Global variable changed by UI interaction
global flag_deadlock_solution
flag_deadlock_solution = False

if __name__ == '__main__':
    # Declare variable for algorithm workflow
    init_variable()
    # Init GUI
    app = QApplication(sys.argv)
    window = SchedulerWindow()
    # Show GUI
    window.show()
    sys.exit(app.exec())
