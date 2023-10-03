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

# function used for the transaction that are in waiting for cb("Any resource") = True
def check_waiting(resource_to_check):
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
            apply_rules(elem)
            ignored_actions.remove(elem)

#Function for rollback, we need to know the ts of the transaction that generate rollback
#In order to use the same trick of the commit to update the value wts with wts_c i.e., the
#timestamp of the transaction Tj that wrote the element before the current one, and set cb(X) to true (indeed, Tj has surely committed)
def rollback(transaction_ts):
    resource_to_check = None
    for index, elem in enumerate(resource_info):
        if elem.wts == transaction_ts :
            elem.wts = elem.wts_c
            elem.cb = True
            resource_to_check = elem.name
            # Now we need to check if some other transaction is in waiting , each time we put to True a new variable
            check_waiting(resource_to_check)

def apply_rules(elem):  
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
                    print("Action :",action_type," Transaction :",transaction ," over element :", resource," STATUS = OK ")
                else:
                    print("Action :",action_type," Transaction :",transaction ," over element :", resource," STATUS = WAITING ")
                    #add the action that generate waiting into deadlock_list_detector
                    deadlock_detector.append(( transaction , action_type, resource ))
                    transaction_info[transaction_index].waiting = True   
                    ignored_actions.append(( transaction , action_type, resource ))

            elif (transaction_ts >= resource_info[resource_index].rts) and (transaction_ts < resource_info[resource_index].wts):
                if resource_info[resource_index].cb == True:
                    print("Action :",action_type," Transaction :",transaction ," over element :", resource," STATUS = IGNORE ")
                else:
                    print("Action :",action_type," Transaction :",transaction ," over element :", resource," STATUS = WAITING ")
                    #add the action that generate waiting into deadlock_list_detector
                    deadlock_detector.append(( transaction , action_type, resource ))
                    transaction_info[transaction_index].waiting = True
                    ignored_actions.append(( transaction , action_type, resource ))   
            else:
                print("Action :",action_type," Transaction :",transaction ," over element :", resource," STATUS = ROLLBACK ")
                #add the transaction that need to be rollbacked
                rollback_transaction.append(transaction)
                transaction_info[transaction_index].rollback = True 
                rollback(transaction_ts)

        elif action_type == "read":
            if (transaction_ts >= resource_info[resource_index].wts):
                if (resource_info[resource_index].cb == True) or (transaction_ts  == resource_info[resource_index].wts):
                    print("Action :",action_type," Transaction :",transaction ," over element :", resource," STATUS = OK ")
                    resource_info[resource_index].rts = max(transaction_ts, resource_info[resource_index].rts)
                else:
                    print("Action :",action_type," Transaction :",transaction ," over element :", resource," STATUS = WAITING ")
                    #add the action that generate waiting into deadlock_list_detector
                    deadlock_detector.append(( transaction , action_type, resource ))
                    transaction_info[transaction_index].waiting = True
                    ignored_actions.append(elem)   
            else:
                print("Action :",action_type," Transaction :",transaction ," over element :", resource," STATUS = ROLLBACK ")
                #add the transaction that need to be rollbacked
                rollback_transaction.append(transaction)
                transaction_info[transaction_index].rollback = True 
                rollback(transaction_ts)

        elif action_type == "commit":
            print("Action :",action_type," Transaction :",transaction ," STATUS = OK ")
            # We can check in the array of the datainfo objects, if there's resources where the last write have the ts = to the ts of the commit, it means is the last transaction that wrote in this element,
            # so we need to set it to true
            resource_to_check = None
            for index, elem in enumerate(resource_info):
                if elem.wts == transaction_ts :
                    elem.cb = True
                    elem.wts_c = transaction_ts
                    resource_to_check = elem.name
            # Now we need to check if some other actions are in waiting for this commit.
            check_waiting(resource_to_check)

def apply_timestamp(scheduler):
    for elem in scheduler: 
        apply_rules(elem)

def get_scheduler():
    scheduler = []
    print("Algorithm for serializability and concurrency potrocol 2PL")

    # User interact, get the input from the user
    while True:
        user_input = input("Enter action (e.g., 'read 1 X' or 'write 2 Y', 'confirm' to generate scheduler): ").strip().lower()
        if user_input == 'confirm':
            break

        parts = user_input.split()
        if len(parts) == 1 or len(parts) == 0 or len(parts) >= 4 :
            print("Invalid input format. Please enter 'read' or 'write' followed by transaction ID and resource.")
            continue
        
        if len(parts) == 2:
            action_type, transaction_id = parts
            if action_type == "commit":
                resource = None
        else :
            action_type, transaction_id, resource = parts
            
       
        if action_type not in ['read', 'write','commit']:
            print("Invalid action type. Please enter 'read' or 'write' or 'commit' .")
            continue
        try:
            transaction_id = int(transaction_id)
        except ValueError:
            print("Invalid transaction ID. Please enter a valid integer.")
            continue
        transaction_id = "T"+str(transaction_id)
        scheduler.append((transaction_id, action_type, resource))

        print(f"Added: {user_input}")

    # Better visualization of the scheduler generated by the user
    scheduler_str = "S : {"
    for action in scheduler:
        transaction_id, action_type, resource = action
        if action_type in ["read","write"]:
            scheduler_str += f" {action_type}{transaction_id[1]}({resource}),"
        else : 
            scheduler_str += f" {action_type}{transaction_id[1]},"
    scheduler_str = scheduler_str.rstrip(',')  # Remove the trailing comma
    scheduler_str += " }"
    print(f"Scheduler Obtained : {scheduler_str}")
    return scheduler
    # Check concurrency throught timestamp

#--------- SERIALIZABILITY -----------#
def check_serializability(scheduler):
    # Init precedent graph
    precedence_graph = {}
    # Scan the scheduler in order to find the conflict pair for adding edges
    for i in range(len(scheduler)):
        action_i, transaction_i, element_i = scheduler[i]
        if transaction_i not in precedence_graph:
            precedence_graph[transaction_i] = []
        
        for j in range(i + 1, len(scheduler)):
            action_j, transaction_j, element_j = scheduler[j]
            if (
                transaction_i != transaction_j
                and element_i == element_j
                and (action_i == 'write' or action_j == 'write')
            ):
                if transaction_j not in precedence_graph:
                    precedence_graph[transaction_j] = []
                precedence_graph[transaction_i].append(transaction_j)

    # Print precedent graph
    #for transaction, adjacents in precedence_graph.items():
    #    print(f"{transaction}: {adjacents}")

    # Check if the graph is acyclic.
    if has_cycle(precedence_graph):
        return False
    else:
        return True
# Function for checking if there'are cycle (use DFS search)
def has_cycle(graph):
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
#--------- SERIALIZABILITY -----------#

def init():
    scheduler = get_scheduler()
    print(scheduler)

    # Generate a list without duplicates of the needed element
    list_resource = set([tupla[2] for tupla in scheduler if tupla[2] is not None])
    list_transaction = set([tupla[0] for tupla in scheduler])
    # print(list_resource)
    # print(list_transaction)

    # Generate array of object's class in order to keep a better track of the element

    for elem in list_resource:
        resource_info.append(ResourceInfo(elem))
    for elem in list_transaction:
        transaction_info.append(Transaction(elem))
    
    # Apply concurrency control through timestamp
    apply_timestamp(scheduler)
    # Concurrency Control results
    for elem in resource_info:
        print(vars(elem))
    for elem in transaction_info:
        print(vars(elem))

    scheduler_for_conflict = [elem for elem in scheduler if "commit" not in elem ]
    result = check_serializability(scheduler_for_conflict)
    if result:
        print("The scheduler is serializable")
    else:
        print("The scheduler is not serializable")
  
#Setup usefull List 
ignored_actions = []
rollback_transaction = []
deadlock_detector = []
resource_info = []
transaction_info = []

init()