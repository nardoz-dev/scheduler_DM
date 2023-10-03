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
    global ignored_actions
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

def apply_timestamp():
    for elem in schedule: 
        apply_rules(elem)        

# We assume that the physical time is always correct and in order
# We assume that the timestamp of each transaction Ti coincide with the subscript i, i.e. w2(x) -> ts = 2


# 1 SCHEDULE : OK 
"""schedule = [
    ("T1","write","u"),
    ("T1","read","x"),
    ("T3","write","x"),
    ("T2","read","y"),
    ("T1","read","y"),
    ("T1","commit",None),
    ("T4","read","u"),
    ("T2","write","y"),
    ("T3","write","y"),
    ("T3","commit",None),
    ("T4","write","z"),
    ("T4","commit",None),
    ("T2","read","z"),
    ("T2","commit",None)
    ]
"""
# 2 SCHEDULE = OK 
schedule = [
    ("T1","read","z"),
    ("T1","read","y"),
    ("T3","write","y"),
    ("T1","read","x"),
    ("T2","read","x"),
    ("T1","commit",None),
    ("T4","write","z"),
    ("T2","write","x"),
    ("T3","write","x"),
    ("T3","commit",None),
    ("T4","read","u"),
    ("T4","commit",None),
    ("T2","write","u"),
    ("T2","commit",None)
    ]

ignored_actions = []
rollback_transaction = []
deadlock_detector = []

# Generate a list without duplicates of the needed element
list_resource = set([tupla[2] for tupla in schedule if tupla[2] is not None])
list_transaction = set([tupla[0] for tupla in schedule])
print(list_resource)
print(list_transaction)

# Generate array of object's class in order to keep a better track of the element
resource_info = []
transaction_info = []
for elem in list_resource:
    resource_info.append(ResourceInfo(elem))
for elem in list_transaction:
    transaction_info.append(Transaction(elem))

# Apply concurrency control through timestamp
apply_timestamp()


# Debug print for checking results:
for elem in resource_info:
    print(vars(elem))
for elem in transaction_info:
    print(vars(elem))
print("Action that generate waiting and are in deadlock_list :" ,deadlock_detector)
print("Action that are ignored because transactions in waiting :" ,ignored_actions)
print("Transazione dal quale bisogna ignorare le azioni perchè in rollback : ", rollback_transaction)
