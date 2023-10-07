import itertools

def extract_read_from_final_write(schedule):
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
"""
def is_serial(schedule):
    for i in range(len(schedule)):
        for j in range(i + 1, len(schedule)):
            if schedule[i][0] != schedule[j][0] and (
                schedule[i][1] == "write" or
                (schedule[i][1] == "read" and schedule[j][1] == "read" and schedule[i][2] == schedule[j][2])
            ):
                return False
    return True
"""

def is_view_serializable(schedule):
    read_from, final_write = extract_read_from_final_write(schedule)
    
    list_transaction = set([tupla[0] for tupla in schedule])
    all_permutations = itertools.permutations(list_transaction)

    for permuted_transactions in all_permutations:
        permuted_schedule = []
        
        for t_id in permuted_transactions:
            t_actions = [action for action in schedule if action[0] == t_id]
            permuted_schedule.extend(t_actions)
        
        print("Permuted scheduler : ",permuted_schedule)
        permuted_read_from, permuted_final_write = extract_read_from_final_write(permuted_schedule)

        read_from_to_check = set(read_from)
        final_write_to_check = set(final_write)
        #print("my_final_write",final_write_to_check)
        read_from_permuted = set(permuted_read_from)
        final_Write_permuted = set(final_write)
        #print("permuted_final_write",final_Write_permuted)
        if read_from_to_check == read_from_permuted and final_write_to_check == final_Write_permuted:
            print(permuted_transactions)
            print("FOUNDIT")
            return True

    return False

    

# Esempio di uno scheduler
#r1x,w2x,r3x,r1y,r4z,w2y,r1v,w3v,r4v,w4y,w5y,w5z
"""
scheduler = [
    ("T1", "read", "x"),
    ("T2", "write", "x"),
    ("T3", "read", "x"),
    ("T1","read","y"),
    ("T4", "read", "z"),
    ("T2", "write", "y"),
    ("T1", "read", "v"),
    ("T3","write","v"),
    ("T4", "read", "v"),
    ("T4", "write", "y"),
    ("T5", "write", "y"),
    ("T5","write","z")
]

#w1(y) r2(x) w2(x) r1(x) w2(z) - No view
scheduler = [
    ("T1", "write", "y"),
    ("T2", "read", "x"),
    ("T2", "write", "x"),
    ("T1","read","x"),
    ("T2","write","z"),
]
# w1(x) r2(x) w2(y) r1(y) - No view 
scheduler = [
    ("T1", "write", "x"),
    ("T2", "read", "x"),
    ("T2", "write", "y"),
    ("T1","read","y"),
]

#w0(x) r1(x) w1(x) w2(z) w1(z) - View and T2,T0,T1 is the serial schedule
"""
scheduler = [
    ("T0","write", "x"),
    ("T1","read", "x"),
    ("T1","write", "x"),
    ("T2","write","z"),
    ("T1","write","z"),
]

if is_view_serializable(scheduler):
    print("Lo scheduler è view-serializable.")
else:
    print("Lo scheduler non è view-serializable.")
