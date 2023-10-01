def can_apply_2pl_with_anticipation(schedule):
    active_transactions = set()
    locks = {}

    for action in schedule:
        transaction, action_type, data_item = action[0], action[1], action[2]

        # Se ho una read, controllo se un'altra transazione non abbia già "L'ESCLUSIVE LOCK su questa risorsa altrimenti devo provare a fare una lock anticipation o addirittura dichiarare NO2PL"
        # Ovviamente se la read appartiene agli shared locks, nessuno problema possiamo assegnarli un nuovo shared lock. 

        # Per quanto riguarda le write, se ne abbiamo una, dobbiamo controllare : 
        # - Se la risorsa ha una shared lock e la write è della stessa transazione allora trasformiamo la shared in una exclusive
        # - Se la risorsa ha una exclusive lock, dobbiamo fare l'unlock (Se possibile)
        # - Se la risorsa non ha nulla, dobbiamo procedere per una exclusive lock. 

        # Lock anticipation 
        # Per poter effettuare il lock anticipation dobbiamo : data una write, 

        if action_type == "read":
            if transaction in locks.get(data_item, []) and locks[data_item][transaction] == "X":
                return "2PL cannot be applied due to a conflict."
            if any(locks.get(data_item, {}).values()):
                if "X" in locks.get(data_item, {}).values():
                    return "2PL cannot be applied due to a conflict."
            locks.setdefault(data_item, {})[transaction] = "S"
        elif action_type == "write":
            if transaction in locks.get(data_item, {}) and locks[data_item][transaction] in ["S", "X"]:
                return "2PL cannot be applied due to a conflict."
            if any(locks.get(data_item, {}).values()):
                if "X" in locks.get(data_item, {}).values() or len(locks.get(data_item, {})) > 1:
                    return "2PL cannot be applied due to a conflict."
            locks.setdefault(data_item, {})[transaction] = "X"
        elif action_type == "commit":
            locks.pop(data_item, None)

    return "2PL can be applied."

# Example usage with a schedule represented as a list of tuples (transaction, action_type, data_item):
schedule = [("T1", "read", "a"), ("T2", "read", "a"), ("T3", "read", "b"), ("T1", "write", "a"), ("T2", "read", "c"),("T2","read","b"),("T2", "write", "b"),("T1","write","c")]
#w1(y)r2(y)r3(x)w1(x) not 2pl
#read 2 a, read 3b,write 1 a , read 2 c, read 2 d , write 1 d    2PL
#lock anticipation needed : r1(X) w2(X) w1(Y)
#deadlock so no 2pl : r1a,r2a,r3b,w1a,r2c,r2b,w2b,w1c
result = can_apply_2pl_with_anticipation(schedule)
print(result)



