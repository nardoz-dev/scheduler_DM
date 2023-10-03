def can_apply_2pl_with_anticipation(schedule):
    locks = []
    index = 0 
    for elem in schedule: 
        transaction, action_type, data_item = elem[0], elem[1], elem[2]
        print(elem)
        # Se ho una read, controllo se un'altra transazione non abbia già "L'ESCLUSIVE LOCK su questa risorsa altrimenti devo provare a fare una lock anticipation o addirittura dichiarare NO2PL"
        # Ovviamente se la read appartiene agli shared locks, nessuno problema possiamo assegnarli un nuovo shared lock. 

        # Per quanto riguarda le write, se ne abbiamo una, dobbiamo controllare : 
        # - Se la risorsa ha una shared lock e la write è della stessa transazione allora trasformiamo la shared in una exclusive
        # - Se la risorsa ha una exclusive lock, dobbiamo fare l'unlock (Se possibile)
        # - Se la risorsa non ha nulla, dobbiamo procedere per una exclusive lock. 

        # Lock anticipation 
        # Per poter effettuare il lock anticipation dobbiamo : data una write, 
        # -r1(x) w2(x) w1(y)
        #  sl1(x) r1(X) xl1(y) ul1(x) xl2(x) w2(X) w1(Y) ul2(x) ul1(y)
        # -r1(x) w2(x) w1(x)
        #  sl1(x) r1(x) ul1(x) w2(x) 
        # -r1x, r3z,w1y,w2x,w1z,w3u,w1v,w2u,w2v    si 2pl ?
        # -r1(z), w3(y), w3(v), r1(y), r2(v), w2(y) ( lock anticipation + no 2pl)
        if action_type == "read": 
            if not locks:
                #print("Apply shared lock")
                #add it to the schedule
                schedule.insert(index,(transaction, "sharedLock", data_item))
                locks.append((transaction,data_item,"SL"))
                index += 2
            else: 
                for i in range(len(locks)):
                    owner_lock, data, lock_type = locks[i]
                    #print(locks)
                    if data == data_item and lock_type == "SL":
                        #print("Apply shared lock")
                        schedule.insert(index,(transaction, "sharedLock", data_item))
                        locks.append((transaction,data_item,"SL"))
                        index += 2
                    if data == data_item and lock_type == "EL":
                        print("Try lock anticipation over the transaction that lock X the element in order to unlock it")
    
    return "2PL can be applied."          
    #print(locks)
    #print(schedule)

        #if action_type == "read":
        #    if transaction in locks.get(data_item, []) and locks[data_item][transaction] == "X":
        #        return "2PL cannot be applied due to a conflict."
        #    if any(locks.get(data_item, {}).values()):
        #        if "X" in locks.get(data_item, {}).values():
        #            return "2PL cannot be applied due to a conflict."
        #    locks.setdefault(data_item, {})[transaction] = "S"
        #elif action_type == "write":
        #    if transaction in locks.get(data_item, {}) and locks[data_item][transaction] in ["S", "X"]:
        #        return "2PL cannot be applied due to a conflict."
        #    if any(locks.get(data_item, {}).values()):
        #        if "X" in locks.get(data_item, {}).values() or len(locks.get(data_item, {})) > 1:
        #            return "2PL cannot be applied due to a conflict."
        #    locks.setdefault(data_item, {})[transaction] = "X"
        #elif action_type == "commit":
        #    locks.pop(data_item, None)
def check_anticipation(lock,schedule):
    #print(scheduler_2pl[index])
    #devo controllare se a partire dall'indice in cui mi trovo adesso se ci sono delle azioni di T2 con una risorsa 
    #diversa da quella richiesta dall'azione write.
    #for i in range(schedule.index(elem)+1,len(schedule)):
    print(lock)


def test_function(schedule): 
    locks = []
    index = 0 
    for elem in schedule:
        transaction, action_type, data_item = elem[0], elem[1], elem[2]

        # READ OPERATION
        if action_type == "read": 
            if len(locks) == 0 :
                scheduler_2pl.insert(index,(transaction, "sharedLock", data_item))
                locks.append((transaction,data_item,"SL"))
                index += 2
            else: 
                flag_to_do = "add"
                for elem in locks:
                    owner_lock, data, lock_type = elem[0], elem[1], elem[2]
                    if data == data_item and lock_type == "SL" and transaction == owner_lock :
                        flag_to_do = "notadd"
                    if data == data_item and lock_type == "XL":
                        flag_to_do = "notadd" 

                if flag_to_do == "add":
                    scheduler_2pl.insert(index,(transaction, "sharedLock", data_item))
                    locks.append((transaction,data_item,"SL"))
                    index += 2
                elif flag_to_do == "anticipation":
                    print("C'è qualche errore")

                print("Lista lock alla fine di ogni read :" ,locks)

        # WRITE OPERATION
        if action_type == "write":
            if len(locks) == 0 :
                scheduler_2pl.insert(index,(transaction, "exclusiveLock", data_item))
                locks.append((transaction,data_item,"EL"))
                index += 2
            else: 
                #create a dictionary in order to decide different operation
                flag_to_do = "add"
                transaction_to_check = ""

                for element in locks:
                    owner_lock, data, lock_type = element[0], element[1], element[2]
                    if data == data_item and lock_type == "SL" and transaction == owner_lock :
                        flag_to_do = "add"
                    elif data == data_item and lock_type == "SL" and transaction != owner_lock :
                        flag_to_do = "anticipation"
                    elif data == data_item and lock_type == "EL":
                        flag_to_do = "error"
                
                print(flag_to_do)
                if flag_to_do == "add":
                    scheduler_2pl.insert(index,(transaction, "exclusiveLock", data_item))
                    locks.append((transaction,data_item,"EL"))
                    index += 2
                elif flag_to_do == "anticipation":
                    result = check_anticipation(locks,schedule)
                elif flag_to_do == "error":
                    print("C'è qualche errore")

                


    
# Example usage with a schedule represented as a list of tuples (transaction, action_type, data_item):
schedule = [("T2", "read", "a"),("T3", "read", "b"),("T1", "read", "a")]#,("T2", "read", "c")]#,("T2", "read", "d"),("T1", "write", "d")]
#[("T1", "read", "a"),("T2", "read", "a"),("T3", "read", "b"),("T1", "write", "a")]
#w1(y)r2(y)r3(x)w1(x) not 2pl
#read 2 a, read 3b,write 1 a , read 2 c, read 2 d , write 1 d    2PL
#lock anticipation needed : r1(X) w2(X) w1(Y)
#deadlock so no 2pl : r1a,r2a,r3b,w1a,r2c,r2b,w2b,w1c
#result = can_apply_2pl_with_anticipation(schedule)
#print(result)
scheduler_2pl = [("T2", "read", "a"),("T3", "read", "b"),("T1", "read", "a")]#,("T2", "read", "c")]#,("T2", "read", "d"),("T1", "write", "d")]
test_function(schedule)
print(scheduler_2pl)




