
def check_2pl(schedule,locks):
    locks = []
    index = 0
    index_schedule = 0
    for elem in schedule:
        index_schedule += 1 
        transaction, action_type, resource = elem[0],elem[1],elem[2]

        #if we encounter a read in the schedule
        if action_type == "read":
            flag = True
            flag_unlock = False
            for element in locks :
                owner_lock, resource_locked, lock_type = element[0],element[1],element[2]
                if resource_locked == resource and owner_lock == transaction and lock_type == "SL":
                    print("SharedLock already granted, do nothing")
                if resource_locked == resource and owner_lock == transaction and lock_type == "XL":
                    print("Don't leave exclusive lock, do read in exclusive lock")
                if resource_locked == resource and owner_lock != transaction and lock_type == "XL":
                    print("We need to perform an unlock")
                    flag_unlock = True
            if flag :
                #print("Add shared lock")
                if not flag_unlock : 
                    scheduler_2pl.insert(index,(transaction, "sharedLock", resource))
                    locks.append((transaction,resource,"SL"))
                    index += 2
                else : 
                    print("Lock anticipation")

        if action_type == "write":
            flag_upgrade = False
            flag_unlock = False
            transaction_to_upgrade = None
            transaction_to_check = None
            # quando scrivo ? quando : 
            #   - se nessuna transazione ha la mia risorsa allora direttamente exclusive
            #   - se la transazione che ha la risorsa che mi serve è la mia ed è una exclusive
            # quando devo fare l'unlock ? 
            #   - quando c'è un'altra transazione con un lock qualsiasi sulla stessa risorsa
            #print(locks)
            for i in range(len(locks)):
                owner_lock, resource_locked, lock_type = locks[i]
                # lock upgrade : quindi stessa transazione con un sl su quella risorsa
                if resource_locked == resource and transaction == owner_lock and lock_type == "SL":
                    flag_upgrade = True
                    transaction_to_upgrade = locks[i]
                # different transaction operates the same resources handle situation.
                if resource_locked == resource and transaction != owner_lock and (lock_type == "SL" or lock_type == "XL"):
                    flag_unlock = True
                    transaction_to_check = locks[i]

            # So it means we don't handle different transaction over the same element
            if not flag_unlock: 
                #add the lock operation into the schedule
                scheduler_2pl.insert(index,(transaction, "exclusiveLock", resource))
                if flag_upgrade:
                    #update list of locks for algorithm purpose
                    #print("Transazione da upgradare" ,transaction_to_upgrade)
                    index_to_remove = locks.index(transaction_to_upgrade)
                    new_transaction, new_resource = transaction_to_upgrade[0],transaction_to_upgrade[1]
                    locks.append((transaction,resource,"XL"))
                    index += 2
                else:
                    locks.append((transaction,resource,"XL"))
                    index += 2
            # So it means we handle different transaction over the same element 
            else: 
                # Try to apply an unlock         
                # Set the id of the transaction that we are try to unlock
                print("Stiamo nel caso in cui abbiamo transazioni diverse sullo stesso elemento")
                print("Transazione che possiede l'acquire sulla stessa risorsa", transaction_to_check)

                transaction_to_verify = transaction_to_check[0]
                #print(transaction_to_verify)
                # we use the actual index of the schedule in order to start the check of anticipation by looking only after the write 
                for i in range(index_schedule-1,len(schedule)):
                    # check if we have other action of the transaction that we will try to unlock
                    if transaction_to_verify in schedule[i]:
                        print("Devi verificare se puoi fare la lock anticipation")
                        # Teoricamente dovresti aggiungere eventuale sl, o xl in base al tipo di operazione che è
                    else: 
                        #unlock over the transaction that has the same resources so [transaction_to_check]
                        scheduler_2pl.insert(index,(transaction_to_check[0],"unLock",transaction_to_check[1]))
                        index += 1
                        #update the Locks list
                        locks.remove(transaction_to_check)
                # add exclusive lock
                scheduler_2pl.insert(index,(transaction,"exclusiveLock",transaction_to_check[1]))
                index += 1
                locks.append((transaction,resource,"XL"))
                print(locks)
                            


# Test
# r1x. r2x. w1x. r2z.
# sl1x. r1x. sl2x. r2x. sl2z. ul2x. xl1x. w1x. ul1x. r2z. ul2z.

# OSS per il momento non tiene traccia nel caso in cui abbiamo due unlock di fila quindi ad esempio : 
# r1x, r2x, w3x .     deve fare due unlock e poi una exclusive lock.
# OSS non tiene nemmeno traccia nel caso in cui ho ad esempio 
# r1x, r2x, w1x .    non aggiorna bene il lock perchè lo dovrebbe fare anche in questo caso. le unlock sono messe bene.

# SECONDO ME IN QUESTO MODO L'ALGORITMO è STRUTTURATO MEGLIO
# TOGLIERE LA DIFFERENZAZIONE TRA WRITE E READ E PARTIRE DIRETTAMENTE AD ANALIZZARE SE LA RISORSA UTILIZZATA NELL'AZIONE è GIà USATA DA UNA TRANSAZIONE O MENO
# CASO IN CUI è LA STESSA TRANSAZIONE : 
# SE HO UNA READ NON FARE NULLA
# SE HO UNA WRITE TRASFORMA SL CON XL 
# CASO IN CUI LA TRANSAZIONE è DIVERSA :
# SE HO UNA READ APPLICO EVENTUALI UNLOCK E ANTICIPATION
# SE HO UNA WRITE APPLICO EVENTUALI UNLOCK E ANTICIPATION
def check_anticipation(elem,locks):
    print("Check anticipation over the action", elem)
    global index_schedule
    print(index_schedule)

    for i in range(index_schedule,len(schedule)):
        print(i)
        if elem[0] == schedule[i][0]:
            print("ho un'altra azione")
    # unlock the resource
    # scheduler_2pl.insert(index,(locks[0][0], "unlock", elem[2]))
    # remove it from lock list
    # locks.remove((locks[0][0],resource,"SL"))

def apply_unlock(elem,locks):
    global index
    # ricorda che se sono qui dentro la risorsa dell'azione che sto analizzando è già lockata da qualcuno 
    # vediamo quante transaction trattengono quella risorsa : 
     
    resource = elem[2]
    if len(locks) == 1:  # prima esamino il caso in cui ho solo un elemento dentro la lista locks
        # print(elem[1]) # write 
        # print(locks[0][1]) # a 
        

        # se la transazione è uguale, e action_type == SL, ed ho una read , non fare nulla
        # se la transazione è uguale e action_type == XL, ed ho una read , non fare nulla ? = caso che non avverrà mai secondo me write2(x) read2(x)
        # se la transazione è uguale e action_type == XL, ed ho una write, non fare nulla ? = caso che hai tipo write2(x) write2(x)
        # if transaction is the same , and action type == SL, and we have a write , lock upgrade
        if locks[0][0] == elem[0] and locks[0][2] == "SL" and elem[1] == "write":
            print("locks Upgrade")
            # apply exclusive lock
            scheduler_2pl.insert(index,(elem[0], "exclusiveLock", resource))
            locks.remove((elem[0],resource,"SL"))
            locks.append((elem[0],resource,"XL"))
            # index += 2
        
        # se la transazione è diversa, e action_type == SL, ed ho una read, applica shared lock
        if locks[0][0] != elem[0] and locks[0][2] == "SL" and elem[1] == "read":
            print("Shared Lock")
            scheduler_2pl.insert(index,(elem[0], "sharedLock", resource))
            locks.append((elem[0],resource,"SL"))
            index += 2
        # se la transazione è diversa, e action_type == SL, ed ho una write, unlock + lock anticipation
        if (locks[0][0] != elem[0] and locks[0][2] == "SL" and elem[1] == "write"):
            # apply exclusive lock
            scheduler_2pl.insert(index,(elem[0], "exclusiveLock", resource))
            locks.append((elem[0],resource,"XL"))
            # index += 2
            # In order to apply unlock i need to check anticipation:
            
            # creation of the action that generated the lock 
            transaction_to_check = locks[0][0], "read", resource
            check_anticipation(transaction_to_check,locks)
            # locks.append((elem[0],elem[2],"SL"))

        # se la transazione è diversa, e action_type == XL, ed ho una read, unlock + lock anticipation
        if (locks[0][0] != elem[0] and locks[0][2] == "XL" and elem[1] == "read"):
            # apply shared lock
            scheduler_2pl.insert(index,(elem[0], "sharedLock", resource))
            locks.append((elem[0],resource,"SL"))
            # index += 2
            
            # creation of the action that generated the lock 
            transaction_to_check = locks[0][0], "write", resource
            check_anticipation(transaction_to_check,locks)

        # se la transazione è diversa, e action_type == XL, ed ho una write, unlock + lock anticipation
        if (locks[0][0] != elem[0] and locks[0][2] == "XL" and elem[1] == "write"):
            # apply exclusive lock
            scheduler_2pl.insert(index,(elem[0], "exclusiveLock", resource))
            locks.append((elem[0],resource,"XL"))
            # index += 2
            # creation of the action that generated the lock 
            transaction_to_check = locks[0][0], "write", resource
            check_anticipation(transaction_to_check,locks)
    else: 
        print("Ho più di un elemento dentro la lista lock, devi controllarli tutti")
        # controllo se invece la transazione che trattiene quella risorsa è la stessa 

        # (quindi creo una lista di tutte le transazioni diverse che trattengono quella risorsa)
        # in quelle diverse devo applicare la lock anticipation 


    #print("Elemento che stiamo analizzando " , elem)
    print("Locks", locks)
    # Controlliamo se è un SL o XL?
    #if elem[1] == "read" and 

    #if ("read","SL") in locks: 
        #se entriamo qui :
    #    print("hello") 
    #for i in range(len(locks)):
    #    if resource in locks[i]:
    #        print("porcodio")

index = 0
index_schedule = 0
def check_2pl_new(schedule):
    locks = []
    global index 
    global index_schedule
    #start to scan the schedule
    for elem in schedule: 
        index_schedule += 1
        # split the action 
        transaction, action_type, resource = elem[0],elem[1],elem[2]
        #print("Elemento trattato : ", elem)

        # If the list is empty, then execute the action directly
        if not locks:
            if action_type == "read":
                scheduler_2pl.insert(index,(transaction, "sharedLock", resource))
                locks.append((transaction,resource,"SL"))
                index += 2
            else:
                scheduler_2pl.insert(index,(transaction, "exclusiveLock", resource))
                locks.append((transaction,resource,"XL"))
                index += 2
        else :
            flag = False
            # check if the action have the resourse  already in the locks list.
            for i in range(len(locks)):
                if resource in locks[i]: # It means the resources that you want to use is already used either by the same or other transaction
                    flag = True 
            if not flag: #significa che non ho nessuna transazione in lock che ha una lock sull'elemento che sta usando l'azione che sto considerando
                #read
                if action_type == "read":
                    scheduler_2pl.insert(index,(transaction, "sharedLock", resource))
                    locks.append((transaction,resource,"SL"))
                    index += 2
                else:
                #write
                    scheduler_2pl.insert(index,(transaction, "exclusiveLock", resource))
                    locks.append((transaction,resource,"XL"))
                    index += 2
            else:
                apply_unlock(elem,locks)


schedule = [("T1", "read", "a"),("T2", "write", "a"),("T1", "write", "b")]#,("T2", "read", "z")]
scheduler_2pl = [("T1", "read", "a"),("T2", "write", "a"),("T1", "write", "b")]#,("T2", "read", "z")]

#schedule = [("T1", "read", "a"),("T2", "read", "b"),("T1", "write", "a"),("T5", "read", "z")]
#scheduler_2pl = [("T1", "read", "a"),("T2", "read", "b"),("T1", "write", "a"),("T5", "read", "z")]
check_2pl_new(schedule)
print(scheduler_2pl)