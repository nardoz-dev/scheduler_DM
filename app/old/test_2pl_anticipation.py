class TwoPhaseLockingVerifier:
    def __init__(self):
        self.locks = {}  # Dizionario per tenere traccia dei lock acquisiti

    def acquire_lock(self, transaction_id, data_item, lock_type):
        # Verifica se il lock può essere acquisito secondo il protocollo 2PL
        if lock_type == "exclusive":
            # Controllo se il dato è già in lock da un'altra transazione
            if data_item in self.locks:
                if self.locks[data_item][0] == transaction_id:
                    return True  # Il lock esclusivo può essere riacquisito dalla stessa transazione
                else:
                    return False  # Il lock esclusivo è già acquisito da un'altra transazione
            else:
                self.locks[data_item] = (transaction_id, lock_type)
                return True  # Il lock esclusivo è stato acquisito

        elif lock_type == "shared":
            # Controllo se il dato è in lock esclusivo da un'altra transazione
            if data_item in self.locks and self.locks[data_item][1] == "exclusive":
                return False  # Il lock condiviso non può essere acquisito se il dato è in lock esclusivo
            else:
                self.locks[data_item] = (transaction_id, lock_type)
                return True  # Il lock condiviso è stato acquisito

    def release_lock(self, transaction_id, data_item):
        # Rilascio del lock
        if data_item in self.locks and self.locks[data_item][0] == transaction_id:
            del self.locks[data_item]  # Rilascio il lock

    def is_2pl_protocol(self, schedule):
        for action in schedule:
            transaction_id, action_type, data_item = action
            if action_type == "read":
                # Per le letture, possiamo acquisire lock condivisi
                if not self.acquire_lock(transaction_id, data_item, "shared"):
                    return False  # Violazione del protocollo 2PL
                else 
                    schedule_2pl
            elif action_type == "write":
                # Per le scritture, è necessario acquisire lock esclusivi
                if not self.acquire_lock(transaction_id, data_item, "exclusive"):
                    return False  # Violazione del protocollo 2PL
        return True  # La sequenza rispetta il protocollo 2PL

# Esempio di utilizzo:
schedule = [
    ("T1", "read", "A"),
    ("T2", "write", "A"),
    ("T1", "write", "A"),
]

schedule_2pl = [

]

verifier = TwoPhaseLockingVerifier()
result = verifier.is_2pl_protocol(schedule)
print("Lo scheduler segue il protocollo 2PL:", result)

"""def can_apply_2pl_with_anticipation(schedule):
    active_transactions = set()
    locks = {}

    for action in schedule:
        action_type, transaction, data_item = action[0], action[1], action[2]

        if action_type == "read":
            if transaction in locks.get(data_item, []) and locks[data_item][transaction] == "X":
                return False
            if any(locks.get(data_item, {}).values()):
                if "X" in locks.get(data_item, {}).values():
                    return False
            locks.setdefault(data_item, {})[transaction] = "S"
        elif action_type == "write":
            if transaction in locks.get(data_item, {}) and locks[data_item][transaction] in ["S", "X"]:
                return False
            if any(locks.get(data_item, {}).values()):
                if "X" in locks.get(data_item, {}).values() or len(locks.get(data_item, {})) > 1:
                    return False
            locks.setdefault(data_item, {})[transaction] = "X"
        elif action_type == "commit":
            locks.pop(data_item, None)

    return True"""