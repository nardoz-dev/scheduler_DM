# CONFLICT SERIALAIZABLE SENZA DFS

def is_conflict_serializable(schedule):
    # Inizializza il grafo di precedenza
    precedence_graph = {}
    
    # Scansione dello schedule per costruire il grafo di precedenza
    for i in range(len(schedule)):
        transaction_i, element_i ,action_i = schedule[i]
        if transaction_i not in precedence_graph:
            precedence_graph[transaction_i] = set()

        # Scansione delle operazioni successive nello schedule
        for j in range(i + 1, len(schedule)):
            transaction_j, element_j ,action_j= schedule[j]
            # Verifica se ci sono conflitti tra le transazioni
            if (transaction_i != transaction_j and element_i == element_j and (action_i == "write" or action_j =="write")):
                # Aggiunge un arco nel grafo di precedenza
                precedence_graph[transaction_i].add(transaction_j)
                print("aggiungo adiacenza")

    for transaction, adjacents in precedence_graph.items():
        print(f"{transaction}: {adjacents}")
    
    # Funzione per verificare se il grafo ha cicli utilizzando DFS
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
    for transaction in precedence_graph:
        if not visited[transaction]:
            if has_cycle(transaction, visited, rec_stack,topological_order):
                return False, [] # Lo schedule non è conflict-serializable

    return True,topological_order  # Lo schedule è conflict-serializable
"""
# Esempio di utilizzo
# Non è conflict
sequence = [
    ('T1', 'a', 'read'),
    ('T3', 'c', 'read'),
    ('T3', 'b', 'write'),
    ('T2', 'a', 'read'),
    ('T1', 'b', 'write'),
    ('T1', 'a', 'write'),
    ('T2', 'a', 'write'),
    ('T1', 'c', 'read'),

    ('T3', 'c', 'write'),
    ('T3', 'a', 'read'),
    ('T2', 'd', 'read'),
]

#CONFLICT SERIALIZABLE e un probabile ordine topologico è t2,t1,t4,t3
sequence = [
    ('T1', 'a', 'read'),
    ('T2', 'a', 'read'),
    ('T2', 'b', 'read'),
    ('T1', 'a', 'write'),
    ('T2', 'd', 'write'),
    ('T3', 'c', 'read'),
    ('T1', 'c', 'read'),
    ('T3', 'b', 'write'),

    ('T4', 'a', 'read'),
    ('T3', 'c', 'write'),
]
"""
# Conflict con un ordine topologico : t2,t3,t1
sequence = [
    ('T1', 'a', 'read'),
    ('T3', 'c', 'read'),
    ('T3', 'b', 'write'),
    ('T2', 'a', 'read'),
    ('T1', 'b', 'write'),
    ('T2', 'c', 'read'),
    ('T3', 'c', 'write'),
    ('T3', 'a', 'read'),
    ('T2', 'd', 'write'),
]

is_conflict, serial_schedule = is_conflict_serializable(sequence)
if is_conflict:
    print("Lo schedule è conflict-serializable")
else:
    print("Lo schedule non è conflict-serializable")

inverted_list = serial_schedule[::-1]
print("Scheduler seriale : ",inverted_list)
