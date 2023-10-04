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
    def has_cycle(node, visited, rec_stack):
        visited[node] = True
        rec_stack[node] = True

        for neighbor in precedence_graph.get(node, []):
            if not visited[neighbor]:
                if has_cycle(neighbor, visited, rec_stack):
                    return True
            elif rec_stack[neighbor]:
                return True

        rec_stack[node] = False
        return False

    # Verifica se il grafo di precedenza ha cicli
    visited = {transaction: False for transaction in precedence_graph}
    rec_stack = {transaction: False for transaction in precedence_graph}
    for transaction in precedence_graph:
        if not visited[transaction]:
            if has_cycle(transaction, visited, rec_stack):
                return False  # Lo schedule non è conflict-serializable

    return True  # Lo schedule è conflict-serializable

# Esempio di utilizzo
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

"""

#CONFLICT SERIALIZABLE
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

if is_conflict_serializable(sequence):
    print("Lo schedule è conflict-serializable")
else:
    print("Lo schedule non è conflict-serializable")
