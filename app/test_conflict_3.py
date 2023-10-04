# CONFLICT SERIALAIZABLE CON DFS

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

    for transaction, adjacents in precedence_graph.items():
        print(f"{transaction}: {adjacents}")

        # Check if the graph is acyclic.
    if has_cycle(precedence_graph):
        return False
    else:
        return True
    
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
