#FIRST FILE REFERENCES OF CONFLICT SERIALIZABILITY

# Esempio di sequenza di azioni (ciascuna azione è una tupla: (transazione, elemento, tipo))
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
    #('T2', 'v', 'read'),
    #('T1', 'x', 'read'),
    #('T4', 'v', 'write'),
    #('T3', 'y', 'write'),
    #('T2', 'x', 'write'),
    #('T1', 'y', 'read'),
    #('T4', 'y', 'write'),
    #('T3', 'v', 'write'),
    # Altre azioni...  w1(x)r2(x)w2(y)r1(y) 
]

# Inizializza il grafo di precedenza
precedence_graph = {}

# Scansiona la sequenza di azioni per identificare le conflict pair e costruire il grafo
for i in range(len(sequence)):
    transaction_i, element_i, action_i = sequence[i]
    if transaction_i not in precedence_graph:
        precedence_graph[transaction_i] = []
    
    for j in range(i + 1, len(sequence)):
        transaction_j, element_j, action_j = sequence[j]
        if (
            transaction_i != transaction_j
            and element_i == element_j
            and (action_i == 'write' or action_j == 'write')
        ):
            if transaction_j not in precedence_graph:
                precedence_graph[transaction_j] = []
            precedence_graph[transaction_i].append(transaction_j)

# Stampare il grafo di precedenza
for transaction, adjacents in precedence_graph.items():
    print(f"{transaction}: {adjacents}")

    
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

# Esempio di utilizzo con il grafo di precedenza precedentemente creato
if has_cycle(precedence_graph):
    print("Il grafo ha un ciclo.")
else:
    print("Il grafo non ha cicli.")