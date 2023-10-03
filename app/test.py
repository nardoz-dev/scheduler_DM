
deadlock_detector = [('T3', 'write', 'y')]
transaction = "T4"
print(any(transaction in element for element in deadlock_detector))