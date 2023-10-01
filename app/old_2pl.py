def can_apply_2pl_with_anticipation(schedule):
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

    return True