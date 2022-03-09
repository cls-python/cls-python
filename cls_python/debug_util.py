from typing import Iterable


def deep_str(obj) -> str:
    if isinstance(obj, list):
        return f"[{','.join(map(deep_str, obj))}]"
    elif isinstance(obj, dict):
        sep = ",\n"
        return f"map([{sep.join(map(lambda kv: ':'.join([deep_str(kv[0]), deep_str(kv[1])]) , obj.items()))}])"
    elif isinstance(obj, set):
        return f"set([{','.join(map(deep_str, obj))}])"
    elif isinstance(obj, tuple):
        return f"({','.join(map(deep_str, obj))})"
    elif isinstance(obj, str):
        return obj
    elif isinstance(obj, Iterable):
        return f"iter([{','.join(map(deep_str, obj))}])"
    else:
        return str(obj)