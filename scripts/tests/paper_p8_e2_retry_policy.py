"""Shared strict validation for the opt-in official E2 retry policy."""

POLICY_KEYS = frozenset({'mode', 'initial_interval_ms', 'max_interval_ms',
                         'jitter_percent', 'rate_per_second', 'burst'})


def validate_retry_policy(value, *, pending_messages):
    if type(value) is not dict or set(value) != POLICY_KEYS:
        raise ValueError('e2_retry_policy_rejected')
    bounds = {
        'initial_interval_ms': (100, 60000), 'max_interval_ms': (100, 60000),
        'jitter_percent': (0, 100), 'rate_per_second': (1, 10000),
        'burst': (1, 100000),
    }
    if value.get('mode') != 'bounded_backoff_v1':
        raise ValueError('e2_retry_policy_rejected')
    for key, (low, high) in bounds.items():
        if type(value.get(key)) is not int or not low <= value[key] <= high:
            raise ValueError('e2_retry_policy_rejected')
    if (value['max_interval_ms'] < value['initial_interval_ms']
            or value['burst'] > pending_messages):
        raise ValueError('e2_retry_policy_rejected')
    return {key: value[key] for key in ('mode', *sorted(POLICY_KEYS - {'mode'}))}
