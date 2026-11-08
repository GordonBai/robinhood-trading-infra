"""Synthetic exit rule that can be made slow for bounded-probe tests."""
import os
import time


def slow_exit(view):
    path = view.get('pid_file')
    if path:
        with open(path, 'w', encoding='ascii') as handle:
            handle.write('%d %d' % (os.getpid(), os.getpgrp()))
    time.sleep(int(view.get('sleep_seconds', 0)))
    return {'action': 'HOLD', 'reason': 'PROBE_HOLD'}


def always_exit(view):
    return {'action': 'EXIT', 'reason': 'ALWAYS_EXIT'}


def bad_action(view):
    return {'action': 'EXIT', 'reason': 'BAD', 'unexpected': 1}
