import threading

# Set when an AI inference request is in flight.
# The NLP batch job checks this before and between chunks — stops immediately if set.
_ai_active = threading.Event()


def ai_start():
    _ai_active.set()


def ai_done():
    _ai_active.clear()


def is_ai_active():
    return _ai_active.is_set()
