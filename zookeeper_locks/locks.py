"""Tools for creating distributed locks."""
import os
import threading
from contextlib import contextmanager
from functools import wraps
from logging import getLogger
from typing import (
    Any,
    Dict,
    Optional,
)

from kazoo.exceptions import LockTimeout as ZookeeperLockTimeout

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from .connection import zookeeper_connection_manager


logger = getLogger(__name__)


class LockTimeout(Exception):

    """Indicates that lock has failed to acquire during the timeout period."""


class Locked(Exception):

    """Indicates that the lock couldn't be acquired immediately."""


class Lock:

    """Exclusive distributed lock backed with Zookeeper.

    Example:

    >>> lock = Lock('my-lock-{object_id}')
    >>>
    >>> with lock(object_id=123):
    >>>     print('so alone')
    >>>
    >>> try:
    >>>     with lock(object_id=123, timeout=9):
    >>>         print('so alone')
    >>> except LockTimeout:
    >>>     print('unable to lock after waiting for 9s')
    >>>
    >>> try:
    >>>     with lock(object_id=123, blocking=False):
    >>>         print('so alone')
    >>> except Locked:
    >>>     print('unable to lock immediately')
    """

    # map key to lock object id
    keys_registry: Dict[str, int] = dict()

    def __init__(self, key: str):
        """Register the given key and store it in the instance."""
        self.key = key
        self._register()
        self._locked_store = threading.local()

    def __del__(self):
        """Unregister the key when destructing a lock."""
        for key, value in self.keys_registry.items():
            if value == id(self):
                del self.keys_registry[key]
                break

    def _register(self):
        """Make sure the key has not been already used and add the key to the registry."""
        if self.key in self.keys_registry:
            raise ImproperlyConfigured('Attempt to register the same key twice: {}'.format(self.key))
        self.keys_registry[self.key] = id(self)

    @contextmanager
    def __call__(self, blocking: bool = True, timeout: Optional[float] = None, **key_params: Any):
        """Connect to Zookeeper and acquire the lock."""
        self._setup_locked_store()
        key_with_params = self._get_key_with_params(key_params)
        if key_with_params in self._get_locked_keys():
            yield
        else:
            with zookeeper_connection_manager:
                zk = zookeeper_connection_manager.get_client()
                lock = zk.Lock('/locks/{namespace}/{key}'.format(namespace=settings.ZOOKEEPER_APP_NAMESPACE, key=key_with_params))
                logger.info('Acquiring lock', extra={"namespace": settings.ZOOKEEPER_APP_NAMESPACE, "key": key_with_params})
                try:
                    acquired = lock.acquire(blocking=blocking, timeout=timeout)
                except ZookeeperLockTimeout as e:
                    raise LockTimeout('Timeout occurred while trying to acquire a blocking lock on {}'.format(key_with_params)) from e
                if acquired:
                    try:
                        self._add_locked_key(key_with_params)
                        yield
                    finally:
                        lock.release()
                        self._remove_locked_key(key_with_params)
                if not acquired:
                    raise Locked('Failed to acquire a non-blocking lock on {}'.format(key_with_params))

    def _get_key_with_params(self, key_params: Dict[str, Any]) -> str:
        """Format key with the given parameters."""
        return self.key.format(**key_params)

    def _get_locked_keys(self):
        """Returns Process Specific set of locked keys"""
        pid = os.getpid()
        return self._locked_store.locked_keys.get(pid) or set()

    def _add_locked_key(self, key):
        """Adds to Process Specific set of locked keys a given key"""
        pid = os.getpid()
        if pid not in self._locked_store.locked_keys:
            self._locked_store.locked_keys[pid] = set()
        self._locked_store.locked_keys[pid].add(key)

    def _remove_locked_key(self, key):
        """Removes a given key from Process Specific set of locked keys"""
        pid = os.getpid()
        if pid not in self._locked_store.locked_keys:
            self._locked_store.locked_keys[pid] = set()
        self._locked_store.locked_keys[pid].remove(key)

    def _setup_locked_store(self):
        try:
            self._locked_store.locked_keys
        except AttributeError:
            # Dict of PID(int): Locked Keys(set) to avoid issues with forking memory
            self._locked_store.locked_keys = dict()


def return_when_locked(return_value_when_locked: str = 'Locked'):
    """A decorator for functions that may raise the 'Locked' exception that returns the given value instead of letting 'Locked' to bubble.

    Usage:

    >>> lock = Lock('key')
    >>> @return_when_locked('already locked')
    >>> @lock(blocking=False)
    >>> def do_something():
    >>>     return 'done'

    When calling 'do_something()' while the lock has been already acquired, 'already locked' will be returned instead of raising the 'Locked'
    exception.
    """
    def real_decorator(fn):
        """Decorator function."""
        @wraps(fn)
        def inner(*args, **kwargs):
            """Inner function."""
            try:
                return fn(*args, **kwargs)
            except Locked:
                return return_value_when_locked
        return inner
    return real_decorator
