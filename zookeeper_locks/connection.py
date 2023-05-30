"""Utility for managing the Zookeeper connection for ZookeeperClient instances."""

import threading
from contextlib import ContextDecorator

from kazoo.client import KazooClient as ZookeeperClient
from kazoo.exceptions import ConnectionClosedError

from django.conf import settings


__all__ = ['zookeeper_connection_manager']


class ZookeeperConnectionManager(ContextDecorator):

    """A context manager / decorator for managing the Zookeeper connection."""

    # data holds objects shared among all instances of ZookeeperConnectionManager:
    #  - client - the ZookeeperClient instance
    #  - reference_counter - used to determine when to stop the client connection
    _data = threading.local()
    _data_lock = threading.Lock()

    def start_context(self, *args, **kwargs):
        """Increment the reference counter."""
        with self._data_lock:
            if not hasattr(self._data, 'reference_counter'):
                self._data.reference_counter = 0
            self._data.reference_counter += 1

    def stop_context(self, *args, **kwargs):
        """Decrement the reference counter and stop the connection while exiting the outermost context."""
        with self._data_lock:
            assert self.is_managed, 'Calling stop_context before start_context.'
            self._data.reference_counter -= 1
            if self._data.reference_counter == 0 and self.has_client:
                self._stop_connection()

    def get_client(self) -> ZookeeperClient:
        """Return a connected ZookeeperClient instance."""
        with self._data_lock:
            assert self.is_managed, 'Use the zookeeper_locks.connection.ZookeeperConnectionManager as a context manager or decorator.'
            if not self.has_client:
                self._start_connection()
            return self._data.client

    @property
    def is_managed(self) -> bool:
        """Determine if the managed context is active."""
        return getattr(self._data, 'reference_counter', 0) > 0

    @property
    def has_client(self) -> bool:
        """Determine if the client has been created."""
        return bool(getattr(self._data, 'client', None))

    def __enter__(self) -> 'ZookeeperConnectionManager':
        """Enter the context when the manager is used as a context manager or decorator."""
        self.start_context()
        return self

    def __exit__(self, exc_type, *exc) -> bool:
        """Exit the context when the manager is used as a context manager or decorator."""
        self.stop_context()
        if exc_type is ConnectionClosedError and self.has_client:
            self._data.client.restart()
        return False

    def _start_connection(self):
        """Create a ZookeeperClient instance and establish a connection."""
        self._data.client = self._create_client()
        self._data.client.start()

    def _stop_connection(self):
        """Stop the client connection and set the client attribute to None."""
        self._data.client.stop()
        self._data.client = None

    def _create_client(self) -> ZookeeperClient:
        return ZookeeperClient(
            hosts=','.join(settings.ZOOKEEPER_HOSTS),
        )


zookeeper_connection_manager = ZookeeperConnectionManager()
