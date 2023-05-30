"""Multiprocessing hooks."""
from multiprocessing.util import Finalize

from zookeeper_locks.connection import zookeeper_connection_manager


def zookeeper_multiprocessing_connection_initializer():
    """Zookeeper multiprocessing connection initializer. Connection will not be closed on an raise of unhandled exception"""
    zookeeper_connection_manager.start_context()
    Finalize(zookeeper_connection_manager, zookeeper_connection_manager.stop_context, exitpriority=16)
