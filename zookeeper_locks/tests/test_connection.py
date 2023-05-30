"""Tests for the ZookeeperConnectionManager class."""

from unittest import mock

from kazoo.exceptions import ConnectionClosedError

from django.test import TestCase

from zookeeper_locks.connection import zookeeper_connection_manager

from .utils import FakeZookeeperClient


class ZookeeperConnectionManagerTestCase(TestCase):

    """Test the ZookeeperConnectionManager."""

    def test_context_manager(self):
        """Test that the context manager interface of the manager has correct is_managed property."""
        self.assertFalse(zookeeper_connection_manager.is_managed)
        with zookeeper_connection_manager:
            self.assertTrue(zookeeper_connection_manager.is_managed)
        self.assertFalse(zookeeper_connection_manager.is_managed)

    def test_nesting_context_manager(self):
        """Test that the is_manager property has a proper value when nesting context managers."""
        with zookeeper_connection_manager:
            with zookeeper_connection_manager:
                self.assertTrue(zookeeper_connection_manager.is_managed)
            self.assertTrue(zookeeper_connection_manager.is_managed)
        self.assertFalse(zookeeper_connection_manager.is_managed)

    def test_decorator(self):
        """Test that the decorator interface of the manager has correct is_managed property."""
        @zookeeper_connection_manager
        def some_function():
            """Just assert that the is_manager property is True."""
            self.assertTrue(zookeeper_connection_manager.is_managed)
        self.assertFalse(zookeeper_connection_manager.is_managed)
        some_function()
        self.assertFalse(zookeeper_connection_manager.is_managed)

    def test_getting_client_out_of_context(self):
        """Test that getting a client out of the manager context raises an AssertionError."""
        self.assertFalse(zookeeper_connection_manager.is_managed)
        with self.assertRaises(AssertionError) as cm:
            zookeeper_connection_manager.get_client()
        self.assertFalse(zookeeper_connection_manager.has_client)
        self.assertTupleEqual(
            cm.exception.args,
            ('Use the zookeeper_locks.connection.ZookeeperConnectionManager as a context manager or decorator.', )
        )

    def test_getting_client(self):
        """Test that using the connection manager creates and connects the ZookeeperClient when necessary."""
        fake_client = FakeZookeeperClient()
        with mock.patch('zookeeper_locks.locks.zookeeper_connection_manager._create_client', return_value=fake_client), \
                mock.patch.object(fake_client, 'start', wraps=fake_client.start) as start_mock:
            with zookeeper_connection_manager:
                client = zookeeper_connection_manager.get_client()
                self.assertIs(client, fake_client)
                self.assertTrue(zookeeper_connection_manager.has_client)
                self.assertTrue(fake_client.started)
                zookeeper_connection_manager.get_client()
                with zookeeper_connection_manager:
                    self.assertTrue(zookeeper_connection_manager.has_client)
                    self.assertTrue(fake_client.started)
                    zookeeper_connection_manager.get_client()
                self.assertTrue(fake_client.started)
                start_mock.assert_called_once()
            self.assertFalse(fake_client.started)
            with zookeeper_connection_manager:
                zookeeper_connection_manager.get_client()
            start_mock.assert_has_calls([mock.call(), mock.call()])

    def test_restarting_client(self):
        """Test that connection is restarted on ConnectionClosedError when it's needed."""
        fake_client = FakeZookeeperClient()
        with mock.patch('zookeeper_locks.locks.zookeeper_connection_manager._create_client', return_value=fake_client), \
                mock.patch.object(fake_client, 'restart', wraps=fake_client.restart) as restart_mock:
            with self.assertRaises(ConnectionClosedError):
                with zookeeper_connection_manager:
                    zookeeper_connection_manager.get_client()
                    raise ConnectionClosedError()
            restart_mock.assert_not_called()

            with self.assertRaises(ConnectionClosedError):
                with zookeeper_connection_manager:
                    with zookeeper_connection_manager:
                        raise ConnectionClosedError()
            restart_mock.assert_not_called()

            with self.assertRaises(ConnectionClosedError):
                with zookeeper_connection_manager:
                    with zookeeper_connection_manager:
                        zookeeper_connection_manager.get_client()
                        raise ConnectionClosedError()
            restart_mock.assert_called_once()

