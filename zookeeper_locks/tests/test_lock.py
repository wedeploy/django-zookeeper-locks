"""Tests for the Lock class."""

from unittest import mock

from kazoo.exceptions import LockTimeout as ZookeeperLockTimeout

from django.core.exceptions import ImproperlyConfigured
from django.test import TestCase

from zookeeper_locks.locks import (
    Lock,
    Locked,
    LockTimeout,
    return_when_locked,
)

from .utils import (
    FakeZookeeperClient,
    parametrize,
    parametrized_test_case,
)


@parametrized_test_case
class LockTestCase(TestCase):

    """Test the Lock class."""

    def test_registering_locks(self):
        """Test registering and checking lock keys."""
        lock1 = Lock('key1')
        self.assertEqual(lock1.key, 'key1')
        self.assertIn(lock1.key, Lock.keys_registry)
        lock2 = Lock('key2')
        self.assertIn(lock1.key, Lock.keys_registry)
        self.assertIn(lock2.key, Lock.keys_registry)
        lock3 = None
        with self.assertRaises(ImproperlyConfigured) as cm:
            lock3 = Lock('key2')
        self.assertTupleEqual(cm.exception.args, ('Attempt to register the same key twice: key2', ))
        self.assertIsNone(lock3)
        self.assertIn(lock1.key, Lock.keys_registry)
        self.assertIn(lock2.key, Lock.keys_registry)
        del lock1
        self.assertNotIn('key1`', Lock.keys_registry)
        self.assertIn(lock2.key, Lock.keys_registry)

    @parametrize(
        ['key', 'key_params', 'zookeeper_key', 'blocking', 'timeout'],
        {
            'without_params': ['sample-key', {}, '/locks/django-zookeeper-test-app/sample-key', True, None],
            'with_params': ['sample-key-{param}', {'param': 21}, '/locks/django-zookeeper-test-app/sample-key-21', True, None],
            'blocking_timeout': ['sample-key', {}, '/locks/django-zookeeper-test-app/sample-key', True, 10.0],
            'not_blocking': ['sample-key', {}, '/locks/django-zookeeper-test-app/sample-key', False, None],
        }
    )
    def test_locking(self, key, key_params, zookeeper_key, blocking, timeout):
        """Test successful lock scenarios."""
        lock = Lock(key)
        fake_client = FakeZookeeperClient()
        self.assertFalse(fake_client.started)
        with mock.patch('zookeeper_locks.locks.zookeeper_connection_manager._create_client', return_value=fake_client), \
                mock.patch.object(fake_client.Lock, '__init__', return_value=None) as init_mock, \
                mock.patch.object(fake_client.Lock, 'acquire', return_value=True) as acquire_mock, \
                mock.patch.object(fake_client.Lock, 'release') as release_mock:
            with lock(blocking=blocking, timeout=timeout, **key_params):
                self.assertTrue(fake_client.started)
                init_mock.assert_called_once_with(zookeeper_key)
                acquire_mock.assert_called_once_with(blocking=blocking, timeout=timeout)
                release_mock.assert_not_called()
        release_mock.assert_called_once_with()
        self.assertFalse(fake_client.started)

    @parametrize(
        ['blocking', 'timeout', 'acquire_exception', 'expected_exception', 'exception_message'], {
            'blocking_timeout': [True, 10.0, ZookeeperLockTimeout, LockTimeout, 'Timeout occurred while trying to acquire a blocking lock on key'],
            'not_blocking': [False, None, None, Locked, 'Failed to acquire a non-blocking lock on key'],
        }
    )
    def test_locking_failed(self, blocking, timeout, acquire_exception, expected_exception, exception_message):
        """Test unsuccessful lock scenarios."""
        lock = Lock('key')
        fake_client = FakeZookeeperClient()
        self.assertFalse(fake_client.started)
        with mock.patch('zookeeper_locks.locks.zookeeper_connection_manager._create_client', return_value=fake_client), \
                mock.patch.object(fake_client.Lock, '__init__', return_value=None) as init_mock, \
                mock.patch.object(fake_client.Lock, 'acquire', return_value=False, side_effect=acquire_exception) as acquire_mock, \
                mock.patch.object(fake_client.Lock, 'release') as release_mock:
            with self.assertRaises(expected_exception) as cm:
                with lock(blocking=blocking, timeout=timeout):
                    self.fail('Should not be executed.')
            self.assertTupleEqual(cm.exception.args, (exception_message, ))
            init_mock.assert_called_once_with('/locks/django-zookeeper-test-app/key')
            acquire_mock.assert_called_once_with(blocking=blocking, timeout=timeout)
            release_mock.assert_not_called()
        self.assertTrue(fake_client.ever_started)
        self.assertFalse(fake_client.started)

    def test_nested_locks(self):
        """Test if nested locks are acquirable."""
        lock = Lock('key')
        fake_client = FakeZookeeperClient()
        assert not fake_client.started
        with mock.patch('zookeeper_locks.locks.zookeeper_connection_manager._create_client', return_value=fake_client), \
                mock.patch.object(fake_client.Lock, '__init__', return_value=None) as init_mock, \
                mock.patch.object(fake_client.Lock, 'acquire', return_value=True) as acquire_mock, \
                mock.patch.object(fake_client.Lock, 'release') as release_mock:

            with lock(blocking=False):
                with lock(blocking=False):
                    pass
            init_mock.assert_called_once_with('/locks/django-zookeeper-test-app/key')
            acquire_mock.assert_called_once_with(blocking=False, timeout=None)
            release_mock.assert_called_once()


class ReturnWhenLockedTestCase(TestCase):

    """Test of the decorator return_when_locked."""

    @staticmethod
    def get_test_method_with_lock(do_raise):
        """Get method wrapped by lock."""
        def lock(fn):
            """Fake lock"""
            def inner():
                """Call fake task or raise already locked"""
                if do_raise:
                    raise Locked()
                else:
                    return fn()
            return inner

        @return_when_locked('already locked')
        @lock
        def do_something_task():
            """A tested method."""
            return 'done'

        return do_something_task

    def test_locking(self):
        """Test successful lock scenarios."""
        method = self.get_test_method_with_lock(False)
        result = method()
        self.assertEqual(result, 'done')

    def test_locking_failed(self):
        """Test unsuccessful lock scenarios."""
        method = self.get_test_method_with_lock(True)
        result = method()
        self.assertEqual(result, 'already locked')
