"""Custom test runners for mocking the ZookeeperClient."""

from unittest import mock

from django.test.runner import DiscoverRunner

from zookeeper_locks.tests.utils import FakeZookeeperClient


class ZookeeperClientMockingTestRunner(DiscoverRunner):

    """Test runner that mocks the Zookeeper client created with the zookeeper_connection_manager."""

    def run_tests(self, *args, **kwargs):  # pylint: disable=arguments-differ
        """Mock the `_create_client` method while running tests."""
        with mock.patch('zookeeper_locks.connection.zookeeper_connection_manager._create_client', return_value=FakeZookeeperClient()):
            return super().run_tests(*args, **kwargs)
