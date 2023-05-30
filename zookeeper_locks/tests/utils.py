"""Utils for zookeeper utils tests."""

from typing import (
    Callable,
    Iterable,
    Optional,
    Type,
    Union,
)

from django.test import TestCase


class FakeZookeeperClient:

    """A dummy zookeeper client for testing purposes."""

    class Lock:

        """A dummy lock class."""

        def __init__(self, key):  # pylint: disable=unused-argument
            """Mock the original `__init__` method."""

        def acquire(self, blocking=True, timeout=None):  # pylint: disable=unused-argument
            """Mock the original `acquire` method."""
            return True

        def release(self):
            """Mock the original `release` method."""

    def __init__(self):
        """Initialize the dummy client attributes."""
        self.started = False
        self.ever_started = False
        self.restarted = False

    def start(self):
        """Set the started attribute to True."""
        self.started = True
        self.ever_started = True

    def stop(self):
        """Set the started attribute to False."""
        self.started = False

    def restart(self):
        """Set restarted attribute to True. Call stop and start."""
        self.restarted = True
        self.stop()
        self.start()


def params_to_kwargs(params_names: Iterable, params: Iterable) -> dict:
    """Merge iterables of names and values to dict"""
    return dict(zip(params_names, params))


def parametrize(params_names: Iterable, params: Union[Iterable, dict], custom_suffix_func: Optional[Callable]=None) -> Callable:
    """Decorator used to parametrize test method with supplied param_names.

    Params should be iterable with lists of params in the same order as in param_names or dict with those lists as values.
    In the first case tests suffixes will be generated from param iterable indexes, in the second from dict keys.
    To create custom tests suffixes supply custom_suffix_func which takes __cls argument (TestCase class) and all params of current test,
    should return string - custom suffix.
    """
    assert not any(param_name.startswith('__') for param_name in params_names), "Param names can't start with __."

    if isinstance(params, dict):
        params_list = params.items()
    else:
        params_list = enumerate(params)

    params = {k: params_to_kwargs(params_names, v) for k, v in params_list}
    suffix_func = custom_suffix_func or (lambda **kwargs: kwargs['__suffix'])

    def decorator(func: Callable) -> Callable:
        """Decorator function"""
        func.is_parametrized = True
        func.params = params
        func.suffix_func = suffix_func
        return func

    return decorator


def parametrized_test_case(cls: Type[TestCase]) -> Type[TestCase]:
    """Decorator for TestCase to enable use of parametrize decorator on its test methods"""
    def test_wrapper(test: Callable, **kwargs):
        """Create test executor"""
        def test_executor(instance: TestCase):
            """Execute test"""
            return test(instance, **kwargs)
        return test_executor

    for name, func in list(cls.__dict__.items()):
        if not getattr(func, 'is_parametrized', False):
            continue
        for suffix, params in func.params.items():
            setattr(cls, '{}__{}'.format(name, func.suffix_func(__suffix=suffix, __cls=cls, **params)), test_wrapper(func, **params))
        delattr(cls, name)
    return cls
