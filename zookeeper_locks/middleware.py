"""Middleware classes for using with the zookeeper library."""

from typing import Callable

from django.http.request import HttpRequest
from django.http.response import HttpResponse

from .connection import zookeeper_connection_manager


class ZookeeperConnectionMiddleware:

    """Runs the zookeeper connection manager while the request is being processed."""

    def __init__(self, get_response: Callable[[HttpRequest], HttpResponse]):
        """Store thr `get_response` function as an instance attribute."""
        self.get_response = get_response

    def __call__(self, request: HttpRequest) -> HttpResponse:
        """Generate the response in the managed context."""
        with zookeeper_connection_manager:
            return self.get_response(request)
