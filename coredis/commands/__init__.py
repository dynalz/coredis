import enum
import functools
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from packaging.version import parse


def redis_command(
    command_name: str,
    group: "CommandGroup",
    minimum_server_version: Optional[str] = None,
    arguments: Optional[Dict[str, str]] = {},
):
    if minimum_server_version:
        min_server_version = parse(minimum_server_version)

    def _inner(func):
        @functools.wraps(func)
        async def __inner(*a, **k):
            return await func(*a, *k)

        __inner.__doc__ = (
            __inner.__doc__
            + f"""

        Redis documentation: {_redis_command_link(command_name)}
        """
        )
        if minimum_server_version:
            __inner.__doc__ += f"""
        Introduced in Redis version ``{minimum_server_version}``

        """
        return __inner

    return _inner


class CommandMixin(ABC):
    @abstractmethod
    def execute_command(self, command: str, *args: Any, **kwargs: Any) -> Any:
        pass


class CommandGroup(enum.Enum):
    BITMAP = "bitmap"
    CLUSTER = "cluster"
    CONNECTION = "connection"
    GENERIC = "generic"
    GEO = "geo"
    HASH = "hash"
    HYPERLOGLOG = "hyperloglog"
    LIST = "list"
    PUBSUB = "pubsub"
    SCRIPTING = "scripting"
    SERVER = "server"
    SET = "set"
    SORTED_SET = "sorted-set"
    STREAM = "stream"
    STRING = "string"
    TRANSACTIONS = "transactions"


def _redis_command_link(command):
    return (
        f'`{command} <https://redis.io/commands/{command.lower().replace(" ", "-")}>`_'
    )
