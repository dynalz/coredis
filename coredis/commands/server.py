import datetime
from typing import Any, Dict, List, Literal, Optional, Union

from deprecated.sphinx import versionadded

from coredis.exceptions import RedisError
from coredis.tokens import PureToken
from coredis.utils import (
    NodeFlag,
    b,
    bool_ok,
    bool_ok_or_int,
    dict_merge,
    list_keys_to_dict,
    nativestr,
    pairs_to_dict,
    string_if_bytes,
    string_keys_to_dict,
)
from coredis.validators import mutually_inclusive_parameters

from . import CommandMixin


def parse_slowlog_get(response, **options):
    return [
        {
            "id": item[0],
            "start_time": int(item[1]),
            "duration": int(item[2]),
            "command": b(" ").join(item[3]),
        }
        for item in response
    ]


def parse_client_info(info):
    # Values might contain '='

    return dict([pair.split("=", 1) for pair in nativestr(info).split(" ")])


def parse_client_list(response, **options):
    clients = []

    for c in response.splitlines():
        clients.append(parse_client_info(c))

    return clients


def parse_tracking_info(response, **options):
    values = [string_if_bytes(r) for r in response]

    return dict(zip(values[::2], values[1::2]))


def parse_config_get(response, **options):
    response = [nativestr(i) if i is not None else None for i in response]

    return response and pairs_to_dict(response) or {}


def timestamp_to_datetime(response):
    """Converts a unix timestamp to a Python datetime object"""

    if not response:
        return None
    try:
        response = int(response)
    except ValueError:
        return None

    return datetime.datetime.fromtimestamp(response)


def parse_debug_object(response):
    """
    Parses the results of Redis's DEBUG OBJECT command into a Python dict
    """
    # The 'type' of the object is the first item in the response, but isn't
    # prefixed with a name
    response = nativestr(response)
    response = "type:" + response
    response = dict([kv.split(":") for kv in response.split()])

    # parse some expected int values from the string response
    # note: this cmd isn't spec'd so these may not appear in all redis versions
    int_fields = ("refcount", "serializedlength", "lru", "lru_seconds_idle")

    for field in int_fields:
        if field in response:
            response[field] = int(response[field])

    return response


def parse_info(response):
    """Parses the result of Redis's INFO command into a Python dict"""
    info = {}
    response = nativestr(response)

    def get_value(value):
        if "," not in value or "=" not in value:
            try:
                if "." in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                return value
        else:
            sub_dict = {}

            for item in value.split(","):
                k, v = item.rsplit("=", 1)
                sub_dict[k] = get_value(v)

            return sub_dict

    for line in response.splitlines():
        if line and not line.startswith("#"):
            if line.find(":") != -1:
                key, value = line.split(":", 1)
                info[key] = get_value(value)
            else:
                # if the line isn't splittable, append it to the "__raw__" key
                info.setdefault("__raw__", []).append(line)

    return info


def parse_role(response):
    role = nativestr(response[0])

    def _parse_master(response):
        offset, slaves = response[1:]
        res = {"role": role, "offset": offset, "slaves": []}

        for slave in slaves:
            host, port, offset = slave
            res["slaves"].append(
                {"host": host, "port": int(port), "offset": int(offset)}
            )

        return res

    def _parse_slave(response):
        host, port, status, offset = response[1:]

        return {
            "role": role,
            "host": host,
            "port": port,
            "status": status,
            "offset": offset,
        }

    def _parse_sentinel(response):
        return {"role": role, "masters": response[1:]}

    parser = {
        "master": _parse_master,
        "slave": _parse_slave,
        "sentinel": _parse_sentinel,
    }[role]

    return parser(response)


class ServerCommandMixin(CommandMixin):
    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict("BGREWRITEAOF BGSAVE", lambda r: True),
        string_keys_to_dict("FLUSHALL FLUSHDB SAVE " "SHUTDOWN SLAVEOF", bool_ok),
        {
            "ROLE": parse_role,
            "SLOWLOG GET": parse_slowlog_get,
            "SLOWLOG LEN": int,
            "SLOWLOG RESET": bool_ok,
            "CLIENT TRACKING": bool_ok_or_int,
            "CLIENT GETNAME": lambda r: r and nativestr(r),
            "CLIENT KILL": bool_ok_or_int,
            "CLIENT INFO": parse_client_info,
            "CLIENT ID": lambda r: int(r),
            "CLIENT LIST": parse_client_list,
            "CLIENT TRACKINGINFO": parse_tracking_info,
            "CLIENT SETNAME": bool_ok,
            "CLIENT PAUSE": bool_ok,
            "CLIENT UNPAUSE": bool_ok,
            "CLIENT UNBLOCK": lambda r: int(r),
            "CONFIG GET": parse_config_get,
            "CONFIG RESETSTAT": bool_ok,
            "CONFIG SET": bool_ok,
            "DEBUG OBJECT": parse_debug_object,
            "INFO": parse_info,
            "LASTSAVE": timestamp_to_datetime,
            "TIME": lambda x: (int(x[0]), int(x[1])),
        },
    )

    async def bgrewriteaof(self):
        """Tell the Redis server to rewrite the AOF file from data in memory"""

        return await self.execute_command("BGREWRITEAOF")

    async def bgsave(self):
        """
        Tells the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.
        """

        return await self.execute_command("BGSAVE")

    async def client_kill(
        self,
        *,
        ip_port: Optional[str] = None,
        id_: Optional[int] = None,
        type_: Optional[
            Literal[
                PureToken.NORMAL,
                PureToken.MASTER,
                PureToken.SLAVE,
                PureToken.REPLICA,
                PureToken.PUBSUB,
            ]
        ] = None,
        user: Optional[str] = None,
        addr: Optional[str] = None,
        laddr: Optional[str] = None,
        skipme: Optional[bool] = None
    ) -> Union[int, bool]:

        """
        Disconnects the client at ``ip_port``

        :return: True if the connection exists and has been closed
         or the number of clients killed.

        ... versionchanged:: 3.0.0
        """

        pieces = []

        if ip_port:
            pieces.append(ip_port)

        if id_:
            pieces.extend(["ID", id_])

        if type_:
            pieces.extend(["TYPE", type_.value])

        if user:
            pieces.extend(["USER", user])

        if addr:
            pieces.extend(["ADDR", addr])

        if laddr:
            pieces.extend(["LADDR", laddr])

        if laddr:
            pieces.extend(["SKIPME", skipme and "yes" or "no"])

        return await self.execute_command("CLIENT KILL", *pieces)

    async def client_list(
        self,
        *,
        type_: Optional[
            Literal[
                PureToken.NORMAL, PureToken.MASTER, PureToken.REPLICA, PureToken.PUBSUB
            ]
        ] = None,
        client_id: Optional[int] = None
    ) -> List[Dict[str, str]]:
        """
        Get the list of client connections
        :return: a list of dictionaries containing client fields
        """

        pieces = []

        if type_:
            pieces.extend(["TYPE", type_.value])

        if client_id is not None:
            pieces.extend(["ID", client_id])

        return await self.execute_command("CLIENT LIST", *pieces)

    async def client_getname(self) -> Optional[str]:
        """
        Returns the current connection name

        :return: The connection name, or ``None`` if no name is set.
        """

        return await self.execute_command("CLIENT GETNAME")

    async def client_setname(self, connection_name: str) -> bool:
        """
        Set the current connection name
        :return: ```True``` if the connection name was successfully set.
        """

        return await self.execute_command("CLIENT SETNAME", connection_name)

    async def client_pause(
        self,
        timeout: int,
        *,
        mode: Optional[Literal[PureToken.WRITE, PureToken.ALL]] = None
    ) -> bool:
        """
        Stop processing commands from clients for some time

        :return: The command returns ``True`` or raises an error if the timeout is invalid.

        .. versionchanged:: 3.0.0

          Added the optional :paramref:`mode` parameter

        """

        return await self.execute_command("CLIENT PAUSE", timeout)

    async def client_unpause(self) -> bool:
        """
        Resume processing of clients that were paused

        :return: The command returns ```True```

        .. versionadded:: 3.0.0
        """

        return await self.execute_command("CLIENT UNPAUSE")

    async def client_unblock(
        self,
        client_id: int,
        *,
        timeout_error: Optional[Literal[PureToken.TIMEOUT, PureToken.ERROR]] = None
    ) -> int:
        """
        Unblock a client blocked in a blocking command from a different connection

        :return: 1 if client was unblocked else 0

        .. versionadded:: 3.0.0
        """
        pieces = [client_id]

        if timeout_error is not None:
            pieces.append(timeout_error.value)

        return await self.execute_command("CLIENT UNBLOCK", *pieces)

    async def client_getredir(self) -> int:
        """
        Get tracking notifications redirection client ID if any

        :return: the ID of the client we are redirecting the notifications to.
         The command returns ``-1`` if client tracking is not enabled,
         or ``0`` if client tracking is enabled but we are not redirecting the
         notifications to any client.


        .. versionadded:: 3.0.0
        """

        return await self.execute_command("CLIENT GETREDIR")

    async def client_id(self) -> int:
        """
        Returns the client ID for the current connection

        :return: The id of the client.

        .. versionadded:: 3.0.0
        """

        return await self.execute_command("CLIENT ID")

    async def client_info(self) -> Dict[str, str]:
        """
        Returns information about the current client connection.

        .. versionadded:: 3.0.0
        """

        return await self.execute_command("CLIENT INFO")

    async def client_reply(
        self, mode: Literal[PureToken.ON, PureToken.OFF, PureToken.SKIP]
    ) -> bool:
        """
        Instruct the server whether to reply to commands

        :return: ```True```.

        .. versionadded:: 3.0.0
        """

        return self.execute_command("CLIENT REPLY", [mode.value])

    async def client_tracking(
        self,
        status: Literal[PureToken.ON, PureToken.OFF],
        *,
        prefixes: Optional[List[Optional[str]]] = None,
        redirect: Optional[int] = None,
        bcast: Optional[bool] = None,
        optin: Optional[bool] = None,
        optout: Optional[bool] = None,
        noloop: Optional[bool] = None
    ) -> bool:
        """
        Enable or disable server assisted client side caching support

        :return: ```True``` if the connection was successfully put in tracking mode or if the
         tracking mode was successfully disabled.

        .. versionadded:: 3.0.0
        """

        pieces = [status.value]

        if prefixes:
            pieces.extend(prefixes)

        if redirect is not None:
            pieces.extend(["REDIRECT", redirect])

        if bcast is not None:
            pieces.append("BCAST")

        if optin is not None:
            pieces.append("OPTIN")

        if optout is not None:
            pieces.append("OPTOUT")

        if noloop is not None:
            pieces.append("NOLOOP")

        return await self.execute_command("CLIENT TRACKING", *pieces)

    async def client_trackinginfo(self) -> Dict[str, str]:
        """
        Return information about server assisted client side caching for the current connection

        :return: a mapping of tracking information sections and their respective values

        .. versionadded:: 3.0.0
        """

        return await self.execute_command("CLIENT TRACKINGINFO")

    async def config_get(self, pattern="*"):
        """Returns a dictionary of configuration based on the ``pattern``"""

        return await self.execute_command("CONFIG GET", pattern)

    async def config_set(self, name, value):
        """Sets config item ``name`` to ``value``"""

        return await self.execute_command("CONFIG SET", name, value)

    async def config_resetstat(self):
        """Resets runtime statistics"""

        return await self.execute_command("CONFIG RESETSTAT")

    async def config_rewrite(self):
        """
        Rewrites config file with the minimal change to reflect running config
        """

        return await self.execute_command("CONFIG REWRITE")

    async def dbsize(self):
        """Returns the number of keys in the current database"""

        return await self.execute_command("DBSIZE")

    async def debug_object(self, key):
        """Returns version specific meta information about a given key"""

        return await self.execute_command("DEBUG OBJECT", key)

    async def flushall(self):
        """Deletes all keys in all databases on the current host"""

        return await self.execute_command("FLUSHALL")

    async def flushdb(self):
        """Deletes all keys in the current database"""

        return await self.execute_command("FLUSHDB")

    async def info(self, section=None):
        """
        Returns a dictionary containing information about the Redis server

        The ``section`` option can be used to select a specific section
        of information

        The section option is not supported by older versions of Redis Server,
        and will generate ResponseError
        """

        if section is None:
            return await self.execute_command("INFO")
        else:
            return await self.execute_command("INFO", section)

    async def lastsave(self):
        """
        Returns a Python datetime object representing the last time the
        Redis database was saved to disk
        """

        return await self.execute_command("LASTSAVE")

    @versionadded(version="3.0.0")
    async def memory_doctor(self) -> Any:
        """
        Outputs memory problems report





        :return:

        """

    @versionadded(version="3.0.0")
    async def memory_malloc_stats(self) -> str:
        """
        Show allocator internal stats





        :return:

        the memory allocator's internal statistics report

        """

    @versionadded(version="3.0.0")
    async def memory_malloc_stats(self) -> str:
        """
        Show allocator internal stats





        :return:

        the memory allocator's internal statistics report

        """

    @versionadded(version="3.0.0")
    async def memory_purge(self) -> str:
        """
        Ask the allocator to release memory





        :return:

        """

    @versionadded(version="3.0.0")
    async def memory_stats(self) -> list:
        """
        Show memory usage details





        :return:

        nested list of memory usage metrics and their values
        **A note about the word slave used in this man page**: Starting with Redis 5, if not for backward compatibility, the Redis project no longer uses the word slave. Unfortunately in this command the word slave is part of the protocol, so we'll be able to remove such occurrences only when this API will be naturally deprecated.

        """

    @versionadded(version="3.0.0")
    async def memory_usage(
        self, key: str, *, samples: Optional[int] = None
    ) -> Optional[int]:
        """
        Estimate the memory usage of a key


        :param key:
        :param samples:



        :return:

        the memory usage in bytes, or ``None`` when the key does not exist.

        """

    @versionadded(version="3.0.0")
    async def module_list(self) -> list:
        """
        List all modules loaded by the server





        :return:

        list of loaded modules. Each element in the list represents a
        module, and is in itself a list of property names and their values. The
        following properties is reported for each loaded module:
        *   ``name``: Name of the module
        *   ``ver``: Version of the module

        """

    @versionadded(version="3.0.0")
    async def module_load(self, *, path: str, args: Optional[List[str]]) -> bool:
        """
        Load a module


        :param path:
        :param args:



        :return:

        ``OK`` if module was loaded.

        """

    @versionadded(version="3.0.0")
    async def module_unload(self, *, name: str) -> bool:
        """
        Unload a module


        :param name:



        :return:

        ``OK`` if module was unloaded.

        """

    @versionadded(version="3.0.0")
    async def monitor(self) -> Any:
        """
        Listen for all requests received by the server in real time





        :return:

        """

    @versionadded(version="3.0.0")
    async def replicaof(self, *, host: str, port: str) -> str:
        """
        Make the server a replica of another instance, or promote it as master.


        :param host:
        :param port:



        :return:

        """

    @versionadded(version="3.0.0")
    async def swapdb(self, *, index1: int, index2: int) -> bool:
        """
        Swaps two Redis databases


        :param index1:
        :param index2:



        :return:

        ``OK`` if ``SWAPDB`` was executed correctly.

        """

    @mutually_inclusive_parameters("host", "port")
    @versionadded(version="3.0.0")
    async def failover(
        self,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        force: Optional[bool] = None,
        abort: Optional[bool] = None,
        timeout: Optional[Union[int, datetime.timedelta]] = None
    ) -> bool:
        """
        Start a coordinated failover between this server and one of its replicas.



        :param host:
        :param port:
        :param force:
        :param abort:
        :param timeout:


        :return:

        ``OK`` if the command was accepted and a coordinated failover is in progress. An error if the operation cannot be executed.

        """

    @versionadded(version="3.0.0")
    async def latency_doctor(self) -> str:
        """
        Return a human readable latency analysis report.





        :return:

        """

    @versionadded(version="3.0.0")
    async def latency_graph(self, *, event: str) -> Any:
        """
        Return a latency graph for the event.



        :param event:


        :return:

        """

    @versionadded(version="3.0.0")
    async def latency_history(self, *, event: str) -> list:
        """
        Return timestamp-latency samples for the event.



        :param event:


        :return:

        The command returns an array where each element is a two elements array
        representing the timestamp and the latency of the event.

        """

    @versionadded(version="3.0.0")
    async def latency_latest(self) -> list:
        """
        Return the latest latency samples for all events.





        :return:

        The command returns an array where each element is a four elements array
        representing the event's name, timestamp, latest and all-time latency measurements.

        """

    @versionadded(version="3.0.0")
    async def latency_reset(self, *, events: Optional[List[str]]) -> int:
        """
        Reset latency data for one or more events.



        :param events:


        :return:

        the number of event time series that were reset.

        """

    async def save(self):
        """
        Tells the Redis server to save its data to disk,
        blocking until the save is complete
        """

        return await self.execute_command("SAVE")

    async def shutdown(self):
        """Stops Redis server"""
        try:
            await self.execute_command("SHUTDOWN")
        except ConnectionError:
            # a ConnectionError here is expected

            return
        raise RedisError("SHUTDOWN seems to have failed.")

    async def slaveof(self, host=None, port=None):
        """
        Sets the server to be a replicated slave of the instance identified
        by the ``host`` and ``port``. If called without arguments, the
        instance is promoted to a master instead.
        """

        if host is None and port is None:
            return await self.execute_command("SLAVEOF", b("NO"), b("ONE"))

        return await self.execute_command("SLAVEOF", host, port)

    async def slowlog_get(self, num=None):
        """
        Gets the entries from the slowlog. If ``num`` is specified, get the
        most recent ``num`` items.
        """
        args = ["SLOWLOG GET"]

        if num is not None:
            args.append(num)

        return await self.execute_command(*args)

    async def slowlog_len(self):
        """Gets the number of items in the slowlog"""

        return await self.execute_command("SLOWLOG LEN")

    async def slowlog_reset(self):
        """Removes all items in the slowlog"""

        return await self.execute_command("SLOWLOG RESET")

    async def time(self):
        """
        Returns the server time as a 2-item tuple of ints:
        (seconds since epoch, microseconds into this second).
        """

        return await self.execute_command("TIME")

    async def role(self):
        """
        Provides information on the role of a Redis instance in the context of replication,
        by returning if the instance is currently a master, slave, or sentinel.
        The command also returns additional information about the state of the replication
        (if the role is master or slave)
        or the list of monitored master names (if the role is sentinel).
        :return:
        """

        return await self.execute_command("ROLE")

    async def lolwut(self, version, *arguments):
        """
        Get the Redis version and a piece of generative computer art

        ... versionadded:: 2.1.0
        """

        return await self.execute_command("LOLWUT VERSION", version, *arguments)


class ClusterServerCommandMixin(ServerCommandMixin):
    NODES_FLAGS = dict_merge(
        list_keys_to_dict(["SHUTDOWN", "SLAVEOF", "CLIENT SETNAME"], NodeFlag.BLOCKED),
        list_keys_to_dict(["FLUSHALL", "FLUSHDB"], NodeFlag.ALL_MASTERS),
        list_keys_to_dict(
            [
                "SLOWLOG LEN",
                "SLOWLOG RESET",
                "SLOWLOG GET",
                "TIME",
                "SAVE",
                "LASTSAVE",
                "DBSIZE",
                "CONFIG RESETSTAT",
                "CONFIG REWRITE",
                "CONFIG GET",
                "CONFIG SET",
                "CLIENT KILL",
                "CLIENT LIST",
                "CLIENT GETNAME",
                "INFO",
                "BGSAVE",
                "BGREWRITEAOF",
            ],
            NodeFlag.ALL_NODES,
        ),
    )

    RESULT_CALLBACKS = dict_merge(
        list_keys_to_dict(
            [
                "CONFIG GET",
                "CONFIG SET",
                "SLOWLOG GET",
                "CLIENT KILL",
                "INFO",
                "BGREWRITEAOF",
                "BGSAVE",
                "CLIENT LIST",
                "CLIENT GETNAME",
                "CONFIG RESETSTAT",
                "CONFIG REWRITE",
                "DBSIZE",
                "LASTSAVE",
                "SAVE",
                "SLOWLOG LEN",
                "SLOWLOG RESET",
                "TIME",
                "FLUSHALL",
                "FLUSHDB",
            ],
            lambda res: res,
        )
    )
