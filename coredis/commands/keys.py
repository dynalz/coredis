import datetime
from typing import List, Literal, Optional, Union

from deprecated.sphinx import deprecated, versionadded, versionchanged

from coredis.exceptions import ResponseError
from coredis.tokens import PureToken
from coredis.utils import (
    NodeFlag,
    b,
    bool_ok,
    dict_merge,
    first_key,
    int_or_none,
    list_keys_to_dict,
    merge_result,
    nativestr,
    normalized_milliseconds,
    normalized_seconds,
    normalized_time_milliseconds,
    normalized_time_seconds,
    string_keys_to_dict,
)
from coredis.validators import mutually_inclusive_parameters

from . import CommandMixin


def parse_object(response, infotype):
    """Parse the results of an OBJECT command"""

    if infotype in ("idletime", "refcount"):
        return int_or_none(response)

    return response


def parse_scan(response, **options):
    cursor, r = response

    return int(cursor), r


class KeysCommandMixin(CommandMixin):

    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict("EXISTS EXPIRE EXPIREAT " "MOVE PERSIST RENAMENX", bool),
        {
            "COPY MOVE PERSIST": bool,
            "DEL": int,
            "OBJECT": parse_object,
            "RANDOMKEY": lambda r: r and r or None,
            "SCAN": parse_scan,
            "RENAME": bool_ok,
            "MIGRATE": lambda r: nativestr(r) != "NOKEY",
        },
    )

    @versionadded(version="3.0.0")
    async def copy(
        self,
        source: str,
        destination: str,
        db: Optional[int] = None,
        replace: Optional[bool] = None,
    ) -> bool:
        """
        Copy a key
        """
        pieces = []

        if db is not None:
            pieces.extend(["DB", db])

        if replace:
            pieces.append("REPLACE")

        return await self.execute_command("COPY", source, destination, *pieces)

    async def delete(self, *keys: str) -> int:
        """
        Delete one or more keys specified by ``keys``

        :return: The number of keys that were removed.
        """

        return await self.execute_command("DEL", *keys)

    async def dump(self, key: str) -> str:
        """
        Return a serialized version of the value stored at the specified key.

        :return: the serialized value
        """

        return await self.execute_command("DUMP", key)

    async def exists(self, *keys: str) -> int:
        """
        Determine if a key exists

        :return: the number of keys that exist from those specified as arguments.
        """

        return await self.execute_command("EXISTS", *keys)

    async def expire(self, key: str, seconds: Union[int, datetime.timedelta]) -> bool:
        """
        Set a key's time to live in seconds



        :return: if the timeout was set or not set.
         e.g. key doesn't exist, or operation skipped due to the provided arguments.
        """

        return await self.execute_command("EXPIRE", key, normalized_seconds(seconds))

    async def expireat(
        self, key: str, timestamp: Union[int, datetime.datetime]
    ) -> bool:
        """
        Set the expiration for a key to a specific time


        :return: if the timeout was set or no.
         e.g. key doesn't exist, or operation skipped due to the provided arguments.

        """

        return await self.execute_command(
            "EXPIREAT", key, normalized_time_seconds(timestamp)
        )

    async def keys(self, pattern: str = "*") -> list:
        """
        Find all keys matching the given pattern

        :return: list of keys matching ``pattern``.
        """

        return await self.execute_command("KEYS", pattern)

    @versionadded(version="3.0.0")
    @mutually_inclusive_parameters("username", "password")
    async def migrate(
        self,
        keys: List[str],
        host: str,
        port: str,
        destination_db: int,
        timeout: int,
        copy: Optional[bool] = None,
        replace: Optional[bool] = None,
        auth: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> bool:
        """
        Atomically transfer key(s) from a Redis instance to another one.


        :return: If all keys were found found in the source instance.
        """

        if not keys:
            raise DataError("MIGRATE requires at least one key")
        pieces = []

        if copy:
            pieces.append(b"COPY")

        if replace:
            pieces.append(b"REPLACE")

        if auth:
            pieces.append(b"AUTH")
            pieces.append(auth)

        if username and password:
            pieces.append(b"AUTH2")
            pieces.append(username)
            pieces.append(password)

        pieces.append(b"KEYS")
        pieces.extend(keys)

        return await self.execute_command(
            "MIGRATE", host, port, "", destination_db, timeout, *pieces
        )

    async def move(self, key: str, db: int) -> bool:
        """Move a key to another database"""

        return await self.execute_command("MOVE", key, db)

    @versionadded(version="2.1.0")
    async def object_encoding(self, key: str) -> Optional[str]:
        """
        Return the internal encoding for the object stored at ``key``

        :return: the encoding of the object, or ``None`` if the key doesn't exist
        """

        return await self.execute_command("OBJECT ENCODING", key)

    @versionadded(version="2.1.0")
    async def object_freq(self, key: str) -> int:
        """
        Return the logarithmic access frequency counter for the object
        stored at ``key``

        :return: The counter's value.
        """

        return await self.execute_command("OBJECT FREQ", key)

    @versionadded(version="2.1.0")
    async def object_idletime(self, key: str) -> int:
        """
        Return the time in seconds since the last access to the object
        stored at ``key``

        :return: The idle time in seconds.
        """

        return await self.execute_command("OBJECT IDLETIME", key)

    @versionadded(version="2.1.0")
    async def object_refcount(self, key: str) -> int:
        """
        Return the reference count of the object stored at ``key``

        :return: The number of references.
        """

        return await self.execute_command("OBJECT REFCOUNT", key)

    @deprecated(
        reason="""
            Use explicit methods:

                - :meth:`object_encoding`
                - :meth:`object_freq`
                - :meth:`object_idletime`
                - :meth:`object_refcount`
            """,
        version="3.0.0",
    )
    async def object(self, infotype, key):
        """Returns the encoding, idletime, or refcount about the key"""

        return await self.execute_command("OBJECT", infotype, key, infotype=infotype)

    async def persist(self, key: str) -> bool:
        """Removes an expiration on ``key``"""

        return await self.execute_command("PERSIST", key)

    async def pexpire(
        self, key: str, milliseconds: Union[int, datetime.timedelta]
    ) -> int:
        """
        Set a key's time to live in milliseconds

        :return: if the timeout was set or not.
         e.g. key doesn't exist, or operation skipped due to the provided arguments.
        """

        return await self.execute_command(
            "PEXPIRE", key, normalized_milliseconds(milliseconds)
        )

    async def pexpireat(
        self, key: str, milliseconds_timestamp: Union[int, datetime.datetime]
    ) -> int:
        """
        Set the expiration for a key as a UNIX timestamp specified in milliseconds

        :return: if the timeout was set or not.
         e.g. key doesn't exist, or operation skipped due to the provided arguments.
        """

        return await self.execute_command(
            "PEXPIREAT", key, normalized_time_milliseconds(milliseconds_timestamp)
        )

    async def pttl(self, key: str) -> int:
        """
        Returns the number of milliseconds until the key ``key`` will expire

        :return: TTL in milliseconds, or a negative value in order to signal an error
        """

        return await self.execute_command("PTTL", key)

    async def randomkey(self) -> Optional[str]:
        """
        Returns the name of a random key

        :return: the random key, or ``None`` when the database is empty.
        """

        return await self.execute_command("RANDOMKEY")

    async def rename(self, key: str, newkey: str) -> str:
        """
        Rekeys key ``key`` to ``newkey``
        """

        return await self.execute_command("RENAME", key, newkey)

    async def renamenx(self, key, newkey: str) -> bool:
        """
        Rekeys key ``key`` to ``newkey`` if ``newkey`` doesn't already exist

        :return: False when ``newkey`` already exists.
        """

        return await self.execute_command("RENAMENX", key, newkey)

    async def restore(
        self,
        key: str,
        ttl: int,
        serialized_value: str,
        replace: Optional[bool] = None,
        absttl: Optional[bool] = None,
        idletime: Optional[Union[int, datetime.timedelta]] = None,
        freq: Optional[int] = None,
    ) -> bool:
        """
        Create a key using the provided serialized value, previously obtained using DUMP.
        """
        params = [key, ttl, serialized_value]

        if replace:
            params.append("REPLACE")

        if absttl:
            params.append("ABSTTL")

        if idletime is not None:
            params.extend(["IDLETIME", normalized_milliseconds(idletime)])

        if freq:
            params.extend(["FREQ", freq])

        return await self.execute_command("RESTORE", *params)

    @versionchanged(
        reason="""
        - Changed ``start`` to :paramref:`offset`
        - Changed ``num`` to :paramref:`count`
        - Changed ``get`` to :paramref:`gets` (:class:`List[str]`)
        - Moved ``asc`` and ``desc`` to :paramref:`order` (:class:`PureToken`)
        - Moved ``alpha`` to :paramref:`sorting` (:class:`PureToken`)
        """,
        version="3.0.0",
    )
    @mutually_inclusive_parameters("offset", "count")
    async def sort(
        self,
        key: str,
        gets: List[str] = [],
        by: Optional[str] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = None,
        alpha: Optional[bool] = None,
        store: Optional[str] = None,
    ) -> Union[list, int]:
        """
        Sort the elements in a list, set or sorted set

        :return: a list of sorted elements.

         When the ``store`` option is specified the command returns the number of sorted elements
         in the destination list.
        """

        pieces = [key]

        if by is not None:
            pieces.append(b("BY"))
            pieces.append(by)

        if offset is not None and count is not None:
            pieces.append(b("LIMIT"))
            pieces.append(offset)
            pieces.append(count)

        for g in gets:
            pieces.append(b("GET"))
            pieces.append(g)

        if order:
            pieces.append(order.value)

        if alpha is not None:
            pieces.append("ALPHA")

        if store is not None:
            pieces.append(b("STORE"))
            pieces.append(store)

        options = {}

        return await self.execute_command("SORT", *pieces, **options)

    async def touch(self, *keys: str) -> int:
        """
        Alters the last access time of a key(s).
        Returns the number of existing keys specified.

        :return: The number of keys that were touched.
        """
        return await self.execute_command("TOUCH", *keys)

    async def ttl(self, key: str) -> int:
        """
        Get the time to live for a key in seconds

        :return: TTL in seconds, or a negative value in order to signal an error
        """

        return await self.execute_command("TTL", key)

    async def type(self, key: str) -> str:
        """
        Determine the type stored at key

        :return: type of ``key``, or ``None`` when ``key`` does not exist.
        """

        return await self.execute_command("TYPE", key)

    async def unlink(self, *keys: str) -> int:
        """
        Delete a key asynchronously in another thread.
        Otherwise it is just as :meth:`delete`, but non blocking.

        :return: The number of keys that were unlinked.
        """

        return await self.execute_command("UNLINK", *keys)

    async def wait(self, numreplicas: int, timeout: int) -> int:
        """
        Wait for the synchronous replication of all the write commands sent in the context of
        the current connection

        :return: The command returns the number of replicas reached by all the writes performed
         in the context of the current connection.
        """

        return await self.execute_command("WAIT", numreplicas, timeout)

    async def scan(
        self,
        cursor: int = 0,
        match: Optional[str] = None,
        count: Optional[int] = None,
        type_: Optional[str] = None,
    ) -> None:
        """
        Incrementally iterate the keys space
        """
        pieces = [cursor]

        if match is not None:
            pieces.extend([b("MATCH"), match])

        if count is not None:
            pieces.extend([b("COUNT"), count])

        if type_ is not None:
            pieces.extend([b("TYPE"), type_])

        return await self.execute_command("SCAN", *pieces)


class ClusterKeysCommandMixin(KeysCommandMixin):

    NODES_FLAGS = dict_merge(
        {
            "MOVE": NodeFlag.BLOCKED,
            "RANDOMKEY": NodeFlag.RANDOM,
            "SCAN": NodeFlag.ALL_MASTERS,
        },
        list_keys_to_dict(["KEYS"], NodeFlag.ALL_NODES),
    )

    RESULT_CALLBACKS = {
        "KEYS": merge_result,
        "RANDOMKEY": first_key,
        "SCAN": lambda res: res,
    }

    async def rename(self, src, dst):
        """
        Rename key ``src`` to ``dst``

        Cluster impl:
            This operation is no longer atomic because each key must be querried
            then set in separate calls because they maybe will change cluster node
        """

        if src == dst:
            raise ResponseError("source and destination objects are the same")

        data = await self.dump(src)

        if data is None:
            raise ResponseError("no such key")

        ttl = await self.pttl(src)

        if ttl is None or ttl < 1:
            ttl = 0

        await self.delete(dst)
        await self.restore(dst, ttl, data)
        await self.delete(src)

        return True

    async def delete(self, *keys):
        """
        "Delete one or more keys specified by ``keys``"

        Cluster impl:
            Iterate all keys and send DELETE for each key.
            This will go a lot slower than a normal delete call in StrictRedis.

            Operation is no longer atomic.
        """
        count = 0

        for arg in keys:
            count += await self.execute_command("DEL", arg)

        return count

    async def renamenx(self, src, dst):
        """
        Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist

        Cluster impl:
            Check if dst key do not exists, then calls rename().

            Operation is no longer atomic.
        """

        if not await self.exists(dst):
            return await self.rename(src, dst)

        return False
