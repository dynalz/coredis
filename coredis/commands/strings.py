import datetime
import time
from typing import Dict, List, Literal, Optional, Union

from deprecated.sphinx import versionadded, versionchanged

from coredis.commands import CommandGroup, CommandMixin, redis_command
from coredis.exceptions import DataError, ReadOnlyError, RedisError
from coredis.tokens import PureToken
from coredis.utils import (
    NodeFlag,
    bool_ok,
    dict_merge,
    dict_to_flat_list,
    iteritems,
    list_or_args,
    nativestr,
    normalized_milliseconds,
    normalized_seconds,
    normalized_time_seconds,
    string_keys_to_dict,
)
from coredis.validators import (
    mutually_exclusive_parameters,
    mutually_inclusive_parameters,
)


class BitFieldOperation:
    """
    The command treats a Redis string as a array of bits,
    and is capable of addressing specific integer fields
    of varying bit widths and arbitrary non (necessary) aligned offset.

    The supported types are up to 64 bits for signed integers,
    and up to 63 bits for unsigned integers.

    Offset can be num prefixed with `#` character or num directly,
    for command detail you should see: https://redis.io/commands/bitfield
    """

    def __init__(self, redis_client, key, readonly=False):
        self._command_stack = ["BITFIELD" if not readonly else "BITFIELD_RO", key]
        self.redis = redis_client
        self.readonly = readonly

    def __del__(self):
        self._command_stack.clear()

    def set(self, type, offset, value):
        """
        Set the specified bit field and returns its old value.
        """

        if self.readonly:
            raise ReadOnlyError()

        self._command_stack.extend(["SET", type, offset, value])

        return self

    def get(self, type, offset):
        """
        Returns the specified bit field.
        """

        self._command_stack.extend(["GET", type, offset])

        return self

    def incrby(self, type, offset, increment):
        """
        Increments or decrements (if a negative increment is given)
        the specified bit field and returns the new value.
        """

        if self.readonly:
            raise ReadOnlyError()

        self._command_stack.extend(["INCRBY", type, offset, increment])

        return self

    def overflow(self, type="SAT"):
        """
        fine-tune the behavior of the increment or decrement overflow,
        have no effect unless used before `incrby`
        three types are available: WRAP|SAT|FAIL
        """

        if self.readonly:
            raise ReadOnlyError()
        self._command_stack.extend(["OVERFLOW", type])

        return self

    async def exc(self):
        """execute commands in command stack"""

        return await self.redis.execute_command(*self._command_stack)


class StringsCommandMixin(CommandMixin):
    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict("MSETNX PSETEX SETEX SETNX", bool),
        string_keys_to_dict(
            "BITCOUNT BITPOS DECRBY GETBIT INCRBY " "STRLEN SETBIT", int
        ),
        {
            "INCRBYFLOAT": float,
            "MSET": bool_ok,
            "SET": lambda r: r and nativestr(r) == "OK",
        },
    )

    @redis_command("APPEND", group=CommandGroup.STRING)
    async def append(self, key: str, value: str) -> int:
        """
        Append a value to a key

        :return: the length of the string after the append operation.
        """

        return await self.execute_command("APPEND", key, value)

    @versionchanged(version="3.0.0")
    @mutually_inclusive_parameters("start", "end")
    async def bitcount(
        self, key: str, *, start: Optional[int] = None, end: Optional[int] = None
    ) -> int:
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` paramaters indicate which bytes to consider

        """
        params: List[Union[int, str]] = [key]

        if start is not None and end is not None:
            params.append(start)
            params.append(end)
        elif (start is not None and end is None) or (end is not None and start is None):
            raise RedisError("Both start and end must be specified")

        return await self.execute_command("BITCOUNT", *params)

    async def bitop(self, operation: str, destkey: str, *keys: str) -> int:
        """
        Perform a bitwise operation using ``operation`` between ``keys`` and
        store the result in ``destkey``.
        """

        return await self.execute_command("BITOP", operation, destkey, *keys)

    async def bitpos(
        self,
        key: str,
        bit: int,
        *,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> int:
        """
        Return the position of the first bit set to 1 or 0 in a string.
        ``start`` and ``end`` defines the search range. The range is interpreted
        as a range of bytes and not a range of bits, so start=0 and end=2
        means to look at the first three bytes.
        """

        if bit not in (0, 1):
            raise RedisError("bit must be 0 or 1")
        params = [key, bit]

        start is not None and params.append(start)

        if start is not None and end is not None:
            params.append(end)
        elif start is None and end is not None:
            raise RedisError("start argument is not set, " "when end is specified")

        return await self.execute_command("BITPOS", *params)

    def bitfield(self, key: str) -> BitFieldOperation:
        """
        Return a :class:`BitFieldOperation` instance to conveniently construct one or
        more bitfield operations on ``key``.
        """

        return BitFieldOperation(self, key)

    @versionadded(version="2.1.0")
    def bitfield_ro(self, key: str) -> BitFieldOperation:
        """
        Return a :class:`BitFieldOperation` instance to conveniently construct bitfield
        operations on a read only replica against ``key``.

        Raises :class:`ReadOnlyError` if a write operation is attempted
        """

        return BitFieldOperation(self, key, readonly=True)

    @redis_command(
        "DECR",
        group=CommandGroup.STRING,
    )
    async def decr(self, key: str) -> int:
        """
        Decrement the integer value of a key by one

        :return: the value of ``key`` after the decrement
        """

        return await self.decrby(key, 1)

    @versionadded(version="2.1.0")
    @redis_command(
        "DECRBY",
        group=CommandGroup.STRING,
    )
    async def decrby(self, key: str, decrement: int) -> int:
        """
        Decrement the integer value of a key by the given number

        :return: the value of ``key`` after the decrement
        """

        return await self.execute_command("DECRBY", key, decrement)

    @redis_command(
        "GET",
        group=CommandGroup.STRING,
    )
    async def get(self, key: str) -> Optional[str]:
        """
        Get the value of a key

        :return: the value of ``key``, or ``None`` when ``key`` does not exist.
        """

        return await self.execute_command("GET", key)

    @versionadded(version="2.1.0")
    @redis_command(
        "GETDEL",
        group=CommandGroup.STRING,
        minimum_server_version="6.2.0",
    )
    async def getdel(self, key: str) -> Optional[str]:
        """
        Get the value of a key and delete the key


        :return: the value of ``key``, ``None`` when ``key`` does not exist,
         or an error if the key's value type isn't a string.
        """

        return await self.execute_command("GETDEL", key)

    @versionadded(version="2.1.0")
    @mutually_exclusive_parameters(
        "ex",
        "px",
        "exat",
        "pxat",
        "persist",
        details="See https://redis.io/commands/getex",
    )
    @redis_command(
        "GETEX",
        group=CommandGroup.STRING,
        minimum_server_version="6.2.0",
    )
    async def getex(
        self,
        key: str,
        *,
        ex: Optional[Union[int, datetime.timedelta]] = None,
        px: Optional[Union[int, datetime.timedelta]] = None,
        exat: Optional[int] = None,
        pxat: Optional[int] = None,
        persist: Optional[bool] = None,
    ) -> Optional[str]:
        """
        Get the value of a key and optionally set its expiration


        GETEX is similar to GET, but is a write command with
        additional options. All time parameters can be given as
        :class:`datetime.timedelta` or integers.

        :param key: name of the key
        :param ex: sets an expire flag on key ``key`` for ``ex`` seconds.
        :param px: sets an expire flag on key ``key`` for ``px`` milliseconds.
        :param exat: sets an expire flag on key ``key`` for ``ex`` seconds,
         specified in unix time.
        :param pxat: sets an expire flag on key ``key`` for ``ex`` milliseconds,
         specified in unix time.
        :param persist: remove the time to live associated with ``key``.

        :return: the value of ``key``, or ``None`` when ``key`` does not exist.
        """

        opset = {ex, px, exat, pxat}

        if len(opset) > 1 and persist:
            raise DataError(
                "``ex``, ``px``, ``exat``, ``pxat``, "
                "and ``persist`` are mutually exclusive."
            )

        pieces = []
        # similar to set command

        if ex is not None:
            pieces.append("EX")

            if isinstance(ex, datetime.timedelta):
                ex = int(ex.total_seconds())
            pieces.append(ex)

        if px is not None:
            pieces.append("PX")

            if isinstance(px, datetime.timedelta):
                px = int(px.total_seconds() * 1000)
            pieces.append(px)
        # similar to pexpireat command

        if exat is not None:
            pieces.append("EXAT")

            if isinstance(exat, datetime.datetime):
                s = int(exat.microsecond / 1000000)
                exat = int(time.mktime(exat.timetuple())) + s
            pieces.append(exat)

        if pxat is not None:
            pieces.append("PXAT")

            if isinstance(pxat, datetime.datetime):
                ms = int(pxat.microsecond / 1000)
                pxat = int(time.mktime(pxat.timetuple())) * 1000 + ms
            pieces.append(pxat)

        if persist:
            pieces.append("PERSIST")

        return await self.execute_command("GETEX", key, *pieces)

    @redis_command(
        "GETBIT",
        group=CommandGroup.STRING,
    )
    async def getbit(self, key: str, offset: int) -> int:
        """
        Returns the bit value at offset in the string value stored at key

        :return: the bit value stored at ``offset``.
        """

        return await self.execute_command("GETBIT", key, offset)

    @redis_command(
        "GETRANGE",
        group=CommandGroup.STRING,
    )
    async def getrange(self, key: str, start: int, end: int) -> str:
        """
        Get a substring of the string stored at a key

        :return: The substring of the string value stored at ``key``,
         determined by the offsets ``start`` and ``end`` (both are inclusive)
        """

        return await self.execute_command("GETRANGE", key, start, end)

    @redis_command(
        "GETRANGE",
        group=CommandGroup.STRING,
    )
    async def getset(self, key: str, value: str) -> Optional[str]:
        """
        Set the string value of a key and return its old value

        :return: the old value stored at ``key``, or ``None`` when ``key`` did not exist.
        """

        return await self.execute_command("GETSET", key, value)

    @redis_command(
        "INCR",
        group=CommandGroup.STRING,
    )
    async def incr(self, key: str) -> int:
        """
        Increment the integer value of a key by one

        :return: the value of ``key`` after the increment.
         If no key exists, the value will be initialized as 1.
        """

        return await self.incrby(key, 1)

    @redis_command(
        "INCRBY",
        group=CommandGroup.STRING,
    )
    async def incrby(self, key: str, increment: int) -> int:
        """
        Increment the integer value of a key by the given amount

        :return: the value of ``key`` after the increment
          If no key exists, the value will be initialized as ``increment``
        """

        return await self.execute_command("INCRBY", key, increment)

    @redis_command(
        "INCRBYFLOAT",
        group=CommandGroup.STRING,
    )
    async def incrbyfloat(self, key: str, increment: float) -> float:
        """
        Increment the float value of a key by the given amount

        :return: the value of ``key`` after the increment.
         If no key exists, the value will be initialized as ``increment``
        """

        return await self.execute_command("INCRBYFLOAT", key, increment)

    @redis_command(
        "MGET",
        group=CommandGroup.STRING,
    )
    async def mget(self, *keys: str) -> List[str]:
        """
        Returns a list of values ordered identically to ``keys``
        """

        return await self.execute_command("MGET", *keys)

    @redis_command(
        "MSET",
        group=CommandGroup.STRING,
    )
    async def mset(self, key_values: Dict[str, str]) -> bool:
        """
        Sets multiple keys to multiple values
        """

        return await self.execute_command("MSET", *dict_to_flat_list(key_values))

    @redis_command(
        "MSETNX",
        group=CommandGroup.STRING,
    )
    async def msetnx(self, key_values: Dict[str, str]) -> bool:
        """
        Set multiple keys to multiple values, only if none of the keys exist

        :return: Whether all the keys were set
        """

        return await self.execute_command("MSETNX", *dict_to_flat_list(key_values))

    @redis_command(
        "PSETEX",
        group=CommandGroup.STRING,
    )
    async def psetex(
        self, key: str, milliseconds: Union[int, datetime.timedelta], value: str
    ) -> None:
        """
        Set the value and expiration in milliseconds of a key
        """

        if isinstance(milliseconds, datetime.timedelta):
            ms = int(milliseconds.microseconds / 1000)
            milliseconds = (
                milliseconds.seconds + milliseconds.days * 24 * 3600
            ) * 1000 + ms

        return await self.execute_command("PSETEX", key, milliseconds, value)

    @mutually_exclusive_parameters(
        "ex",
        "px",
        "exat",
        "pxat",
        "keepttl",
        details="See: https://redis.io/commands/SET",
    )
    @redis_command(
        "SET",
        group=CommandGroup.STRING,
        arguments={
            "condition": {
                "minimum_server_version": "2.6.12",
            },
        },
    )
    async def set(
        self,
        key: str,
        value: str,
        *,
        ex: Optional[Union[int, datetime.timedelta]] = None,
        px: Optional[Union[int, datetime.timedelta]] = None,
        exat: Optional[Union[int, datetime.datetime]] = None,
        pxat: Optional[Union[int, datetime.datetime]] = None,
        keepttl: Optional[bool] = None,
        condition: Optional[Literal[PureToken.NX, PureToken.XX]] = None,
        get: Optional[bool] = None,
    ) -> Optional[Union[str, bool]]:
        """
        Set the string value of a key

        ``ex`` sets an expire flag on key ``key`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``key`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``key`` to ``value`` if it
            does not already exist.

        ``xx`` if set to True, set the value at key ``key`` to ``value`` if it
            already exists.
        """
        pieces: List[Union[int, str]] = [key, value]

        if ex is not None:
            pieces.append("EX")
            pieces.append(normalized_seconds(ex))

        if px is not None:
            pieces.append("PX")
            pieces.append(normalized_milliseconds(px))

        if exat is not None:
            pieces.append("EXAT")
            pieces.append(normalized_time_seconds(exat))

        if pxat is not None:
            pieces.append("PXAT")
            pieces.append(normalized_time_seconds(pxat))

        if get:
            pieces.append("GET")

        if condition:
            pieces.append(condition.value)

        return await self.execute_command("SET", *pieces)

    async def setbit(self, key: str, offset: int, value: int) -> int:
        """
        Flag the ``offset`` in ``key`` as ``value``. Returns a boolean
        indicating the previous value of ``offset``.

        :return: the original bit value stored at ``offset``.
        """
        value = value and 1 or 0

        return await self.execute_command("SETBIT", key, offset, value)

    async def setex(
        self, key: str, value: str, seconds: Union[int, datetime.timedelta]
    ) -> str:
        """
        Set the value of key ``key`` to ``value`` that expires in ``seconds``
        """

        return await self.execute_command(
            "SETEX", key, normalized_seconds(seconds), value
        )

    async def setnx(self, key: str, value: str) -> int:
        """
        Sets the value of key ``key`` to ``value`` if key doesn't exist
        """

        return await self.execute_command("SETNX", key, value)

    async def setrange(self, key: str, offset: int, value: str) -> int:
        """
        Overwrite bytes in the value of ``key`` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger than before.
        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        :return: the length of the string after it was modified by the command.
        """

        return await self.execute_command("SETRANGE", key, offset, value)

    async def strlen(self, key: str) -> int:
        """
        Get the length of the value stored in a key

        :return: the length of the string at ``key``, or ``0`` when ``key`` does not
        """

        return await self.execute_command("STRLEN", key)

    async def substr(self, key: str, start: int, end: int) -> str:
        """
        Get a substring of the string stored at a key

        :return: the substring of the string value stored at key, determined by the offsets
         ``start`` and ``end`` (both are inclusive). Negative offsets can be used in order to
         provide an offset starting from the end of the string.
        """

        return await self.execute_command("SUBSTR", key, start, end)


class ClusterStringsCommandMixin(StringsCommandMixin):

    NODES_FLAGS = {"BITOP": NodeFlag.BLOCKED}

    async def mget(self, keys, *args):
        """
        Returns a list of values ordered identically to ``keys``

        Cluster impl:
            Itterate all keys and send GET for each key.
            This will go alot slower than a normal mget call in StrictRedis.

            Operation is no longer atomic.
        """
        res = list()

        for arg in list_or_args(keys, args):
            res.append(await self.get(arg))

        return res

    async def mset(self, *args, **kwargs):
        """
        Sets key/values based on a mapping. Mapping can be supplied as a single
        dictionary argument or as kwargs.

        Cluster impl:
            Itterate over all items and do SET on each (k,v) pair

            Operation is no longer atomic.
        """

        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise RedisError("MSET requires **kwargs or a single dict arg")
            kwargs.update(args[0])

        for pair in iteritems(kwargs):
            await self.set(pair[0], pair[1])

        return True

    async def msetnx(self, *args, **kwargs):
        """
        Sets key/values based on a mapping if none of the keys are already set.
        Mapping can be supplied as a single dictionary argument or as kwargs.
        Returns a boolean indicating if the operation was successful.

        Clutser impl:
            Itterate over all items and do GET to determine if all keys do not exists.
            If true then call mset() on all keys.
        """

        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise RedisError("MSETNX requires **kwargs or a single dict arg")
            kwargs.update(args[0])

        # Itterate over all items and fail fast if one value is True.

        for k, _ in kwargs.items():
            if await self.get(k):
                return False

        return await self.mset(**kwargs)
