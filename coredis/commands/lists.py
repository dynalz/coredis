from typing import List, Literal, Optional, Union

from deprecated.sphinx import versionadded, versionchanged

from coredis.exceptions import RedisClusterException
from coredis.tokens import PureToken
from coredis.utils import b, bool_ok, dict_merge, nativestr, string_keys_to_dict
from coredis.validators import mutually_inclusive_parameters

from . import CommandMixin


class ListsCommandMixin(CommandMixin):

    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict("LPOP RPOP BLPOP BRPOP", lambda r: r and list(r) or []),
        string_keys_to_dict(
            # these return OK, or int if redis-server is >=1.3.4
            "LPUSH RPUSH",
            lambda r: isinstance(r, int) and r or nativestr(r) == "OK",
        ),
        string_keys_to_dict("LSET LTRIM", bool_ok),
        string_keys_to_dict("LINSERT LLEN LPUSHX RPUSHX", int),
    )

    @versionchanged(
        reason="""
        :paramref:`wherefrom` and :paramref:`whereto` types changed from string to
        :class:`PureToken` types
        """,
        version="3.0.0",
    )
    @versionadded(version="2.1.0")
    async def blmove(
        self,
        source: str,
        destination: str,
        wherefrom: Literal[PureToken.LEFT, PureToken.RIGHT],
        whereto: Literal[PureToken.LEFT, PureToken.RIGHT],
        timeout: float,
    ) -> Optional[str]:
        """
        Pop an element from a list, push it to another list and return it;
        or block until one is available


        :return: the element being popped from ``source`` and pushed to ``destination``
        """
        params = [source, destination, wherefrom.value, whereto.value, timeout]

        return await self.execute_command("BLMOVE", *params)

    async def blpop(self, *keys: str, timeout: float) -> List:
        """
        Remove and get the first element in a list, or block until one is available

        :return:
         - ``None`` when no element could be popped and the timeout expired.
         - A tuple with the first element being the name of the key
           where an element was popped and the second element being the value of the
           popped element.
        """

        return await self.execute_command("BLPOP", *keys, timeout)

    async def brpop(self, *keys: str, timeout: float) -> List:
        """
        Remove and get the last element in a list, or block until one is available

        :return:
         - ``None`` when no element could be popped and the timeout expired.
         - A tuple with the first element being the name of the key
           where an element was popped and the second element being the value of the
           popped element.
        """

        return await self.execute_command("BRPOP", *keys, timeout)

    async def brpoplpush(
        self, source: str, destination: str, timeout: float
    ) -> Optional[str]:
        """
        Pop an element from a list, push it to another list and return it; or block until one is available

        :return: the element being popped from ``source`` and pushed to ``destination``.
        """

        return await self.execute_command("BRPOPLPUSH", source, destination, timeout)

    async def lindex(self, key: str, index: int) -> Optional[str]:
        """

        Get an element from a list by its index

        :return: the requested element, or ``None`` when ``index`` is out of range.
        """

        return await self.execute_command("LINDEX", key, index)

    @versionchanged(
        reason="changed :paramref:`where` to  a :class:`PureToken`", version="3.0.0"
    )
    async def linsert(
        self,
        key: str,
        where: Literal[PureToken.BEFORE, PureToken.AFTER],
        pivot: str,
        element: str,
    ) -> int:
        """
        Inserts element in the list stored at key either before or after the reference value
        pivot.

        :return: the length of the list after the insert operation, or ``-1`` when
         the value pivot was not found.
        """

        return await self.execute_command("LINSERT", key, where.value, pivot, element)

    async def llen(self, key: str) -> int:
        """
        :return: the length of the list at ``key``.
        """

        return await self.execute_command("LLEN", key)

    @versionchanged(
        reason="""
        :paramref:`wherefrom` and :paramref:`whereto` types changed from string to
        :class:`PureToken` types
        """,
        version="3.0.0",
    )
    @versionadded(version="2.1.0")
    async def lmove(
        self,
        source: str,
        destination: str,
        wherefrom: Literal[PureToken.LEFT, PureToken.RIGHT],
        whereto: Literal[PureToken.LEFT, PureToken.RIGHT],
    ) -> str:
        """
        Pop an element from a list, push it to another list and return it

        :return: the element being popped and pushed.
        """
        params = [source, destination, wherefrom.value, whereto.value]

        return await self.execute_command("LMOVE", *params)

    @versionchanged(
        reason="changed return type to return a list of popped elements when count is provided",
        version="3.0.0",
    )
    async def lpop(
        self, key: str, count: Optional[int] = 1
    ) -> Optional[Union[str, list]]:
        """
        Remove and get the first ``count`` elements in a list

        :return: the value of the first element, or ``None`` when ``key`` does not exist.
         If ``count`` is provided the return is a list of popped elements,
         or ``None`` when ``key`` does not exist.
        """
        pieces = []
        if count is not None:
            pieces.append(count)
        return await self.execute_command("LPOP", key, *pieces)

    @versionchanged(
        reason="changed return type to return a list of positions when :paramref:`count` is provided",
        version="3.0.0",
    )
    @versionadded(version="2.1.0")
    async def lpos(
        self,
        key: str,
        element: str,
        rank: Optional[int] = None,
        count: Optional[int] = 1,
        maxlen: Optional[int] = None,
    ) -> Optional[Union[int, List[int]]]:
        """

        Return the index of matching elements on a list


        :return: The command returns the integer representing the matching element, or ``None``
         if there is no match.

         If the ``count`` argument is given a list of integers representing
         the matching elements.
        """
        pieces: List[Union[int, str]] = [key, element]

        if count is not None:
            pieces.extend(["COUNT", count])

        if rank is not None:
            pieces.extend(["RANK", rank])

        if maxlen is not None:
            pieces.extend(["MAXLEN", maxlen])

        return await self.execute_command("LPOS", *pieces)

    async def lpush(self, key: str, *elements: str) -> int:
        """
        Prepend one or multiple elements to a list

        :return: the length of the list after the push operations.
        """

        return await self.execute_command("LPUSH", key, *elements)

    async def lpushx(self, key: str, *elements: str) -> int:
        """
        Prepend an element to a list, only if the list exists

        :return: the length of the list after the push operation.
        """

        return await self.execute_command("LPUSHX", key, *elements)

    async def lrange(self, key: str, start: int, stop: int) -> List:
        """
        Get a range of elements from a list

        :return: list of elements in the specified range.
        """

        return await self.execute_command("LRANGE", key, start, stop)

    async def lrem(self, key: str, count: int, element: str) -> int:
        """
        Removes the first ``count`` occurrences of elements equal to ``element``
        from the list stored at ``key``.

        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.

        :return: the number of removed elements.
        """

        return await self.execute_command("LREM", key, count, element)

    async def lset(self, key: str, index: int, element: str) -> str:
        """Sets ``index`` of list ``key`` to ``element``"""

        return await self.execute_command("LSET", key, index, element)

    async def ltrim(self, key: str, start: int, stop: int) -> str:
        """
        Trims the list ``key``, removing all values not within the slice
        between ``start`` and ``stop``

        ``start`` and ``stop`` can be negative numbers just like
        Python slicing notation
        """

        return await self.execute_command("LTRIM", key, start, stop)

    @versionchanged(
        reason="changed return type to return a list of popped elements when count is provided",
        version="3.0.0",
    )
    async def rpop(
        self, key: str, count: Optional[int] = 1
    ) -> Optional[Union[str, list]]:
        """
        Remove and get the last elements in a list

        :return: When called without the ``count`` argument the value of the last element, or
         ``None`` when ``key`` does not exist.

         When called with the ``count`` argument list of popped elements, or ``None`` when
         ``key`` does not exist.
        """

        pieces = []

        if count is not None:
            pieces.extend([count])

        return await self.execute_command("RPOP", key, *pieces)

    async def rpoplpush(self, source: str, destination: str) -> str:
        """
        Remove the last element in a list, prepend it to another list and return it

        :return: the element being popped and pushed.
        """

        return await self.execute_command("RPOPLPUSH", source, destination)

    async def rpush(self, key: str, *elements: str) -> int:
        """
        Append an element(s) to a list

        :return: the length of the list after the push operation.
        """

        return await self.execute_command("RPUSH", key, *elements)

    async def rpushx(self, key: str, *elements: str) -> int:
        """
        Append a element(s) to a list, only if the list exists

        :return: the length of the list after the push operation.
        """

        return await self.execute_command("RPUSHX", key, *elements)


class ClusterListsCommandMixin(ListsCommandMixin):
    async def brpoplpush(self, source, destination, timeout=0):
        """
        Pops a value off the tail of ``source``, push it on the head of ``destination``
        and then return it.

        This command blocks until a value is in ``source`` or until ``timeout``
        seconds elapse, whichever is first. A ``timeout`` value of 0 blocks
        forever.

        Cluster impl:
            Call brpop() then send the result into lpush()

            Operation is no longer atomic.
        """
        try:
            values = await self.brpop(source, timeout=timeout)

            if not values:
                return None
        except TimeoutError:
            return None
        await self.lpush(destination, values[1])

        return values[1]

    async def rpoplpush(self, source, destination):
        """
        RPOP a value off of the ``source`` list and atomically LPUSH it
        on to the ``destination`` list.  Returns the value.

        Cluster impl:
            Call rpop() then send the result into lpush()

            Operation is no longer atomic.
        """
        values = await self.rpop(source)

        if values:
            await self.lpush(destination, values[0])

            return values[0]

        return []

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

        try:
            data_type = b(await self.type(key))

            if data_type == b("none"):
                return []
            elif data_type == b("set"):
                data = list(await self.smembers(key))[:]
            elif data_type == b("list"):
                data = await self.lrange(key, 0, -1)
            else:
                raise RedisClusterException(
                    "Unable to sort data type : {0}".format(data_type)
                )

            if by is not None:
                # _sort_using_by_arg mutates data so we don't
                # need need a return value.
                data = await self._sort_using_by_arg(data, by, alpha)
            elif not alpha:
                data.sort(key=self._strtod_key_func)
            else:
                data.sort()

            if order == PureToken.DESC:
                data = data[::-1]

            if not (offset is None and count is None):
                data = data[offset : offset + count]

            if gets:
                data = await self._retrive_data_from_sort(data, gets)

            if store is not None:
                if data_type == b("set"):
                    await self.delete(store)
                    await self.rpush(store, *data)
                elif data_type == b("list"):
                    await self.delete(store)
                    await self.rpush(store, *data)
                else:
                    raise RedisClusterException(
                        "Unable to store sorted data for data type : {0}".format(
                            data_type
                        )
                    )

                return len(data)

            return data
        except KeyError:
            return []

    async def _retrive_data_from_sort(self, data, gets):
        """
        Used by sort()
        """

        if gets:
            new_data = []

            for k in data:
                for g in gets:
                    single_item = await self._get_single_item(k, g)
                    new_data.append(single_item)
            data = new_data

        return data

    async def _get_single_item(self, k, g):
        """
        Used by sort()
        """

        if getattr(k, "decode", None):
            k = k.decode("utf-8")

        if "*" in g:
            g = g.replace("*", k)

            if "->" in g:
                key, hash_key = g.split("->")
                single_item = await self.get(key, {}).get(hash_key)
            else:
                single_item = await self.get(g)
        elif "#" in g:
            single_item = k
        else:
            single_item = None

        return b(single_item)

    def _strtod_key_func(self, arg):
        """
        Used by sort()
        """

        return float(arg)

    async def _sort_using_by_arg(self, data, by, alpha):
        """
        Used by sort()
        """

        if getattr(by, "decode", None):
            by = by.decode("utf-8")

        async def _by_key(arg):
            if getattr(arg, "decode", None):
                arg = arg.decode("utf-8")

            key = by.replace("*", arg)

            if "->" in by:
                key, hash_key = key.split("->")
                v = await self.hget(key, hash_key)

                if alpha:
                    return v
                else:
                    return float(v)
            else:
                return await self.get(key)

        sorted_data = []

        for d in data:
            sorted_data.append((d, await _by_key(d)))

        return [x[0] for x in sorted(sorted_data, key=lambda x: x[1])]
