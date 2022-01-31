from typing import Optional, Union

from deprecated.sphinx import versionadded, versionchanged

from coredis.utils import b, dict_merge, first_key, string_keys_to_dict

from . import CommandMixin


def parse_sscan(response, **options):
    cursor, r = response

    return int(cursor), r


class SetsCommandMixin(CommandMixin):

    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict(
            "SADD SCARD SDIFFSTORE " "SETRANGE SINTERSTORE " "SREM SUNIONSTORE", int
        ),
        string_keys_to_dict("SISMEMBER SMOVE", bool),
        string_keys_to_dict(
            "SDIFF SINTER SMEMBERS SUNION", lambda r: r and set(r) or set()
        ),
        {"SSCAN": parse_sscan},
    )

    async def sadd(self, key: str, *members: str) -> int:
        """
        Add one or more members to a set

        :return: the number of elements that were added to the set, not including
         all the elements already present in the set.
        """

        return await self.execute_command("SADD", key, *members)

    async def scard(self, key: str) -> int:
        """
        Returns the number of members in the set

        :return the cardinality (number of elements) of the set, or ``0`` if ``key``
         does not exist.
        """

        return await self.execute_command("SCARD", key)

    async def sdiff(self, *keys: str) -> list:
        """
        Subtract multiple sets

        :return: list with members of the resulting set.
        """

        return await self.execute_command("SDIFF", *keys)

    @versionchanged(
        reason="Changed :paramref:`keys` to variable arguments", version="3.0.0"
    )
    async def sdiffstore(self, *keys: str, destination: str) -> int:
        """
        Subtract multiple sets and store the resulting set in a key

        """

        return await self.execute_command("SDIFFSTORE", destination, *keys)

    async def sinter(self, *keys: str) -> list:
        """
        Intersect multiple sets

        :return: list with members of the resulting set
        """

        return await self.execute_command("SINTER", *keys)

    @versionchanged(
        reason="Changed :paramref:`keys` to variable arguments", version="3.0.0"
    )
    async def sinterstore(self, *keys: str, destination: str) -> int:
        """
        Intersect multiple sets and store the resulting set in a key

        :return: the number of elements in the resulting set.
        """

        return await self.execute_command("SINTERSTORE", destination, *keys)

    async def sismember(self, key: str, member: str) -> bool:
        """
        Determine if a given value is a member of a set

        :return:
            * ``1`` if the element is a member of the set.
            * ``0`` if the element is not a member of the set, or if ``key`` does not exist.
        """

        return await self.execute_command("SISMEMBER", key, member)

    async def smembers(self, key: str) -> list:
        """Returns all members of the set"""

        return await self.execute_command("SMEMBERS", key)

    @versionchanged(
        reason="Changed :paramref:`members` to variable arguments", version="3.0.0"
    )
    @versionadded(version="2.1.0")
    async def smismember(self, key: str, *members: str) -> list:
        """
        Returns the membership associated with the given elements for a set

        :return: list representing the membership of the given elements, in the same
         order as they are requested.
        """

        return await self.execute_command("SMISMEMBER", key, *members)

    async def smove(self, source: str, destination: str, member: str) -> int:
        """
        Move a member from one set to another

        :return:
            * ``1`` if the element is moved.
            * ``0`` if the element is not a member of ``source`` and no operation was performed.
        """

        return await self.execute_command("SMOVE", source, destination, member)

    async def spop(
        self, key: str, count: Optional[int] = 1
    ) -> Optional[Union[str, list]]:
        """
        Remove and return one or multiple random members from a set

        :return: When called without the ``count`` argument the removed member, or ``None``
         when ``key`` does not exist.

         When called with the ``count`` argument the removed members, or an empty array when
         ``key`` does not exist.
        """

        if count and isinstance(count, int):
            return await self.execute_command("SPOP", key, count)
        else:
            return await self.execute_command("SPOP", key)

    async def srandmember(
        self, key: str, count: Optional[int] = 1
    ) -> Optional[Union[str, list]]:
        """
        Get one or multiple random members from a set



        :return: without the additional ``count`` argument, the command returns a  randomly
         selected element, or ``None`` when ``key`` does not exist.

         When the additional ``count`` argument is passed, the command returns a list of elements,
         or an empty list when ``key`` does not exist.
        """
        args = count and [count] or []

        return await self.execute_command("SRANDMEMBER", key, *args)

    async def srem(self, key: str, *members: str) -> int:
        """
        Remove one or more members from a set


        :return: the number of members that were removed from the set, not
         including non existing members.
        """

        return await self.execute_command("SREM", key, *members)

    async def sunion(self, *keys: str) -> list:
        """
        Add multiple sets

        :return: list with members of the resulting set.
        """

        return await self.execute_command("SUNION", *keys)

    @versionchanged(
        reason="Changed :paramref:`keys` to variable arguments", version="3.0.0"
    )
    async def sunionstore(self, *keys: str, destination: str) -> int:
        """
        Add multiple sets and store the resulting set in a key

        :return: the number of elements in the resulting set.

        """

        return await self.execute_command("SUNIONSTORE", destination, *keys)

    async def sscan(
        self,
        key: str,
        cursor: int = 0,
        match: Optional[str] = None,
        count: Optional[int] = None,
    ) -> None:
        """
        Incrementally returns lists of elements in a set. Also returns a
        cursor pointing to the scan position.

        :param match: is for filtering the keys by pattern
        :param count: is for hint the minimum number of returns
        """
        pieces = [key, cursor]

        if match is not None:
            pieces.extend([b("MATCH"), match])

        if count is not None:
            pieces.extend([b("COUNT"), count])

        return await self.execute_command("SSCAN", *pieces)


class ClusterSetsCommandMixin(SetsCommandMixin):

    RESULT_CALLBACKS = {"SSCAN": first_key}

    ###
    # Set commands

    async def sdiff(self, *keys: str) -> list:
        """
        Returns the difference of sets specified by ``keys``

        Cluster impl:
            Query all keys and diff all sets and return result
        """
        res = await self.smembers(keys[0])

        for arg in keys[1:]:
            res -= await self.smembers(arg)

        return res

    async def sdiffstore(self, *keys: str, destination: str) -> int:
        """
        Stores the difference of sets specified by ``keys`` into a new
        set named ``destination``.  Returns the number of keys in the new set.
        Overwrites dest key if it exists.

        Cluster impl:
            Use sdiff() --> Delete dest key --> store result in dest key
        """
        res = await self.sdiff(*keys)
        await self.delete(destination)

        if not res:
            return 0

        return await self.sadd(destination, *res)

    async def sinter(self, *keys: str) -> list:
        """
        Returns the intersection of sets specified by ``keys``

        Cluster impl:
            Query all keys, intersection and return result
        """
        res = await self.smembers(keys[0])

        for arg in keys[1:]:
            res &= await self.smembers(arg)

        return res

    async def sinterstore(self, *keys: str, destination: str) -> int:
        """
        Stores the intersection of sets specified by ``keys`` into a new
        set named ``destination``.  Returns the number of keys in the new set.

        Cluster impl:
            Use sinter() --> Delete dest key --> store result in dest key
        """
        res = await self.sinter(*keys)
        await self.delete(destination)

        if res:
            await self.sadd(destination, *res)

            return len(res)
        else:
            return 0

    async def smove(self, source: str, destination: str, member: str) -> int:
        """
        Moves ``member`` from set ``source`` to set ``destination`` atomically

        Cluster impl:
            SMEMBERS --> SREM --> SADD. Function is no longer atomic.
        """
        res = await self.srem(source, member)

        # Only add the element if existed in src set

        if res == 1:
            await self.sadd(destination, member)

        return res

    async def sunion(self, *keys: str) -> list:
        """
        Returns the union of sets specified by ``keys``

        Cluster impl:
            Query all keys, union and return result

            Operation is no longer atomic.
        """
        res = await self.smembers(keys[0])

        for arg in keys[1:]:
            res |= await self.smembers(arg)

        return res

    async def sunionstore(self, *keys: str, destination: str) -> int:
        """
        Stores the union of sets specified by ``keys`` into a new
        set named ``destination``.  Returns the number of keys in the new set.

        Cluster impl:
            Use sunion() --> Dlete dest key --> store result in dest key

            Operation is no longer atomic.
        """
        res = await self.sunion(*keys)
        await self.delete(destination)

        return await self.sadd(destination, *res)
