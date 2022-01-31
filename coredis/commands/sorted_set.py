from typing import Dict, List, Literal, Optional, Tuple, Union

from deprecated.sphinx import deprecated, versionadded, versionchanged

from coredis.exceptions import DataError, RedisError
from coredis.tokens import PureToken
from coredis.utils import (
    b,
    dict_merge,
    dict_to_flat_list,
    first_key,
    int_or_none,
    iteritems,
    string_keys_to_dict,
)

from . import CommandMixin

VALID_ZADD_OPTIONS = {"NX", "XX", "CH", "INCR"}


def float_or_none(response):
    if response is not None:
        return float(response)


def zset_score_pairs(response, **options):
    """
    If ``withscores`` is specified in the options, return the response as
    a list of (value, score) pairs
    """

    if not response or not options.get("withscores"):
        return response

    it = iter(response)

    return list(zip(it, map(float, it)))


def parse_zmscore(response, **options):
    return [float(score) if score is not None else None for score in response]


def parse_zscan(response, **options):
    score_cast_func = options.get("score_cast_func", float)
    cursor, r = response
    it = iter(r)

    return int(cursor), list(zip(it, map(score_cast_func, it)))


class SortedSetCommandMixin(CommandMixin):

    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict(
            "ZADD ZCARD ZLEXCOUNT "
            "ZREM ZREMRANGEBYLEX "
            "ZREMRANGEBYRANK "
            "ZREMRANGEBYSCORE",
            int,
        ),
        string_keys_to_dict("ZSCORE ZINCRBY", float_or_none),
        string_keys_to_dict(
            "ZPOPMAX ZPOPMIN ZINTER ZDIFF ZUNION ZRANGE ZRANGEBYSCORE "
            "ZREVRANGE ZREVRANGEBYSCORE",
            zset_score_pairs,
        ),
        string_keys_to_dict("ZRANK ZREVRANK", int_or_none),
        string_keys_to_dict(
            "BZPOPMIN BZPOPMAX", lambda r: r and (r[0], r[1], float(r[2])) or None
        ),
        {"ZSCAN": parse_zscan},
        {"ZMSCORE": parse_zmscore},
    )

    # Redis command mapping

    @versionchanged(
        reason="Changed :paramref:`keys` to variable arguments", version="3.1.0"
    )
    @versionadded(version="2.1.0")
    async def bzpopmax(
        self, *keys: str, timeout: float
    ) -> List[Tuple[str, str, float]]:
        """
        Remove and return the member with the highest score from one or more sorted sets,
        or block until one is available.

        :return: A triplet with the first element being the name of the key
         where a member was popped, the second element is the popped member itself,
         and the third element is the score of the popped element.
        """

        if timeout is None:
            timeout = 0

        return await self.execute_command("BZPOPMAX", *keys, timeout)

    @versionchanged(
        reason="Changed :paramref:`keys` to variable arguments", version="3.1.0"
    )
    @versionadded(version="2.1.0")
    async def bzpopmin(
        self, *keys: str, timeout: float
    ) -> List[Tuple[str, str, float]]:
        """
        Remove and return the member with the lowest score from one or more sorted sets,
        or block until one is available

        :return: A triplet with the first element being the name of the key
         where a member was popped, the second element is the popped member itself,
         and the third element is the score of the popped element.
        """

        if timeout is None:
            timeout = 0

        return await self.execute_command("BZPOPMIN", *keys, timeout)

    @versionchanged(
        reason="""
        score/member pairs are now only accepted via the :paramref:`member_scores` argument
        """,
        version="3.0.0",
    )
    async def zadd(
        self,
        key: str,
        member_scores: Dict[str, float],
        condition: Optional[Literal[PureToken.NX, PureToken.XX]] = None,
        comparison: Optional[Literal[PureToken.GT, PureToken.LT]] = None,
        change: Optional[bool] = None,
        increment: Optional[bool] = None,
    ) -> Optional[Union[str, int]]:
        """
        Add one or more members to a sorted set, or update its score if it already exists

        :param member_scores:
        :param condition:
        :param comparison:
        :param change:
        :param increment:


        :return:
         - When used without optional arguments, the number of elements added to the sorted set
           (excluding score updates).
         - If the ``change`` option is specified, the number of elements that were changed
           (added or updated).
         - If the ``condition``argument is specified, the new score of ``member``
           (a double precision floating point number) represented as string
         - ``None`` if the operation is aborted

        """
        pieces: List[Union[str, int, float]] = []

        if change is not None:
            pieces.append("CH")

        if increment is not None:
            pieces.append("INCR")

        if condition:
            pieces.append(condition.value)

        if comparison:
            pieces.append(comparison.value)

        pieces.extend(dict_to_flat_list(member_scores, reverse=True))

        return await self.execute_command("ZADD", key, *pieces)

    async def zcard(self, key: str) -> int:
        """
        Get the number of members in a sorted set

        :return: the cardinality (number of elements) of the sorted set, or ``0``
         if the ``key`` does not exist

        """

        return await self.execute_command("ZCARD", key)

    async def zcount(self, key: str, min: float, max: float) -> int:
        """
        Count the members in a sorted set with scores within the given values

        :return: the number of elements in the specified score range.
        """

        return await self.execute_command("ZCOUNT", key, min, max)

    @versionchanged(
        reason="Changed :paramref:`keys` to variable arguments", version="3.1.0"
    )
    @versionadded(version="2.1.0")
    async def zdiff(self, *keys: str, withscores: Optional[bool] = None) -> List:
        """
        Subtract multiple sorted sets

        :param withscores:

        :return: the result of the difference (optionally with their scores, in case
         the ``withscores`` option is given).
        """
        pieces = [len(keys), *keys]

        if withscores:
            pieces.append("WITHSCORES")

        return await self.execute_command("ZDIFF", *pieces)

    @versionchanged(
        reason="Restructured arguments to keep :paramref:`keys` as var args for consistency",
        version="3.0.0",
    )
    @versionadded(version="2.1.0")
    async def zdiffstore(self, *keys: str, destination: str) -> int:
        """
        Subtract multiple sorted sets and store the resulting sorted set in a new key

        :return: the number of elements in the resulting sorted set at ``destination``.
        """
        pieces = [len(keys), *keys]

        return await self.execute_command("ZDIFFSTORE", destination, *pieces)

    @versionchanged(
        reason="Reordered and renamed arguments to be consistent with redis documentation",
        version="3.0.0",
    )
    async def zincrby(self, key: str, increment: int, member: str) -> str:
        """
        Increment the score of a member in a sorted set

        :return: the new score of ``member`` (a double precision floating point number),
         represented as string.
        """

        return await self.execute_command("ZINCRBY", key, increment, member)

    @versionchanged(
        reason="Separated keys and weights into two separate arguments for clarity",
        version="3.0.0",
    )
    @versionadded(version="2.1.0")
    async def zinter(
        self,
        *keys: str,
        weights: Optional[List[Optional[int]]] = None,
        aggregate: Optional[
            Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX]
        ] = None,
        withscores: Optional[bool] = None
    ) -> List:
        """

        Intersect multiple sorted sets

        :param keys:
        :param weights:
        :param aggregate:
        :param withscores:

        :return: the result of intersection (optionally with their scores, in case
         the ``withscores`` option is given).

        """

        return await self._zaggregate(
            "ZINTER", None, keys, weights, aggregate, withscores=withscores
        )

    @versionchanged(
        reason="Separated keys and weights into two separate arguments for clarity",
        version="3.0.0",
    )
    @versionadded(version="2.1.0")
    async def zinterstore(
        self,
        *keys: str,
        destination: str,
        weights: Optional[List[Optional[int]]] = None,
        aggregate: Optional[Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX]] = None
    ) -> int:
        """
        Intersect multiple sorted sets and store the resulting sorted set in a new key

        :return: the number of elements in the resulting sorted set at ``destination``.
        """

        return await self._zaggregate(
            "ZINTERSTORE", destination, keys, weights, aggregate
        )

    async def zlexcount(self, key: str, min: str, max: str) -> int:
        """
        Count the number of members in a sorted set between a given lexicographical range

        :return: the number of elements in the specified score range.
        """

        return await self.execute_command("ZLEXCOUNT", key, min, max)

    @versionchanged(
        reason="""
        :paramref:`members` changed to variable argument list
        """,
        version="3.0.0",
    )
    @versionadded(version="2.1.0")
    async def zmscore(self, key: str, *members: str) -> Optional[list]:
        """
        Get the score associated with the given members in a sorted set

        :param members:

        :return: list of scores or ``None`` associated with the specified ``members``
         values (a double precision floating point number), represented as strings

        """
        if not members:
            raise DataError("ZMSCORE members must be a non-empty list")

        return await self.execute_command("ZMSCORE", key, *members)

    @versionadded(version="2.1.0")
    async def zpopmax(self, key: str, count: Optional[int] = 1) -> List:
        """
        Remove and return members with the highest scores in a sorted set

        :return: list of popped elements and scores.
        """
        args = (count is not None) and [count] or []
        options = {"withscores": True}
        return await self.execute_command("ZPOPMAX", key, *args, **options)

    @versionadded(version="2.1.0")
    async def zpopmin(self, key: str, count: Optional[int] = 1) -> List:
        """
        Remove and return members with the lowest scores in a sorted set

        :return: list of popped elements and scores.
        """
        args = (count is not None) and [count] or []
        options = {"withscores": True}

        return await self.execute_command("ZPOPMIN", key, *args, **options)

    @versionadded(version="2.1.0")
    async def zrandmember(
        self, key: str, count: Optional[int] = None, withscores: Optional[bool] = None
    ) -> Optional[Union[str, list]]:
        """
        Get one or multiple random elements from a sorted set


        :return: without the additional ``count`` argument, the command returns a
         randomly selected element, or ``None`` when ``key`` does not exist.

         If the additional ``count`` argument is passed,
         the command returns a list of elements, or an empty list when ``key`` does not exist.

         If the ``withscores`` argument is used, the return is a list elements and their scores
         from the sorted set.
        """
        params = []

        if count is not None:
            params.append(count)

        if withscores:
            params.append("WITHSCORES")

        return await self.execute_command("ZRANDMEMBER", key, *params)

    @versionchanged(
        reason="""
    - Removed ``byscore`` and ``bylex`` and added :paramref:`sortby` parameter to collapse
      different options
    - Changed ``desc`` to :paramref:`rev`
    - Changed ``num`` to :paramref:`count`
    """,
        version="3.0.0",
    )
    async def zrange(
        self,
        key: str,
        start: str,
        stop: str,
        sortby: Optional[Literal[PureToken.BYSCORE, PureToken.BYLEX]] = None,
        rev: Optional[bool] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
        withscores: Optional[bool] = None,
    ) -> List:
        """

        Return a range of members in a sorted set

        :return: list of elements in the specified range (optionally with their scores, in case
         the ``withscores`` argument is given).
        """
        # if not byscore and not bylex and (offset is None and num is None) and desc:
        #    return self.zrevrange(key, start, end, withscores, score_cast_func)

        return await self._zrange(
            "ZRANGE",
            None,
            key,
            start,
            stop,
            rev,
            sortby,
            withscores,
            offset,
            count,
        )

    @versionchanged(
        reason="""
        - Changed ``num` to :paramref:`count`
        - Added :paramref:`offset`
        """,
        version="3.0.0",
    )
    async def zrangebylex(
        self,
        key: str,
        min: str,
        max: str,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> List:
        """

        Return a range of members in a sorted set, by lexicographical range

        :return: list of elements in the specified score range.
        """

        if (offset is not None and count is None) or (
            count is not None and offset is None
        ):
            raise RedisError("``offset`` and ``count`` must both be specified")
        pieces = ["ZRANGEBYLEX", key, min, max]

        if offset is not None and count is not None:
            pieces.extend([b("LIMIT"), offset, count])

        return await self.execute_command(*pieces)

    @versionchanged(
        reason="""
        - Removed ``score_cast_func``
        - Changed ``num` to :paramref:`count`
        - Changed ``start` to :paramref:`offset`
        """,
        version="3.0.0",
    )
    async def zrangebyscore(
        self,
        key: str,
        min: float,
        max: float,
        withscores: Optional[bool] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> List:
        """

        Return a range of members in a sorted set, by score

        :return: list of elements in the specified score range (optionally with their scores).
        """

        if (offset is not None and count is None) or (
            count is not None and offset is None
        ):
            raise RedisError("``offset`` and ``count`` must both be specified")
        pieces = ["ZRANGEBYSCORE", key, min, max]

        if offset is not None and count is not None:
            pieces.extend([b("LIMIT"), offset, count])

        if withscores:
            pieces.append(b("WITHSCORES"))
        options = {"withscores": withscores}

        return await self.execute_command(*pieces, **options)

    @versionadded(version="2.1.0")
    async def zrangestore(
        self,
        dst: str,
        src: str,
        min: str,
        max: str,
        sortby: Optional[Literal[PureToken.BYSCORE, PureToken.BYLEX]] = None,
        rev: Optional[bool] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> int:
        """
        Store a range of members from sorted set into another key

        :return: the number of elements in the resulting sorted set
        """

        return await self._zrange(
            "ZRANGESTORE",
            dst,
            src,
            min,
            max,
            rev,
            sortby,
            False,
            offset,
            count,
        )

    async def zrank(self, key: str, member: str) -> Optional[int]:
        """
        Determine the index of a member in a sorted set

        :return: the rank of ``member``
        """

        return await self.execute_command("ZRANK", key, member)

    async def zrem(self, key: str, *members: str) -> int:
        """
        Remove one or more members from a sorted set

        :return: The number of members removed from the sorted set, not including non existing
         members.
        """

        return await self.execute_command("ZREM", key, *members)

    async def zremrangebylex(self, key: str, min: str, max: str) -> int:
        """
        Remove all members in a sorted set between the given lexicographical range

        :return: the number of elements removed.
        """

        return await self.execute_command("ZREMRANGEBYLEX", key, min, max)

    async def zremrangebyrank(self, key: str, start: int, stop: int) -> int:
        """
        Remove all members in a sorted set within the given indexes

        :return: the number of elements removed.
        """

        return await self.execute_command("ZREMRANGEBYRANK", key, start, stop)

    async def zremrangebyscore(self, key: str, min: float, max: float) -> int:
        """
        Remove all members in a sorted set within the given scores

        :return: the number of elements removed.
        """

        return await self.execute_command("ZREMRANGEBYSCORE", key, min, max)

    @versionchanged(
        reason="""
        - Removed ``score_cast_func``
        - Renamed ``end`` to :paramref:``stop``
        """,
        version="3.0.0",
    )
    async def zrevrange(
        self, key: str, start: int, stop: int, withscores: Optional[bool] = None
    ) -> List:
        """

        Return a range of members in a sorted set, by index, with scores ordered from
        high to low

        :return: list of elements in the specified range (optionally with their scores).
        """
        pieces = ["ZREVRANGE", key, start, stop]

        if withscores:
            pieces.append(b("WITHSCORES"))
        options = {"withscores": withscores}

        return await self.execute_command(*pieces, **options)

    @versionchanged(
        reason="""
        - Renamed ``start`` to :paramref:``offset``
        - Renamed ``num`` to :paramref:``count``
        """,
        version="3.0.0",
    )
    async def zrevrangebylex(
        self,
        key: str,
        max: str,
        min: str,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> List:
        """

        Return a range of members in a sorted set, by lexicographical range, ordered from
        higher to lower strings.

        :return: list of elements in the specified score range
        """

        if (offset is not None and count is None) or (
            count is not None and offset is None
        ):
            raise RedisError("``offset`` and ``count`` must both be specified")
        pieces = ["ZREVRANGEBYLEX", key, max, min]

        if offset is not None and count is not None:
            pieces.extend([b("LIMIT"), offset, count])

        return await self.execute_command(*pieces)

    @versionchanged(
        reason="""
        - Renamed ``start`` to :paramref:``offset``
        - Renamed ``num`` to :paramref:``count``
        - Removed ``score_cast_func``
        """,
        version="3.0.0",
    )
    async def zrevrangebyscore(
        self,
        key: str,
        max: float,
        min: float,
        withscores: Optional[bool] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> List:
        """

        Return a range of members in a sorted set, by score, with scores ordered from high to low

        :return: list of elements in the specified score range (optionally with their scores)
        """

        if (offset is not None and count is None) or (
            count is not None and offset is None
        ):
            raise RedisError("``offset`` and ``count`` must both be specified")
        pieces = ["ZREVRANGEBYSCORE", key, max, min]

        if offset is not None and count is not None:
            pieces.extend([b("LIMIT"), offset, count])

        if withscores:
            pieces.append(b("WITHSCORES"))
        options = {"withscores": withscores}

        return await self.execute_command(*pieces, **options)

    async def zrevrank(self, key: str, member: str) -> Optional[Union[str, int]]:
        """
        Determine the index of a member in a sorted set, with scores ordered from high to low

        :return: the rank of ``member``
        """

        return await self.execute_command("ZREVRANK", key, member)

    @versionchanged(
        reason="""
        - Removed ``score_cast_func``
        """,
        version="3.0.0",
    )
    async def zscan(
        self,
        key: str,
        cursor: int = 1,
        match: Optional[str] = None,
        count: Optional[int] = None,
    ):
        """
        Incrementally iterate sorted sets elements and associated scores

        """
        pieces = [key, cursor]

        if match is not None:
            pieces.extend([b("MATCH"), match])

        if count is not None:
            pieces.extend([b("COUNT"), count])
        return await self.execute_command("ZSCAN", *pieces)

    async def zscore(self, key: str, member: str) -> str:
        """
        Get the score associated with the given member in a sorted set

        :return: the score of ``member`` (a double precision floating point number),
         represented as string.
        """

        return await self.execute_command("ZSCORE", key, member)

    @versionchanged(
        reason="""
        - Separated keys and weights into two separate arguments for clarity,
        - Changed :paramref:`aggregate` to :class:`PureToken`
        """,
        version="3.0.0",
    )
    @versionadded(version="2.1.0")
    async def zunion(
        self,
        *keys: str,
        weights: Optional[List[Optional[int]]] = None,
        aggregate: Optional[
            Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX]
        ] = None,
        withscores: Optional[bool] = None
    ) -> List:
        """

        Add multiple sorted sets

        :return: the result of union (optionally with their scores, in case the ``withscores``
         argument is given).
        """

        return await self._zaggregate(
            "ZUNION", None, keys, weights, aggregate, withscores=withscores
        )

    @versionchanged(
        reason="""
        - Separated keys and weights into two separate arguments for clarity,
        - Changed :paramref:`aggregate` to :class:`PureToken`
        - Renamed ``dest`` to :paramref:`destination`
        """,
        version="3.0.0",
    )
    async def zunionstore(
        self,
        *keys: str,
        destination: str,
        weights: Optional[List[Optional[int]]] = None,
        aggregate: Optional[Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX]] = None
    ) -> int:
        """
        Add multiple sorted sets and store the resulting sorted set in a new key

        :return: the number of elements in the resulting sorted set at ``destination``.
        """

        return await self._zaggregate(
            "ZUNIONSTORE", destination, keys, weights, aggregate
        )

    @deprecated(
        reason="Use :meth:`zadd` with the appropriate options instead", version="3.0.0"
    )
    async def zaddoption(self, key, option=None, *args, **kwargs):
        """
        Differs from zadd in that you can set either 'XX' or 'NX' option as
        described here: https://redis.io/commands/zadd. Only for Redis 3.0.2 or
        later.

        The following example would add four values to the 'my-key' key:
        redis.zaddoption('my-key', 'XX', 1.1, 'name1', 2.2, 'name2', name3=3.3, name4=4.4)
        redis.zaddoption('my-key', 'NX CH', name1=2.2)

        """

        if not option:
            raise RedisError("ZADDOPTION must take options")
        options = set(opt.upper() for opt in option.split())

        if options - VALID_ZADD_OPTIONS:
            raise RedisError("ZADD only takes XX, NX, CH, or INCR")

        if "NX" in options and "XX" in options:
            raise RedisError("ZADD only takes one of XX or NX")
        pieces = list(options)
        members = []

        if args:
            if len(args) % 2 != 0:
                raise RedisError(
                    "ZADD requires an equal number of " "values and scores"
                )
            members.extend(args)

        for pair in iteritems(kwargs):
            members.append(pair[1])
            members.append(pair[0])

        if "INCR" in options and len(members) != 2:
            raise RedisError("ZADD with INCR only takes one score-name pair")

        return await self.execute_command("ZADD", key, *pieces, *members)

    # Private methods
    async def _zrange(
        self,
        command,
        dest,
        key,
        start,
        stop,
        rev=None,
        sortby: PureToken = None,
        withscores=False,
        offset=None,
        count=None,
    ):
        if (offset is not None and count is None) or (
            count is not None and offset is None
        ):
            raise DataError("``offset`` and ``count`` must both be specified.")

        if sortby == PureToken.BYLEX and withscores:
            raise DataError(
                "``withscores`` not supported in combination " "with ``bylex``."
            )
        pieces = [command]

        if dest:
            pieces.append(dest)
        pieces.extend([key, start, stop])

        if sortby:
            pieces.append(sortby.value)

        if rev is not None:
            pieces.append("REV")

        if offset is not None and count is not None:
            pieces.extend(["LIMIT", offset, count])

        if withscores:
            pieces.append("WITHSCORES")
        options = {"withscores": withscores}

        return await self.execute_command(*pieces, **options)

    async def _zaggregate(
        self,
        command,
        destination: str,
        keys: List[str],
        weights: List[int],
        aggregate: PureToken = None,
        withscores: bool = None,
    ):
        pieces = [command]
        if destination:
            pieces.append(destination)
        pieces.append(len(keys))
        pieces.extend(keys)
        options = {}
        if weights:
            pieces.append(b("WEIGHTS"))
            pieces.extend(weights)

        if aggregate:
            pieces.append(b("AGGREGATE"))
            pieces.append(aggregate.value)

        if withscores is not None:
            pieces.append(b("WITHSCORES"))
            options = {"withscores": True}
        return await self.execute_command(*pieces, **options)


class ClusterSortedSetCommandMixin(SortedSetCommandMixin):

    RESULT_CALLBACKS = {"ZSCAN": first_key}
