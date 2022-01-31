import pytest

from coredis import DataError, PureToken
from coredis.utils import b
from tests.conftest import targets


@targets("redis_basic", "redis_cluster")
@pytest.mark.asyncio()
class TestSortedSet:
    async def test_zadd(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zrange("a{foo}", 0, -1) == [b("a1"), b("a2"), b("a3")]

    async def test_zadd_options(self, client):
        await client.zadd("a{foo}", dict(a1=1))
        assert int(await client.zscore("a{foo}", "a1")) == 1
        assert int(await client.zadd("a{foo}", dict(a1=2), condition=PureToken.NX)) == 0
        assert (
            int(
                await client.zadd(
                    "a{foo}", dict(a1=2), condition=PureToken.NX, change=True
                )
            )
            == 0
        )
        assert int(await client.zscore("a{foo}", "a1")) == 1
        assert await client.zcard("a{foo}") == 1
        assert int(await client.zadd("a{foo}", dict(a2=1), condition=PureToken.XX)) == 0
        assert await client.zcard("a{foo}") == 1
        assert int(await client.zadd("a{foo}", dict(a1=2), condition=PureToken.XX)) == 0
        assert (
            int(
                await client.zadd(
                    "a{foo}", dict(a1=3), condition=PureToken.XX, change=True
                )
            )
            == 1
        )
        assert int(await client.zscore("a{foo}", "a1")) == 3
        assert int(await client.zadd("a{foo}", dict(a2=1), condition=PureToken.NX)) == 1
        assert (
            int(
                await client.zadd(
                    "a{foo}", dict(a3=1), condition=PureToken.NX, change=True
                )
            )
            == 1
        )
        assert await client.zcard("a{foo}") == 3
        await client.zadd("a{foo}", dict(a3=1), increment=True)
        assert int(await client.zscore("a{foo}", "a3")) == 2
        assert (
            int(
                await client.zadd(
                    "a{foo}", dict(a3=1), comparison=PureToken.LT, change=True
                )
            )
            == 1
        )
        assert (
            int(
                await client.zadd(
                    "a{foo}", dict(a3=2), comparison=PureToken.LT, change=True
                )
            )
            == 0
        )
        assert (
            int(
                await client.zadd(
                    "a{foo}", dict(a3=2), comparison=PureToken.GT, change=True
                )
            )
            == 1
        )
        assert (
            int(
                await client.zadd(
                    "a{foo}", dict(a3=1), comparison=PureToken.GT, change=True
                )
            )
            == 0
        )

    async def test_zcard(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zcard("a{foo}") == 3

    async def test_zcount(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zcount("a{foo}", "-inf", "+inf") == 3
        assert await client.zcount("a{foo}", 1, 2) == 2
        assert await client.zcount("a{foo}", 10, 20) == 0

    @pytest.mark.min_server_version("6.2.0")
    async def test_zdiff(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        await client.zadd("b{foo}", dict(a1=1, a2=2))
        assert (await client.zdiff("a{foo}", "b{foo}")) == [b"a3"]
        assert (await client.zdiff("a{foo}", "b{foo}", withscores=True)) == [
            b"a3",
            b"3",
        ]

    @pytest.mark.min_server_version("6.2.0")
    async def test_zdiffstore(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        await client.zadd("b{foo}", dict(a1=1, a2=2))
        assert await client.zdiffstore("a{foo}", "b{foo}", destination="out{foo}")
        assert (await client.zrange("out{foo}", 0, -1)) == [b"a3"]
        assert (await client.zrange("out{foo}", 0, -1, withscores=True)) == [
            (b"a3", 3.0)
        ]

    async def test_zincrby(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zincrby("a{foo}", member="a2", increment=1) == 3.0
        assert await client.zincrby("a{foo}", member="a3", increment=5) == 8.0
        assert await client.zscore("a{foo}", "a2") == 3.0
        assert await client.zscore("a{foo}", "a3") == 8.0

    async def test_zlexcount(self, client):
        await client.zadd("a{foo}", dict(a=0, b=0, c=0, d=0, e=0, f=0, g=0))
        assert await client.zlexcount("a{foo}", "-", "+") == 7
        assert await client.zlexcount("a{foo}", "[b", "[f") == 5

    async def test_zinter_sum(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert await client.zinter("a{foo}", "b{foo}", "c{foo}") == [b"a3", b"a1"]
        assert await client.zinter("a{foo}", "b{foo}", "c{foo}", withscores=True) == [
            (b"a3", 8),
            (b"a1", 9),
        ]

    async def test_zinter_max(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zinter(
                "a{foo}",
                "b{foo}",
                "c{foo}",
                aggregate=PureToken.MAX,
            )
            == [b"a3", b"a1"]
        )
        assert await client.zinter(
            "a{foo}", "b{foo}", "c{foo}", aggregate=PureToken.MAX, withscores=True
        ) == [(b"a3", 5), (b"a1", 6)]

    async def test_zinterstore_sum(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zinterstore("a{foo}", "b{foo}", "c{foo}", destination="d{foo}")
            == 2
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == [
            (b("a3"), 8),
            (b("a1"), 9),
        ]

    async def test_zinterstore_max(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zinterstore(
                "a{foo}",
                "b{foo}",
                "c{foo}",
                destination="d{foo}",
                aggregate=PureToken.MAX,
            )
            == 2
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == [
            (b("a3"), 5),
            (b("a1"), 6),
        ]

    async def test_zinterstore_min(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        await client.zadd("b{foo}", dict(a1=2, a2=3, a3=5))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zinterstore(
                "a{foo}",
                "b{foo}",
                "c{foo}",
                destination="d{foo}",
                aggregate=PureToken.MIN,
            )
            == 2
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == [
            (b("a1"), 1),
            (b("a3"), 3),
        ]

    async def test_zinterstore_with_weight(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zinterstore(
                "a{foo}", "b{foo}", "c{foo}", destination="d{foo}", weights=[1, 2, 3]
            )
            == 2
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == [
            (b("a3"), 20),
            (b("a1"), 23),
        ]

    @pytest.mark.min_server_version("4.9.0")
    async def test_zpopmax(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert (await client.zpopmax("a{foo}")) == [(b"a3", 3)]
        # with count
        assert (await client.zpopmax("a{foo}", count=2)) == [(b"a2", 2), (b"a1", 1)]

    @pytest.mark.min_server_version("4.9.0")
    async def test_zpopmin(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert (await client.zpopmin("a{foo}")) == [(b"a1", 1)]
        # with count
        assert (await client.zpopmin("a{foo}", count=2)) == [(b"a2", 2), (b"a3", 3)]

    @pytest.mark.min_server_version("6.2.0")
    async def test_zrandemember(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3, a4=4, a5=5))
        assert (await client.zrandmember("a{foo}")) is not None
        assert len(await client.zrandmember("a{foo}", 2)) == 2
        # with scores
        assert len(await client.zrandmember("a{foo}", 2, True)) == 4
        # without duplications
        assert len(await client.zrandmember("a{foo}", 10)) == 5
        # with duplications
        assert len(await client.zrandmember("a{foo}", -10)) == 10

    @pytest.mark.min_server_version("4.9.0")
    async def test_bzpopmax(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2))
        await client.zadd("b{foo}", dict(b1=10, b2=20))
        assert (await client.bzpopmax("b{foo}", "a{foo}", timeout=1)) == (
            b"b{foo}",
            b"b2",
            20,
        )
        assert (await client.bzpopmax("b{foo}", "a{foo}", timeout=1)) == (
            b"b{foo}",
            b"b1",
            10,
        )
        assert (await client.bzpopmax("b{foo}", "a{foo}", timeout=1)) == (
            b"a{foo}",
            b"a2",
            2,
        )
        assert (await client.bzpopmax("b{foo}", "a{foo}", timeout=1)) == (
            b"a{foo}",
            b"a1",
            1,
        )
        assert (await client.bzpopmax("b{foo}", "a{foo}", timeout=1)) is None
        await client.zadd("c{foo}", dict(c1=100))
        assert (await client.bzpopmax("c{foo}", timeout=1)) == (b"c{foo}", b"c1", 100)

    @pytest.mark.min_server_version("4.9.0")
    async def test_bzpopmin(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2))
        await client.zadd("b{foo}", dict(b1=10, b2=20))
        assert (await client.bzpopmin("b{foo}", "a{foo}", timeout=1)) == (
            b"b{foo}",
            b"b1",
            10,
        )
        assert (await client.bzpopmin("b{foo}", "a{foo}", timeout=1)) == (
            b"b{foo}",
            b"b2",
            20,
        )
        assert (await client.bzpopmin("b{foo}", "a{foo}", timeout=1)) == (
            b"a{foo}",
            b"a1",
            1,
        )
        assert (await client.bzpopmin("b{foo}", "a{foo}", timeout=1)) == (
            b"a{foo}",
            b"a2",
            2,
        )
        assert (await client.bzpopmin("b{foo}", "a{foo}", timeout=1)) is None
        await client.zadd("c{foo}", dict(c1=100))
        assert (await client.bzpopmin("c{foo}", timeout=1)) == (b"c{foo}", b"c1", 100)

    async def test_zrange(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zrange("a{foo}", 0, 1) == [b("a1"), b("a2")]
        assert await client.zrange("a{foo}", 1, 2) == [b("a2"), b("a3")]

        # withscores
        assert await client.zrange("a{foo}", 0, 1, withscores=True) == [
            (b("a1"), 1.0),
            (b("a2"), 2.0),
        ]
        assert await client.zrange("a{foo}", 1, 2, withscores=True) == [
            (b("a2"), 2.0),
            (b("a3"), 3.0),
        ]

        # custom score function
        assert await client.zrange("a{foo}", 0, 1, withscores=True,) == [
            (b("a1"), 1),
            (b("a2"), 2),
        ]

    @pytest.mark.min_server_version("6.2.0")
    async def test_zrangestore(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zrangestore("b{foo}", "a{foo}", 0, 1)
        assert await client.zrange("b{foo}", 0, -1) == [b"a1", b"a2"]
        assert await client.zrangestore("b{foo}", "a{foo}", 1, 2)
        assert await client.zrange("b{foo}", 0, -1) == [b"a2", b"a3"]
        assert await client.zrange("b{foo}", 0, -1, withscores=True) == [
            (b"a2", 2),
            (b"a3", 3),
        ]
        # reversed order
        assert await client.zrangestore("b{foo}", "a{foo}", 1, 2, rev=True)
        assert await client.zrange("b{foo}", 0, -1) == [b"a1", b"a2"]
        # by score
        assert await client.zrangestore(
            "b{foo}",
            "a{foo}",
            2,
            1,
            sortby=PureToken.BYSCORE,
            offset=0,
            count=1,
            rev=True,
        )
        assert await client.zrange("b{foo}", 0, -1) == [b"a2"]
        # by lex
        assert await client.zrangestore(
            "b{foo}", "a{foo}", "[a2", "(a3", sortby=PureToken.BYLEX, offset=0, count=1
        )
        assert await client.zrange("b{foo}", 0, -1) == [b"a2"]

    async def test_zrangebylex(self, client):
        await client.zadd("a{foo}", dict(a=0, b=0, c=0, d=0, e=0, f=0, g=0))
        assert await client.zrangebylex("a{foo}", "-", "[c") == [b("a"), b("b"), b("c")]
        assert await client.zrangebylex("a{foo}", "-", "(c") == [b("a"), b("b")]
        assert await client.zrangebylex("a{foo}", "[aaa", "(g") == [
            b("b"),
            b("c"),
            b("d"),
            b("e"),
            b("f"),
        ]
        assert await client.zrangebylex("a{foo}", "[f", "+") == [b("f"), b("g")]
        assert await client.zrangebylex("a{foo}", "-", "+", offset=3, count=2) == [
            b("d"),
            b("e"),
        ]

    async def test_zrevrangebylex(self, client):
        await client.zadd("a{foo}", dict(a=0, b=0, c=0, d=0, e=0, f=0, g=0))
        assert await client.zrevrangebylex("a{foo}", "[c", "-") == [
            b("c"),
            b("b"),
            b("a"),
        ]
        assert await client.zrevrangebylex("a{foo}", "(c", "-") == [b("b"), b("a")]
        assert await client.zrevrangebylex("a{foo}", "(g", "[aaa") == [
            b("f"),
            b("e"),
            b("d"),
            b("c"),
            b("b"),
        ]
        assert await client.zrevrangebylex("a{foo}", "+", "[f") == [b("g"), b("f")]
        assert await client.zrevrangebylex("a{foo}", "+", "-", offset=3, count=2) == [
            b("d"),
            b("c"),
        ]

    async def test_zrangebyscore(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3, a4=4, a5=5))
        assert await client.zrangebyscore("a{foo}", 2, 4) == [b("a2"), b("a3"), b("a4")]

        # slicing with start/num
        assert await client.zrangebyscore("a{foo}", 2, 4, offset=1, count=2) == [
            b("a3"),
            b("a4"),
        ]

        # withscores
        assert await client.zrangebyscore("a{foo}", 2, 4, withscores=True) == [
            (b("a2"), 2.0),
            (b("a3"), 3.0),
            (b("a4"), 4.0),
        ]

        # custom score function
        assert (
            await client.zrangebyscore(
                "a{foo}",
                2,
                4,
                withscores=True,
            )
            == [(b("a2"), 2), (b("a3"), 3), (b("a4"), 4)]
        )

    async def test_zrank(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3, a4=4, a5=5))
        assert await client.zrank("a{foo}", "a1") == 0
        assert await client.zrank("a{foo}", "a2") == 1
        assert await client.zrank("a{foo}", "a6") is None

    async def test_zrem(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zrem("a{foo}", "a2") == 1
        assert await client.zrange("a{foo}", 0, -1) == [b("a1"), b("a3")]
        assert await client.zrem("a{foo}", "b{foo}") == 0
        assert await client.zrange("a{foo}", 0, -1) == [b("a1"), b("a3")]

    async def test_zrem_multiple_keys(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zrem("a{foo}", "a1", "a2") == 2
        assert await client.zrange("a{foo}", 0, 5) == [b("a3")]

    async def test_zremrangebylex(self, client):
        await client.zadd("a{foo}", dict(a=0, b=0, c=0, d=0, e=0, f=0, g=0))
        assert await client.zremrangebylex("a{foo}", "-", "[c") == 3
        assert await client.zrange("a{foo}", 0, -1) == [b("d"), b("e"), b("f"), b("g")]
        assert await client.zremrangebylex("a{foo}", "[f", "+") == 2
        assert await client.zrange("a{foo}", 0, -1) == [b("d"), b("e")]
        assert await client.zremrangebylex("a{foo}", "[h", "+") == 0
        assert await client.zrange("a{foo}", 0, -1) == [b("d"), b("e")]

    async def test_zremrangebyrank(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3, a4=4, a5=5))
        assert await client.zremrangebyrank("a{foo}", 1, 3) == 3
        assert await client.zrange("a{foo}", 0, 5) == [b("a1"), b("a5")]

    async def test_zremrangebyscore(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3, a4=4, a5=5))
        assert await client.zremrangebyscore("a{foo}", 2, 4) == 3
        assert await client.zrange("a{foo}", 0, -1) == [b("a1"), b("a5")]
        assert await client.zremrangebyscore("a{foo}", 2, 4) == 0
        assert await client.zrange("a{foo}", 0, -1) == [b("a1"), b("a5")]

    async def test_zrevrange(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zrevrange("a{foo}", 0, 1) == [b("a3"), b("a2")]
        assert await client.zrevrange("a{foo}", 1, 2) == [b("a2"), b("a1")]

        # withscores
        assert await client.zrevrange("a{foo}", 0, 1, withscores=True) == [
            (b("a3"), 3.0),
            (b("a2"), 2.0),
        ]
        assert await client.zrevrange("a{foo}", 1, 2, withscores=True) == [
            (b("a2"), 2.0),
            (b("a1"), 1.0),
        ]

        # custom score function
        assert await client.zrevrange("a{foo}", 0, 1, withscores=True,) == [
            (b("a3"), 3.0),
            (b("a2"), 2.0),
        ]

    async def test_zrevrangebyscore(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3, a4=4, a5=5))
        assert await client.zrevrangebyscore("a{foo}", 4, 2) == [
            b("a4"),
            b("a3"),
            b("a2"),
        ]

        # slicing with start/num
        assert await client.zrevrangebyscore("a{foo}", 4, 2, offset=1, count=2) == [
            b("a3"),
            b("a2"),
        ]

        # withscores
        assert await client.zrevrangebyscore("a{foo}", 4, 2, withscores=True) == [
            (b("a4"), 4.0),
            (b("a3"), 3.0),
            (b("a2"), 2.0),
        ]

        # custom score function
        assert (
            await client.zrevrangebyscore(
                "a{foo}",
                4,
                2,
                withscores=True,
            )
            == [(b("a4"), 4), (b("a3"), 3), (b("a2"), 2)]
        )

    async def test_zrevrank(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3, a4=4, a5=5))
        assert await client.zrevrank("a{foo}", "a1") == 4
        assert await client.zrevrank("a{foo}", "a2") == 3
        assert await client.zrevrank("a{foo}", "a6") is None

    async def test_zscore(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        assert await client.zscore("a{foo}", "a1") == 1.0
        assert await client.zscore("a{foo}", "a2") == 2.0
        assert await client.zscore("a{foo}", "a4") is None

    async def test_zunion_sum(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert await client.zunion("a{foo}", "b{foo}", "c{foo}") == [
            b"a2",
            b"a4",
            b"a3",
            b"a1",
        ]
        assert await client.zunion("a{foo}", "b{foo}", "c{foo}", withscores=True) == [
            (b"a2", 3),
            (b"a4", 4),
            (b"a3", 8),
            (b"a1", 9),
        ]

    async def test_zunionstore_sum(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zunionstore("a{foo}", "b{foo}", "c{foo}", destination="d{foo}")
            == 4
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == [
            (b("a2"), 3),
            (b("a4"), 4),
            (b("a3"), 8),
            (b("a1"), 9),
        ]

    async def test_zunionstore_max(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zunionstore(
                "a{foo}",
                "b{foo}",
                "c{foo}",
                destination="d{foo}",
                aggregate=PureToken.MAX,
            )
            == 4
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == [
            (b("a2"), 2),
            (b("a4"), 4),
            (b("a3"), 5),
            (b("a1"), 6),
        ]

    async def test_zunionstore_min(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=4))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zunionstore(
                "a{foo}",
                "b{foo}",
                "c{foo}",
                destination="d{foo}",
                aggregate=PureToken.MIN,
            )
            == 4
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == [
            (b("a1"), 1),
            (b("a2"), 2),
            (b("a3"), 3),
            (b("a4"), 4),
        ]

    async def test_zunionstore_with_weight(self, client):
        await client.zadd("a{foo}", dict(a1=1, a2=1, a3=1))
        await client.zadd("b{foo}", dict(a1=2, a2=2, a3=2))
        await client.zadd("c{foo}", dict(a1=6, a3=5, a4=4))
        assert (
            await client.zunionstore(
                "a{foo}", "b{foo}", "c{foo}", destination="d{foo}", weights=[1, 2, 3]
            )
            == 4
        )
        assert await client.zrange("d{foo}", 0, -1, withscores=True) == [
            (b("a2"), 5),
            (b("a4"), 12),
            (b("a3"), 20),
            (b("a1"), 23),
        ]

    @pytest.mark.min_server_version("6.1.240")
    async def test_zmscore(self, client):
        with pytest.raises(DataError):
            await client.zmscore("invalid_key")

        assert await client.zmscore("invalid_key", "invalid_member") == [None]

        await client.zadd("a{foo}", dict(a1=1, a2=2, a3=3.5))
        assert (await client.zmscore("a{foo}", "a1", "a2", "a3", "a4")) == [
            1.0,
            2.0,
            3.5,
            None,
        ]

    async def test_zscan(self, client):
        await client.zadd("a", dict(a=1, b=2, c=3))
        cursor, pairs = await client.zscan("a")
        assert cursor == 0
        assert set(pairs) == set([(b("a"), 1), (b("b"), 2), (b("c"), 3)])
        _, pairs = await client.zscan("a", match="a")
        assert set(pairs) == set([(b("a"), 1)])

    async def test_zscan_iter(self, client):
        await client.zadd("a", dict(a=1, b=2, c=3))
        pairs = set()
        async for pair in client.zscan_iter("a"):
            pairs.add(pair)
        assert pairs == set([(b("a"), 1), (b("b"), 2), (b("c"), 3)])
        async for pair in client.zscan_iter("a", match="a"):
            assert pair == (b("a"), 1)
