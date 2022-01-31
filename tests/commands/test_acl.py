import pytest

from tests.conftest import targets


@targets("redis_basic", "redis_auth", "redis_cluster")
@pytest.mark.asyncio()
class TestACL:
    async def test_acl_cat(self, client):
        assert {b"keyspace"} & set(await client.acl_cat())
        assert {b"keys"} & set(await client.acl_cat("keyspace"))

    async def test_del_user(self, client):
        assert 0 == await client.acl_deluser("john", "doe")

    async def test_gen_pass(self, client):
        assert len(await client.acl_genpass()) == 64
        assert len(await client.acl_genpass(4)) == 1

    async def test_getuser(self, client):
        assert await client.acl_getuser("default") == b"default"

    async def test_setuser(self, client):
        assert await client.acl_setuser("default") == b"default"

    async def test_list(self, client):
        assert await client.acl_list() == [b"default"]

    async def test_log(self, client):
        assert await client.acl_log() == [b"default"]

    async def test_users(self, client):
        assert await client.acl_users() == [b"default"]

    async def test_whoami(self, client):
        assert await client.acl_whoami() == b"default"
