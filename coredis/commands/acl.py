from typing import Any, Callable, Dict, List, Optional, Union

from deprecated.sphinx import versionadded

from coredis.utils import NodeFlag, dict_merge, list_keys_to_dict
from coredis.validators import mutually_exclusive_parameters

from . import CommandGroup, CommandMixin, redis_command


class ACLCommandMixin(CommandMixin):
    RESPONSE_CALLBACKS: Dict[str, Callable] = {}  # dict_merge(string_keys_to_dict())

    @versionadded(version="3.0.0")
    @redis_command(
        "ACL CAT",
        minimum_server_version="6.0.0",
        group=CommandGroup.SERVER,
    )
    async def acl_cat(self, categoryname: Optional[str] = None) -> list:
        """
        List the ACL categories or the commands inside a category


        :return: a list of ACL categories or a list of commands inside a given category.
         The command may return an error if an invalid category name is given as argument.

        """

        pieces = []

        if categoryname:
            pieces.append(categoryname)

        return await self.execute_command("ACL CAT", *pieces)

    @versionadded(version="3.0.0")
    @redis_command(
        "ACL DELUSER",
        minimum_server_version="6.0.0",
        group=CommandGroup.SERVER,
    )
    async def acl_deluser(self, *usernames: str) -> int:
        """
        Remove the specified ACL users and the associated rules


        :return: The number of users that were deleted.
         This number will not always match the number of arguments since
         certain users may not exist.
        """

        return await self.execute_command("ACL DELUSER", *usernames)

    @versionadded(version="3.0.0")
    @redis_command(
        "ACL GENPASS", minimum_server_version="6.0.0", group=CommandGroup.SERVER
    )
    async def acl_genpass(self, bits: Optional[int] = None) -> str:
        """
        Generate a pseudorandom secure password to use for ACL users


        :return: by default 64 bytes string representing 256 bits of pseudorandom data.
         Otherwise if an argument if needed, the output string length is the number of
         specified bits (rounded to the next multiple of 4) divided by 4.
        """
        pieces = []

        if bits is not None:
            pieces.append(bits)

        return await self.execute_command("ACL GENPASS", *pieces)

    @versionadded(version="3.0.0")
    @redis_command(
        "ACL GETUSER",
        minimum_server_version="6.0.0",
        group=CommandGroup.SERVER,
    )
    async def acl_getuser(self, username: str) -> None:
        """
        Get the rules for a specific ACL user

        :return:
        """

        return await self.execute_command("ACL GETUSER", username)

    @versionadded(version="3.0.0")
    @redis_command(
        "ACL LIST",
        minimum_server_version="6.0.0",
        group=CommandGroup.SERVER,
    )
    async def acl_list(self) -> Any:
        """
        List the current ACL rules in ACL config file format

        :return:
        """

        return await self.execute_command("ACL LIST")

    @versionadded(version="3.0.0")
    @redis_command(
        "ACL LOAD",
        minimum_server_version="6.0.0",
        group=CommandGroup.SERVER,
    )
    async def acl_load(self) -> bool:
        """
        Reload the ACLs from the configured ACL file


        :return: True if successful. The command may fail with an error for several reasons:
         - if the file is not readable
         - if there is an error inside the file, and in such case the error will be reported to
           the user in the error.
         - Finally the command will fail if the server is not configured to use an external
           ACL file.

        """

        return await self.execute_command("ACL LOAD")

    @mutually_exclusive_parameters(
        "count", "reset", details="See: https://redis.io/commands/ACL LOG"
    )
    @versionadded(version="3.0.0")
    @redis_command(
        "ACL LOG",
        minimum_server_version="6.0.0",
        group=CommandGroup.SERVER,
    )
    async def acl_log(
        self, count: Optional[int] = None, reset: Optional[bool] = None
    ) -> Union[list, bool]:
        """
        List latest events denied because of ACLs in place


        :param count:
        :param reset:



        :return: When called to show security events a list of ACL security events.
         When called with ``RESET`` True if the security log was cleared.

        """
        pieces = []
        # Handle operation

        if count is not None:
            pieces.append(count)

        if reset is not None:
            pieces.append(reset)

        return await self.execute_command("ACL LOG", *pieces)

    @versionadded(version="3.0.0")
    @redis_command(
        "ACL SAVE",
        minimum_server_version="6.0.0",
        group=CommandGroup.SERVER,
    )
    async def acl_save(self) -> bool:
        """
        Save the current ACL rules in the configured ACL file

        :return: True if successful. The command may fail with an error for several reasons:
         - if the file cannot be written, or
         - if the server is not configured to use an external ACL file.

        """

        return await self.execute_command("ACL SAVE")

    @versionadded(version="3.0.0")
    @redis_command(
        "ACL SETUSER",
        minimum_server_version="6.0.0",
        group=CommandGroup.SERVER,
    )
    async def acl_setuser(self, username: str, rules: Optional[List[str]]) -> bool:
        """
        Modify or create the rules for a specific ACL user


        :return: True if successful. If the rules contain errors, the error is returned.
        """

        return await self.execute_command("ACL SETUSER", username, *rules)

    @versionadded(version="3.0.0")
    @redis_command(
        "ACL USERS",
        minimum_server_version="6.0.0",
        group=CommandGroup.SERVER,
    )
    async def acl_users(self) -> Any:
        """
        List the username of all the configured ACL rules


        :return:
        """

        return await self.execute_command("ACL USERS")

    @versionadded(version="3.0.0")
    @redis_command(
        "ACL WHOAMI",
        minimum_server_version="6.0.0",
        group=CommandGroup.SERVER,
    )
    async def acl_whoami(self) -> str:
        """
        Return the name of the user associated to the current connection


        :return: the username of the current connection.
        """

        return await self.execute_command("ACL WHOAMI")


class ClusterACLCommandMixin:
    NODES_FLAGS = dict_merge(
        list_keys_to_dict(
            [
                "ACL CAT",
                "ACL LIST",
                "ACL WHOAMI",
                "ACL GENPASS",
            ],
            NodeFlag.RANDOM,
        ),
    )
