from typing import List, Optional

from deprecated.sphinx import versionadded

from . import CommandGroup, CommandMixin, redis_command


class CommandCommandMixin(CommandMixin):

    RESPONSE_CALLBACKS = {}  # dict_merge(string_keys_to_dict())

    @versionadded(version="3.0.0")
    @redis_command(
        "COMMAND",
        group=CommandGroup.SERVER,
    )
    async def command(self) -> list:
        """
        Get array of Redis command details

        :return: nested list of command details.  Commands are returned
         in random order.
        """

        return await self.execute_command("COMMAND")

    @versionadded(version="3.0.0")
    @redis_command(
        "COMMAND COUNT",
        group=CommandGroup.SERVER,
    )
    async def command_count(self) -> int:
        """
        Get total number of Redis commands

        :return: number of commands returned by ``COMMAND``

        """

        return await self.execute_command("COMMAND COUNT")

    @versionadded(version="3.0.0")
    @redis_command(
        "COMMAND GETKEYS",
        group=CommandGroup.SERVER,
    )
    async def command_getkeys(self, *args: str) -> list:
        """
        Extract keys given a full Redis command

        :return: list of keys from your command.
        """

        return await self.execute_command("COMMAND GETKEYS", *args)

    @versionadded(version="3.0.0")
    @redis_command(
        "COMMAND INFO",
        group=CommandGroup.SERVER,
    )
    async def command_info(self, command_names: Optional[List[str]]) -> list:
        """
        Get list of specific Redis command details, or all when no argument is given.

        :return: nested list of command details.

        """

        return await self.execute_command("COMMAND INFO", *command_names)
