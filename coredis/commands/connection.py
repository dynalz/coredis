from typing import Optional

from coredis.utils import NodeFlag, bool_ok

from . import CommandMixin


class ConnectionCommandMixin(CommandMixin):

    RESPONSE_CALLBACKS = {
        "AUTH": bool,
        "SELECT": bool_ok,
    }

    async def echo(self, message: str) -> str:
        "Echo the string back from the server"

        return await self.execute_command("ECHO", message)

    async def ping(self, *, message: Optional[str] = None) -> str:
        """
        Ping the server

        :return: ``PONG``, when no argument is provided.
         the argument provided, when applicable.
        """
        pieces = []
        if message:
            pieces.append(message)
        return await self.execute_command("PING", *pieces)


class ClusterConnectionCommandMixin(ConnectionCommandMixin):

    NODES_FLAGS = {"PING": NodeFlag.ALL_NODES, "ECHO": NodeFlag.ALL_NODES}

    RESULT_CALLBACKS = {"ECHO": lambda res: res, "PING": lambda res: res}
