from typing import List, Literal

from deprecated.sphinx import versionadded, versionchanged

from coredis.tokens import PureToken
from coredis.utils import NodeFlag, bool_ok, dict_merge, list_keys_to_dict, nativestr

from . import CommandMixin


class ScriptingCommandMixin(CommandMixin):

    RESPONSE_CALLBACKS = {
        "SCRIPT EXISTS": lambda r: list(map(bool, r)),
        "SCRIPT FLUSH": bool_ok,
        "SCRIPT KILL": bool_ok,
        "SCRIPT LOAD": nativestr,
    }

    @versionchanged(
        reason="""
            - Separate ``keys_and_args`` into :paramref:`keys` and :paramref:`args`
            - Remove ``numkeys`` argument
            """,
        version="3.0.0",
    )
    async def eval(self, script, keys: List[str] = [], args: List[str] = []):
        """
        Execute the Lua ``script``, specifying the ``numkeys`` the script
        will touch and the key names and argument values in ``keys_and_args``.
        Returns the result of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """

        return await self.execute_command("EVAL", script, len(keys), *keys, *args)

    @versionchanged(
        reason="""
            - Separate ``keys_and_args`` into :paramref:`keys` and :paramref:`args`
            - Remove ``numkeys`` argument
            """,
        version="3.0.0",
    )
    async def evalsha(
        self, sha1: str, keys: List[str] = [], args: List[str] = []
    ) -> None:
        """
        Use the ``sha`` to execute a Lua script already registered via EVAL
        or SCRIPT LOAD. Specify the ``numkeys`` the script will touch and the
        key names and argument values in ``keys_and_args``. Returns the result
        of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """

        return await self.execute_command("EVALSHA", sha1, len(keys), *keys, *args)

    async def script_exists(self, *sha1s: str) -> List[bool]:
        """
        Check if a script exists in the script cache by specifying the SHAs of
        each script as ``sha1s``.

        :return: a list of boolean values indicating if each already script exists in the cache.
        """

        return await self.execute_command("SCRIPT EXISTS", *sha1s)

    @versionchanged(
        reason="""
        - Changed :paramref:`sync_type` to :class:`PureToken`
        """,
        version="3.0.0",
    )
    @versionadded(version="2.1.0")
    async def script_flush(
        self, sync_type: Literal[PureToken.SYNC, PureToken.ASYNC] = PureToken.SYNC
    ) -> bool:
        """
        Flushes all scripts from the script cache
        """

        if sync_type:
            pieces = [sync_type.value]

        return await self.execute_command("SCRIPT FLUSH", *pieces)

    async def script_kill(self) -> bool:
        """Kills the currently executing Lua script"""

        return await self.execute_command("SCRIPT KILL")

    async def script_load(self, script: str) -> str:
        """Loads a Lua ``script`` into the script cache. Returns the SHA."""

        return await self.execute_command("SCRIPT LOAD", script)

    def register_script(self, script):
        """
        Registers a Lua ``script`` specifying the ``keys`` it will touch.
        Returns a Script object that is callable and hides the complexity of
        dealing with scripts, keys, and shas. This is the preferred way of
        working with Lua scripts.
        """
        from coredis.scripting import Script

        return Script(self, script)


class ClusterScriptingCommandMixin(ScriptingCommandMixin):

    NODES_FLAGS = dict_merge(
        {"SCRIPT KILL": NodeFlag.BLOCKED},
        list_keys_to_dict(
            ["SCRIPT LOAD", "SCRIPT FLUSH", "SCRIPT EXISTS"], NodeFlag.ALL_MASTERS
        ),
    )

    RESULT_CALLBACKS = {
        "SCRIPT LOAD": lambda res: list(res.values()).pop(),
        "SCRIPT EXISTS": lambda res: [all(k) for k in zip(*res.values())],
        "SCRIPT FLUSH": lambda res: all(res.values()),
    }
