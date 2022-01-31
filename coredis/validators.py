import functools
import inspect
from typing import List

from .exceptions import CommandSyntaxError


class MutuallyExclusiveParametersError(CommandSyntaxError):
    def __init__(self, arguments: List[str], details: str):
        message = (
            f"[{','.join(arguments)}] are mutually exclusive."
            f"{' '+details if details else ''}"
        )
        super().__init__(arguments, message)


class MutuallyInclusiveParametersMissing(CommandSyntaxError):
    def __init__(self, arguments: List[str], details: str):
        message = (
            f"[{','.join(arguments)}] are mutually inclusive and must be provided together."
            f"{' '+details if details else ''}"
        )
        super().__init__(arguments, message)


def mutually_exclusive_parameters(*exclusive_params: str, details=None):
    def _wrapped(func):
        params = inspect.signature(func).parameters

        @functools.wraps(func)
        async def _inner(*args, **kwargs):
            call_args = inspect.getcallargs(func, *args, **kwargs)
            provided_args = [
                k
                for k in exclusive_params
                if call_args.get(k) not in [params[k].default, None]
            ]

            if len(provided_args) > 1:
                raise MutuallyExclusiveParametersError(provided_args, details)

            return await func(*args, **kwargs)

        return _inner

    return _wrapped


def mutually_inclusive_parameters(*inclusive_params: str, details=None):
    def _wrapped(func):
        params = inspect.signature(func).parameters

        @functools.wraps(func)
        async def _inner(*args, **kwargs):
            call_args = inspect.getcallargs(func, *args, **kwargs)
            provided_args = [
                k
                for k in inclusive_params
                if call_args.get(k) not in [params[k].default, None]
            ]

            if provided_args and len(provided_args) != len(inclusive_params):
                raise MutuallyInclusiveParametersMissing(inclusive_params, details)

            return await func(*args, **kwargs)

        return _inner

    return _wrapped
