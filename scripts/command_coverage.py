import datetime
import functools
import inspect
import os
import re
from collections import defaultdict
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import click
import coredis
import inflect
import redis
import redis.cluster
import requests
from coredis import PureToken
from jinja2 import Environment
from packaging import version

MAX_SUPPORTED_VERSION = version.parse("6.999.999")
MIN_SUPPORTED_VERSION = version.parse("5.0.0")

MAPPING = {"DEL": "delete"}
SKIP_SPEC = ["BITFIELD", "BITFIELD_RO"]

REDIS_ARGUMENT_TYPE_MAPPING = {
    "array": List,
    "simple-string": str,
    "bulk-string": str,
    "string": str,
    "pattern": str,
    "key": str,
    "integer": int,
    "double": float,
    "unix-time": Union[int, datetime.datetime],
    "pure-token": bool,
}
REDIS_ARGUMENT_NAME_OVERRIDES = {
    "BITPOS": {"end_index_index_unit": "end_index_unit"},
    "BITCOUNT": {"index_index_unit": "index_unit"},
    "CLIENT REPLY": {"on_off_skip": "mode"},
    "ZADD": {"score_member": "member_score"},
    "SORT": {"sorting": "alpha"},
    "SCRIPT FLUSH": {"async": "sync_type"},
}
IGNORED_ARGUMENTS = {
    "ZDIFF": ["numkeys"],
    "ZDIFFSTORE": ["numkeys"],
    "ZINTER": ["numkeys"],
    "ZINTERSTORE": ["numkeys"],
    "ZUNION": ["numkeys"],
    "ZUNIONSTORE": ["numkeys"],
    "EVAL": ["numkeys"],
    "EVALSHA": ["numkeys"],
    "MIGRATE": ["key_or_empty_string"],
}
REDIS_RETURN_OVERRIDES = {
    "COPY": bool,
    "PERSIST": bool,
    "INCRBYFLOAT": float,
    "EXPIRE": bool,
    "EXPIREAT": bool,
    "CLIENT LIST": List[Dict[str, str]],
    "CLIENT INFO": Dict[str, str],
    "CLIENT TRACKINGINFO": Dict[str, str],
    "LPOS": Optional[Union[int, List[int]]],
    "MGET": List[str],
    "MSETNX": bool,
    "SCRIPT FLUSH": bool,
    "SCRIPT KILL": bool,
    "SCRIPT EXISTS": List[bool],
}
ARGUMENT_DEFAULTS = {
    "LPOS": {"count": 1},
    "LPOP": {"count": 1},
    "RPOP": {"count": 1},
    "ZPOPMAX": {"count": 1},
    "ZPOPMIN": {"count": 1},
    "SPOP": {"count": 1},
    "SRANDMEMBER": {"count": 1},
    "SORT": {"gets": []},
    "SCRIPT FLUSH": {"sync_type": PureToken.SYNC},
    "EVAL": {"keys": [], "args": []},
    "EVALSHA": {"keys": [], "args": []},
}
ARGUMENT_DEFAULTS_NON_OPTIONAL = {
    "KEYS": {"pattern": "*"},
    "HSCAN": {"cursor": 0},
    "SCAN": {"cursor": 0},
    "SSCAN": {"cursor": 0},
    "ZSCAN": {"cursor": 0},
}
ARGUMENT_OPTIONALITY = {
    "MIGRATE": {"keys": False},
}
REDIS_ARGUMENT_FORCED_ORDER = {"SETEX": ["key", "value", "seconds"]}
BLOCK_ARGUMENT_FORCED_ORDER = {"ZADD": {"member_scores": ["member", "score"]}}
STD_GROUPS = [
    "string",
    "bitmap",
    "list",
    "sorted-set",
    "generic",
    "transactions",
    "scripting",
    "geo",
    "hash",
    "hyperloglog",
    "pubsub",
    "set",
    "stream",
]

VERSIONADDED_DOC = re.compile("(.. versionadded:: ([\d\.]+))")
VERSIONCHANGED_DOC = re.compile("(.. versionchanged:: ([\d\.]+))")

inflection_engine = inflect.engine()


def version_changed_from_doc(doc):
    if not doc:
        return
    v = VERSIONCHANGED_DOC.findall(doc)

    if v:
        return version.parse(v[0][1])


def version_added_from_doc(doc):
    if not doc:
        return
    v = VERSIONADDED_DOC.findall(doc)

    if v:
        return version.parse(v[0][1])


@functools.lru_cache
def get_commands():
    if not os.path.isdir("/var/tmp/redis-doc"):
        os.system("git clone git@github.com:redis/redis-doc /var/tmp/redis-doc")

    return requests.get("https://redis.io/commands.json").json()


def render_signature(signature):
    v = str(signature)

    v = re.sub("<class '(.*?)'>", "\\1", v)
    v = re.sub("<PureToken.(.*?): '(.*?)'>", "PureToken.\\1", v)

    return v


def compare_signatures(s1, s2):
    return [(p.name, p.default, p.annotation) for p in s1.parameters.values()] == [
        (p.name, p.default, p.annotation) for p in s2.parameters.values()
    ]


def get_token_mapping():
    commands = get_commands()
    mapping = {}

    for command, details in commands.items():

        def _extract_tokens(obj):
            tokens = []

            if args := obj.get("arguments"):
                for arg in args:
                    if arg["type"] == "pure-token":
                        tokens.append((arg["name"], arg["token"]))

                    if arg.get("arguments"):
                        tokens.extend(_extract_tokens(arg))

            return tokens

        for token in _extract_tokens(details):
            mapping.setdefault(token, []).append(command)

    return mapping


def read_command_docs(command):
    doc = open(
        "/var/tmp/redis-doc/commands/%s.md" % command.lower().replace(" ", "-")
    ).read()

    if not doc.find("@return") > 0:
        return [None, ""]
    return_description = re.compile(
        "(@(.*?)-reply[:,]*\s*(.*?)\n)", re.MULTILINE
    ).findall(doc)

    def sanitize_description(desc):
        if not desc:
            return ""
        return_description = (
            desc.replace("a nil bulk reply", "``None``")
            .replace("a null bulk reply", "``None``")
            .replace(", specifically:", "")
            .replace("specifically:", "")
        )
        return_description = re.sub("`(.*?)`", "``\\1``", return_description)
        return_description = return_description.replace("``nil``", "``None``")
        return_description = re.sub("_(.*?)_", "``\\1``", return_description)
        return_description = return_description.replace(
            "````None````", "``None``"
        )  # lol
        return_description = re.sub("^\s*([^\w]+)", "", return_description)

        return return_description

    full_description = re.compile("@return(.*)@examples", re.DOTALL).findall(doc)

    if not full_description:
        full_description = re.compile("@return(.*)##", re.DOTALL).findall(doc)

    if not full_description:
        full_description = re.compile("@return(.*)$", re.DOTALL).findall(doc)

    if full_description:
        full_description = full_description[0].strip()

    full_description = sanitize_description(full_description)

    if full_description:
        full_description = re.sub("((.*)-reply)", "", full_description)
        full_description = full_description.split("\n")
        full_description = [k.strip().lstrip(":") for k in full_description]
        full_description = [k.strip() for k in full_description if k.strip()]

    if return_description:
        if len(return_description) > 0:
            rtypes = {k[1]: k[2] for k in return_description}
            has_nil = False
            has_bool = False

            if "simple-string" in rtypes and rtypes["simple-string"].find("OK") >= 0:
                has_bool = True
                rtypes.pop("simple-string")

            if "nil" in rtypes:
                rtypes.pop("nil")
                has_nil = True

            for description in rtypes.values():
                if "nil" in description or "null" in description:
                    has_nil = True

            mapped_types = [REDIS_ARGUMENT_TYPE_MAPPING.get(k, "Any") for k in rtypes]

            if has_bool:
                mapped_types.append(bool)

            if len(mapped_types) > 1:
                mapped_types_evaled = eval(
                    ",".join(["%s" % getattr(k , "_name",getattr(k, "__name__", str(k))) for k in mapped_types])
                )
                rtype = (
                    Optional[Union[mapped_types_evaled]]

                    if has_nil
                    else Union[mapped_types_evaled]
                )
            else:
                sub_type = mapped_types[0]
                if 'array' in rtypes:
                    if rtypes['array'].find('nested')>=0:
                        sub_type = sub_type[sub_type[Any]]
                    else:
                        if rtypes['array'].find('integer')>=0:
                            sub_type = sub_type[int]
                        elif rtypes['array'].find('and their')>=0:
                            sub_type = sub_type[Tuple[str,str]]
                        else:
                            sub_type = sub_type[str]
                rtype = Optional[sub_type] if has_nil else sub_type

            rdesc = [sanitize_description(k[2]) for k in return_description]
            rdesc = [k for k in rdesc if k.strip()]

            return rtype, full_description

    return Any, ""


def get_official_commands(group=None):
    response = get_commands()
    by_group = {}
    [
        by_group.setdefault(command["group"], []).append({**command, **{"name": name}})

        for name, command in response.items()

        if version.parse(command["since"]) < MAX_SUPPORTED_VERSION
    ]

    return by_group if not group else by_group.get(group)


def find_method(kls, command_name):
    members = inspect.getmembers(kls)
    mapping = {
        k[0]: k[1]

        for k in members

        if inspect.ismethod(k[1]) or inspect.isfunction(k[1])
    }

    return mapping.get(command_name)


def redis_command_link(command):
    return (
        f'`{command} <https://redis.io/commands/{command.lower().replace(" ", "-")}>`_'
    )


def skip_command(command):
    # if command["name"] == "MIGRATE":
    #    return False
    # return True

    if (
        command["name"].find(" HELP") >= 0
        or command["summary"].find("container for") >= 0
    ):
        return True

    return False


def is_deprecated(command, kls):
    if (
        command.get("deprecated_since")
        and version.parse(command["deprecated_since"]) < MAX_SUPPORTED_VERSION
    ):
        replacement = command.get("replaced_by", "")
        replacement = re.sub("`(.*?)`", "``\\1``", replacement)
        replacement_method = re.search("(``(.*?)``)", replacement)
        replacement_method = replacement_method.group()

        if replacement_method:
            preferred_method = f"Use :meth:`~coredis.{kls.__name__}.{sanitized(replacement_method, None).replace('`','')}` "
            replacement = replacement.replace(replacement_method, preferred_method)

        return command["deprecated_since"], replacement


def sanitized(x, command=None):
    cleansed_name = x.lower().replace("-", "_").replace(":", "_")

    if command:
        override = REDIS_ARGUMENT_NAME_OVERRIDES.get(command["name"], {}).get(
            cleansed_name
        )

        if override:
            cleansed_name = override

    if cleansed_name in ["id", "type"]:
        cleansed_name = cleansed_name + "_"

    return cleansed_name


def skip_arg(argument, command):
    arg_version = argument.get("since")

    if arg_version and version.parse(arg_version) > MAX_SUPPORTED_VERSION:
        return True

    if argument["name"] in IGNORED_ARGUMENTS.get(command["name"], []):
        return True

    return False


def get_type(arg):
    inferred_type = REDIS_ARGUMENT_TYPE_MAPPING.get(arg["type"], Any)

    if arg["name"] in ["seconds", "milliseconds"] and inferred_type == int:
        return Union[int, datetime.timedelta]

    if arg["name"] == "yes/no" and inferred_type == str:
        return bool

    return inferred_type


def get_type_annotation(arg, default=None):
    if arg["type"] == "oneof" and all(
        k["type"] == "pure-token" for k in arg["arguments"]
    ):
        tokens = ["PureToken.%s" % s["name"].upper() for s in arg["arguments"]]
        literal_type = eval(f"Literal[{','.join(tokens)}]")

        if arg.get("optional") and not default is not None:
            return Optional[literal_type]

        return literal_type
    else:
        return get_type(arg)


def get_argument(
    arg, parent, command, arg_type=inspect.Parameter.KEYWORD_ONLY, multiple=False
):
    if skip_arg(arg, command):
        return [[], []]
    param_list = []
    decorators = []

    if arg["type"] == "block":
        if arg.get("multiple"):
            name = inflection_engine.plural(sanitized(arg["name"], command))
            forced_order = BLOCK_ARGUMENT_FORCED_ORDER.get(command["name"], {}).get(
                name
            )

            if forced_order:
                child_args = sorted(
                    arg["arguments"], key=lambda a: forced_order.index(a["name"])
                )
            else:
                child_args = arg["arguments"]
            child_types = [get_type(child) for child in child_args]

            if len(child_types) == 1:
                annotation = List[child_types[0]]
            elif len(child_types) == 2 and child_types[0] == str:
                annotation = Dict[child_types[0], child_types[1]]
            else:
                child_types_repr = ",".join(["%s" % k.__name__ for k in child_types])
                annotation = List[eval(f"Tuple[{child_types_repr}]")]

            param_list.append(
                inspect.Parameter(
                    name,
                    arg_type,
                    annotation=annotation,
                )
            )

        else:
            plist_d = []

            for child in sorted(
                arg["arguments"], key=lambda v: int(v.get("optional") == True)
            ):
                plist, declist = get_argument(
                    child, arg, command, arg_type, arg.get("multiple")
                )
                param_list.extend(plist)

                if not child.get("optional"):
                    plist_d.extend(plist)

            if len(plist_d) > 1:
                mutually_inclusive_params = ",".join(
                    ["'%s'" % child.name for child in plist_d]
                )
                decorators.append(
                    f"@mutually_inclusive_parameters({mutually_inclusive_params})"
                )
    elif arg["type"] == "oneof":
        extra_params = {}

        if all(child["type"] == "pure-token" for child in arg["arguments"]):
            if parent:
                syn_name = sanitized(f"{parent['name']}_{arg.get('name')}", command)
            else:
                syn_name = sanitized(f"{arg.get('token', arg.get('name'))}", command)

            if arg.get("optional"):
                extra_params["default"] = ARGUMENT_DEFAULTS.get(
                    command["name"], {}
                ).get(syn_name)
            param_list.append(
                inspect.Parameter(
                    syn_name,
                    arg_type,
                    annotation=get_type_annotation(
                        arg, default=extra_params.get("default")
                    ),
                    **extra_params,
                )
            )
        else:
            plist_d = []

            for child in arg["arguments"]:
                plist, declist = get_argument(child, arg, command, arg_type, multiple)
                param_list.extend(plist)
                plist_d.extend(plist)
            mutually_exclusive_params = ",".join(["'%s'" % p.name for p in plist_d])
            decorators.append(
                f"@mutually_exclusive_parameters({mutually_exclusive_params}, details='See: https://redis.io/commands/{command['name']}')"
            )
    else:
        name = sanitized(
            arg.get("token", arg["name"])

            if not arg.get("type") == "pure-token"
            else arg["name"],
            command,
        )
        is_variadic = False
        type_annotation = get_type_annotation(
            arg, default=ARGUMENT_DEFAULTS.get(command["name"], {}).get(name)
        )
        extra_params = {}

        if parent and parent.get("optional"):
            type_annotation = Optional[type_annotation]
            extra_params = {"default": None}

        if is_arg_optional(arg, command) and not arg.get("multiple"):
            type_annotation = Optional[type_annotation]
            extra_params = {"default": None}
        else:
            default = ARGUMENT_DEFAULTS_NON_OPTIONAL.get(command["name"], {}).get(name)

            if default is not None:
                extra_params["default"] = default
                arg_type = inspect.Parameter.KEYWORD_ONLY

        if multiple:
            name = inflection_engine.plural(name)

            if not inflection_engine.singular_noun(name):
                name = inflection_engine.plural(name)
            is_variadic = not arg.get("optional")

            if not is_variadic:
                if (
                    default := ARGUMENT_DEFAULTS.get(command["name"], {}).get(name)
                ) is not None:
                    type_annotation = List[type_annotation]
                    extra_params["default"] = default
                elif is_arg_optional(arg, command):
                    type_annotation = Optional[List[type_annotation]]
                else:
                    type_annotation = List[type_annotation]
            else:
                arg_type = inspect.Parameter.VAR_POSITIONAL

        if "default" in extra_params:
            extra_params["default"] = ARGUMENT_DEFAULTS.get(command["name"], {}).get(
                name, extra_params.get("default")
            )

        param_list.append(
            inspect.Parameter(
                name, arg_type, annotation=type_annotation, **extra_params
            )
        )

    return [param_list, decorators]


def is_arg_optional(arg, command):
    command_optionality = ARGUMENT_OPTIONALITY.get(command["name"], {})
    override = command_optionality.get(
        sanitized(arg.get("name", ""), command)
    ) or command_optionality.get(sanitized(arg.get("token", ""), command))

    if override is not None:
        return override

    return arg.get("optional")


def get_command_spec(command):
    arguments = command.get("arguments", [])
    recommended_signature = []
    decorators = []
    forced_order = REDIS_ARGUMENT_FORCED_ORDER.get(command["name"], [])
    mapping = {}

    for k in arguments:
        if not is_arg_optional(k, command) and not k.get("multiple"):
            plist, dlist = get_argument(
                k,
                None,
                command,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
            mapping[k["name"]] = (k, plist)
            recommended_signature.extend(plist)
            decorators.extend(dlist)

    for k in arguments:
        if not is_arg_optional(k, command) and k.get("multiple"):
            plist, dlist = get_argument(
                k,
                None,
                command,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                True,
            )
            mapping[k["name"]] = (k, plist)
            recommended_signature.extend(plist)
            decorators.extend(dlist)

    var_args = [
        k.name

        for k in recommended_signature

        if k.kind == inspect.Parameter.VAR_POSITIONAL
    ]

    if forced_order:
        recommended_signature = sorted(
            recommended_signature,
            key=lambda r: forced_order.index(r.name)

            if r.name in forced_order
            else recommended_signature.index(r),
        )

    if not var_args or "keys" in var_args:
        recommended_signature = sorted(
            recommended_signature,
            key=lambda r: -2

            if r.name in ["key", "keys"]
            else -1

            if r.name == "weights"
            else recommended_signature.index(r),
        )

        for idx, k in enumerate(recommended_signature):
            if k.name == "key":
                n = inspect.Parameter(
                    k.name,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    default=k.default,
                    annotation=k.annotation,
                )
                recommended_signature.remove(k)
                recommended_signature.insert(idx, n)
            elif k.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                n = inspect.Parameter(
                    k.name,
                    inspect.Parameter.KEYWORD_ONLY,
                    default=k.default,
                    annotation=k.annotation,
                )
                recommended_signature.remove(k)
                recommended_signature.insert(idx, n)

    elif {"key"} & {r.name for r in recommended_signature}:
        new_recommended_signature = sorted(
            recommended_signature,
            key=lambda r: -1 if r.name in ["key"] else recommended_signature.index(r),
        )
        reordered = [k.name for k in new_recommended_signature] != [
            k.name for k in recommended_signature
        ]

        for idx, k in enumerate(new_recommended_signature):
            if reordered:
                if k.kind == inspect.Parameter.VAR_POSITIONAL:
                    n = inspect.Parameter(
                        k.name,
                        inspect.Parameter.KEYWORD_ONLY,
                        default=k.default,
                        annotation=List[k.annotation],
                    )
                    new_recommended_signature.remove(k)
                    new_recommended_signature.insert(idx, n)

            if k.name == "key":
                n = inspect.Parameter(
                    k.name,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    default=k.default,
                    annotation=k.annotation,
                )
                new_recommended_signature.remove(k)
                new_recommended_signature.insert(idx, n)
            recommended_signature = new_recommended_signature

    for k in arguments:
        if is_arg_optional(k, command) and k.get("multiple"):
            plist, dlist = get_argument(
                k, None, command, inspect.Parameter.KEYWORD_ONLY, True
            )
            mapping[k["name"]] = (k, plist)
            recommended_signature.extend(plist)
            decorators.extend(dlist)

    for k in [
        k for k in arguments if (is_arg_optional(k, command) and not k.get("multiple"))
    ]:
        if skip_arg(k, command):
            continue
        plist, dlist = get_argument(k, None, command)
        mapping[k["name"]] = (k, plist)
        recommended_signature.extend(plist)
        decorators.extend(dlist)

    if (
        len(recommended_signature) > 1
        and recommended_signature[-2].kind == inspect.Parameter.POSITIONAL_ONLY
    ):
        recommended_signature[-1] = inspect.Parameter(
            recommended_signature[-1].name,
            inspect.Parameter.POSITIONAL_ONLY,
            default=recommended_signature[-1].default,
            annotation=recommended_signature[-1].annotation,
        )

    return recommended_signature, decorators, mapping


def generate_compatibility_section(
    section, kls, parent_kls, redis_namespace, groups, debug=False, next_version="6.6.6"
):
    env = Environment()
    section_template_str = """
{{section}}
{{len(section)*'^'}}

{% for group in groups %}
{% if group in methods_by_group %}
{{group.title()}}
{{len(group)*'-'}}

{% if debug -%}
{% endif %}
{% for method in methods_by_group[group]["supported"] %}
{{redis_command_link(method['redis_method']['name'])}} -> :meth:`~coredis.{{kls.__name__}}.{{method["located"].__name__}}`
{% if method["redis_version_introduced"] > MIN_SUPPORTED_VERSION %}
- New in redis: {{method["redis_version_introduced"]}}
{% endif %}
{% if method["version_added"] %}
- {{method["version_added"]}}
{% endif %}
{% if method["version_changed"] %}
- {{method["version_changed"]}}
{% endif %}
{% if debug %}
Current Signature {% if method.get("full_match") %} (Full Match) {% endif %}

.. code::

    {% for decorator in method["rec_decorators"] %}
    {{decorator}}
    {% endfor -%}
    @redis_command(
    "{{method["command"]["name"]}}",
    {% if method["redis_version_introduced"] > MIN_SUPPORTED_VERSION -%}
    minimum_server_version="{{method["command"].get("since")}}",
    {% endif -%}
    group=CommandGroup.{{method["command"]["group"].upper()}},
    {% if len(method["arg_mapping"]) > 0 -%}
    arguments = {
    {%- for name, arg  in method["arg_mapping"].items() -%}
    {%- for param in arg[1] -%}
    {%- if arg[0].get("since") -%}
    "{{param.name}}" : {"minimum_server_version": "{{arg[0].get("since")}}"}},
    {%- endif -%}
    {%- endfor -%}
    {%- endfor -%}
    {%- endif -%}}
    )
    {% set implementation = method["located"] %}
    {% set implementation = inspect.getclosurevars(implementation).nonlocals.get("func", implementation) %}

    async def {{method["name"]}}{{render_signature(method["current_signature"])}}:
        \"\"\"
        {% for line in implementation.__doc__.split("\n") -%}
        {{line.lstrip()}}
        {% endfor %}
        {% if method["return_summary"] and not method["located"].__doc__.find(":return:")>=1-%}
        \"\"\"

        \"\"\"
        Recommended docstring:

        {{method["summary"]}}

        {% if method["located"].__doc__.find(":param:") < 0 -%}
        {% for p in list(method["rec_signature"].parameters)[1:] -%}
        {% if p != "key" -%}
        :param {{p}}:
        {%- endif -%}
        {% endfor %}
        {% endif -%}
        {% if len(method["return_summary"]) == 1 -%}
        :return: {{method["return_summary"][0]}}
        {%- else -%}
        :return:
        {% for desc in method["return_summary"] -%}
        {{desc}}
        {%- endfor -%}
        {% endif %}
        {% endif -%}
        \"\"\"


        {% if "execute_command" not in inspect.getclosurevars(implementation).unbound -%}
        {{ inspect.getclosurevars(implementation).unbound }}
        # Not Implemented
        {% if len(method["arg_mapping"]) > 0 -%}
        pieces = []
        {% for name, arg  in method["arg_mapping"].items() -%}
        # Handle {{name}}
        {% if len(arg[1]) > 0 -%}
        {% for param in arg[1] -%}
        {% if not arg[0].get("optional") -%}
        {% if arg[0].get("multiple") -%}
        {% if arg[0].get("token") -%}
        pieces.extend(*{{param.name}})
        {% else -%}
        pieces.extend(*{{param.name}})
        {% endif -%}
        {% else -%}
        {% if arg[0].get("token") -%}
        pieces.append("{{arg[0].get("token")}}")
        pieces.append({{param.name}})
        {% else -%}
        pieces.append({{param.name}})
        {% endif -%}
        {% endif -%}
        {% else -%}
        {% if arg[0].get("multiple") -%}

        if {{arg[1][0].name}}:
            pieces.extend({{param.name}})
        {% else -%}

        if {{param.name}}{% if arg[0].get("type") != "pure-token" -%} is not None{%endif%}:
        {%- if arg[0].get("token") -%}
            pieces.append({{arg[0].get("token")}})
        {%- else -%}
            pieces.append({{param.name}})
        {% endif -%}
        {% endif -%}
        {% endif -%}
        {% endfor -%}
        {% endif -%}
        {% endfor -%}

        return await self.execute_command("{{method["command"]["name"]}}", *pieces)
        {% else -%}

        return await self.execute_command("{{method["command"]["name"]}}")
        {% endif -%}
        {% endif -%}
{% if not method.get("full_match") %}
Recommended Signature:

.. code::

    {% for decorator in method["rec_decorators"] %}
    {{decorator}}
    {% endfor -%}
    async def {{method["name"]}}{{render_signature(method["rec_signature"])}}:
        \"\"\"
        {{method["summary"]}}

        {% if "rec_signature" in method %}
        {% for p in list(method["rec_signature"].parameters)[1:] -%}
        :param {{p}}:
        {% endfor %}
        {% endif %}
        {% if len(method["return_summary"]) == 1 %}
        :return: {{method["return_summary"][0]}}
        {% else %}
        :return:
        {% for desc in method["return_summary"] %}
        {{desc}}
        {%- endfor %}
        {% endif %}
        \"\"\"
        pass


{% if method["diff_plus"] or method["diff_minus"] %}
.. code:: text

    Plus: {{ method["diff_plus"] }}
    Minus: {{ method["diff_minus"] }}
{% endif %}

{% endif %}
{% endif %}
{% endfor %}
{% for method in methods_by_group[group]["missing"] %}
{{redis_command_link(method['redis_method']['name'])}} (Unimplemented)
{% if debug %}
Recommended Signature:

.. code::

    {% for decorator in method["rec_decorators"] %}
    {{decorator}}
    {% endfor -%}
    @versionadded(version="{{next_version}}")
    @redis_command(
    "{{method["command"]["name"]}}",
    {% if method["redis_version_introduced"] > MIN_SUPPORTED_VERSION %}minimum_server_version="{{method["command"].get("since")}}",{% endif %}
    group=CommandGroup.{{method["command"]["group"].upper()}},
    {% if len(method["arg_mapping"]) > 0 -%}
    arguments = {
    {% for name, arg  in method["arg_mapping"].items() -%}
    {% for param in arg[1] -%}
    {% if arg[0].get("since") -%}
    "{{param.name}}" = {
        "minimum_server_version": "{{arg[0].get("since")}}",
    },
    {% endif -%}
    {% endfor -%}
    {% endfor -%}
    {% endif -%}
    }
    )
    async def {{method["name"]}}{{render_signature(method["rec_signature"])}}:
        \"\"\"
        {{method["summary"]}}

        {% if "rec_signature" in method %}
        {% for p in list(method["rec_signature"].parameters)[1:] %}
        :param {{p}}:
        {%- endfor %}
        {% endif %}
        {% if len(method["return_summary"]) == 0 %}
        :return: {{method["return_summary"][0]}}
        {% else %}
        :return:
        {% for desc in method["return_summary"] %}
        {{desc}}
        {%- endfor %}
        {% endif %}
        \"\"\"
        pass
{% endif %}
{% endfor %}
{% endif %}
{% endfor %}

    """
    env.globals.update(
        MIN_SUPPORTED_VERSION=MIN_SUPPORTED_VERSION,
        MAX_SUPPORTED_VERSION=MAX_SUPPORTED_VERSION,
        get_official_commands=get_official_commands,
        inspect=inspect,
        len=len,
        list=list,
        skip_command=skip_command,
        redis_command_link=redis_command_link,
        find_method=find_method,
        read_command_docs=read_command_docs,
        kls=kls,
        render_signature=render_signature,
        next_version=next_version,
        debug=debug,
    )
    section_template = env.from_string(section_template_str)
    methods_by_group = {}

    for group in groups:
        supported = []
        missing = []

        methods = {"supported": [], "missing": []}
        for method in get_official_commands(group):
            method_details = {"kls": kls, "command": method}

            if skip_command(method):
                continue
            name = MAPPING.get(
                method["name"],
                method["name"].lower().replace(" ", "_").replace("-", "_"),
            )
            method_details["name"] = name
            method_details["redis_method"] = method
            method_details["located"] = located = find_method(kls, name)
            if parent_kls and find_method(parent_kls, name) == located:
                continue
            method_details[
                "redis_version_introduced"
            ] = redis_version_introduced = version.parse(method["since"])
            method_details["summary"] = summary = method["summary"]
            return_description = ""
            return_summary = ""
            rec_decorators = ""

            doc_string_recommendation = ""

            if not method["name"] in SKIP_SPEC:
                recommended_return = read_command_docs(method["name"])

                if recommended_return:
                    return_summary = recommended_return[1]
                rec_params, rec_decorators, arg_mapping = get_command_spec(method)
                method_details["arg_mapping"] = arg_mapping
                method_details["rec_decorators"] = rec_decorators
                try:
                    rec_signature = inspect.Signature(
                        [
                            inspect.Parameter(
                                "self", inspect.Parameter.POSITIONAL_OR_KEYWORD
                            )
                        ]
                        + rec_params,
                        return_annotation=REDIS_RETURN_OVERRIDES.get(
                            method["name"], recommended_return[0]
                        )
                        if recommended_return
                        else None,
                    )
                    method_details["rec_signature"] = rec_signature
                except:
                    print(method["name"], rec_params)
                    raise Exception(
                        method["name"], [(k.name, k.kind) for k in rec_params]
                    )
            server_new_in = ""
            server_deprecated = ""
            recommended_replacement = ""
            method_details["deprecation_info"] = deprecation_info = is_deprecated(
                method, kls
            )
            method_details["return_summary"] = return_summary

            if deprecation_info:
                server_deprecated = f"☠️ Deprecated in redis: {deprecation_info[0]}."

                if deprecation_info[1]:
                    method_details[
                        "recommended_replacement"
                    ] = recommended_replacement = deprecation_info[1]
            if redis_version_introduced > MIN_SUPPORTED_VERSION:
                server_new_in = f"🎉 New in redis: {method['since']}"

            if located:
                version_added = VERSIONADDED_DOC.findall(located.__doc__)
                version_added = (version_added and version_added[0][0]) or ""
                version_added.strip()

                version_changed = VERSIONCHANGED_DOC.findall(located.__doc__)
                version_changed = (version_changed and version_changed[0][0]) or ""
                method_details["version_changed"] = version_changed
                method_details["version_added"] = version_added

                if not method["name"] in SKIP_SPEC:
                    cur = inspect.signature(located)
                    current_signature = [k for k in cur.parameters]
                    method_details["current_signature"] = cur
                    if (
                        compare_signatures(cur, rec_signature)
                        and cur.return_annotation != inspect._empty
                    ):
                        method_details["full_match"] = True
                    elif cur.parameters == rec_signature.parameters:
                        recommended_return = read_command_docs(method["name"])
                        recommendation = "- Missing return type."
                        if recommended_return:
                            new_sig = inspect.Signature(
                                [
                                    inspect.Parameter(
                                        "self", inspect.Parameter.POSITIONAL_OR_KEYWORD
                                    )
                                ]
                                + rec_params,
                                return_annotation=recommended_return[0],
                            )
                    else:
                        diff_minus = [
                            str(k)
                            for k, v in rec_signature.parameters.items()
                            if k not in current_signature
                        ]
                        diff_plus = [
                            str(k)
                            for k in current_signature
                            if k not in rec_signature.parameters
                        ]
                        method_details["diff_minus"] = diff_minus
                        method_details["diff_plus"] = diff_plus
                methods["supported"].append(method_details)
            elif not is_deprecated(method, kls):
                methods["missing"].append(method_details)
        if methods["supported"] or methods["missing"]:
            methods_by_group[group] = methods
    return section_template.render(
        section=section, groups=groups, methods_by_group=methods_by_group
    )


@click.group()
@click.option("--debug", default=False, help="Output debug")
@click.option("--next-version", default="6.6.6", help="Next version")
@click.pass_context
def code_gen(ctx, debug: bool, next_version: str):
    ctx.ensure_object(dict)
    ctx.obj["DEBUG"] = debug
    ctx.obj["NEXT_VERSION"] = next_version


@code_gen.command()
@click.option("--path", default="docs/source/compatibility.rst")
@click.pass_context
def coverage_doc(ctx, path: str):
    output = f"""
Command compatibility
=====================

This document is generated by parsing the `official redis command documentation <https://redis.io/commands>`_

"""

    # Strict Redis client
    kls = coredis.StrictRedis
    output += generate_compatibility_section(
        "Redis Client",
        kls,
        None,
        "redis.commands.core.CoreCommands",
        STD_GROUPS + ["server", "connection"],
        debug=ctx.obj["DEBUG"],
        next_version=ctx.obj["NEXT_VERSION"],
    )

    # Cluster client
    cluster_kls = coredis.StrictRedisCluster
    sync_cluster_kls = redis.cluster.RedisCluster
    output += generate_compatibility_section(
        "Redis Cluster Client",
        cluster_kls,
        kls,
        "redis.commands.cluster.RedisClusterCommands",
        STD_GROUPS + ["cluster"],
        debug=ctx.obj["DEBUG"],
        next_version=ctx.obj["NEXT_VERSION"],
    )
    open(path, "w").write(output)
    print(f"Generated coverage doc at {path}")


@code_gen.command()
@click.option("--path", default="coredis/tokens.py")
@click.pass_context
def token_enum(ctx, path):
    mapping = get_token_mapping()
    env = Environment()
    t = env.from_string(
        """

import enum

class PureToken(enum.Enum):
    '''
    Enum for using pure-tokens with the redis api.
    '''

    {% for token, command_usage in token_mapping.items() %}

    #: Used by:
    {%- for c in command_usage %}
    #:
    #:  - ``{{c}}``
    {%- endfor %}
    {{ token[0].upper() }} = "{{token[1]}}"
    {% endfor %}


    """
    )

    result = t.render(token_mapping=mapping)
    open(path, "w").write(result)
    print(f"Generated token enum at {path}")


@code_gen.command()
def generate_changes():
    cur_version = version.parse(coredis.__version__.split("+")[0])
    kls = coredis.StrictRedis
    cluster_kls = coredis.StrictRedisCluster
    new_methods = defaultdict(list)
    changed_methods = defaultdict(list)
    new_cluster_methods = defaultdict(list)
    changed_cluster_methods = defaultdict(list)
    for group in STD_GROUPS + ["server", "connection", "cluster"]:
        for cmd in get_official_commands(group):
            name = MAPPING.get(
                cmd["name"],
                cmd["name"].lower().replace(" ", "_").replace("-", "_"),
            )
            method = find_method(kls, name)
            cluster_method = find_method(cluster_kls, name)
            if method:
                vchanged = version_changed_from_doc(method.__doc__)
                vadded = version_added_from_doc(method.__doc__)
                if vadded and vadded > cur_version:
                    new_methods[group].append(method)
                if vchanged and vchanged > cur_version:
                    changed_methods[group].append(method)
            if cluster_method and method != cluster_method:
                vchanged = version_changed_from_doc(cluster_method.__doc__)
                vadded = version_added_from_doc(cluster_method.__doc__)
                if vadded and vadded > cur_version:
                    new_cluster_methods[group].append(cluster_method)
                if vchanged and vchanged > cur_version:
                    changed_cluster_methods[group].append(cluster_method)

    print("New APIs:")
    print()
    for group, methods in new_methods.items():
        print(f"    * {group.title()}:")
        print()
        for new_method in sorted(methods, key=lambda m: m.__name__):
            print(f"        * ``{kls.__name__}.{new_method.__name__}``")
        for new_method in sorted(
            new_cluster_methods.get(group, []), key=lambda m: m.__name__
        ):
            print(f"        * ``{cluster_kls.__name__}.{new_method.__name__}``")
        print()
    print()
    print("Changed APIs:")
    print()
    for group, methods in changed_methods.items():
        print(f"    * {group.title()}:")
        print()
        for changed_method in sorted(methods, key=lambda m: m.__name__):
            print(f"        * ``{kls.__name__}.{changed_method.__name__}``")
        for changed_method in sorted(
            changed_cluster_methods.get(group, []), key=lambda m: m.__name__
        ):
            print(f"        * ``{cluster_kls.__name__}.{changed_method.__name__}``")
        print()


if __name__ == "__main__":
    code_gen()
