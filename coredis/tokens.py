import enum


class PureToken(enum.Enum):
    """
    Enum for using pure-tokens with the redis api.
    """

    #: Used by:
    #:
    #:  - ``ACL LOG``
    RESET = "RESET"

    #: Used by:
    #:
    #:  - ``BGSAVE``
    SCHEDULE = "SCHEDULE"

    #: Used by:
    #:
    #:  - ``BITCOUNT``
    #:
    #:  - ``BITPOS``
    BYTE = "BYTE"

    #: Used by:
    #:
    #:  - ``BITCOUNT``
    #:
    #:  - ``BITPOS``
    BIT = "BIT"

    #: Used by:
    #:
    #:  - ``BITFIELD``
    WRAP = "WRAP"

    #: Used by:
    #:
    #:  - ``BITFIELD``
    SAT = "SAT"

    #: Used by:
    #:
    #:  - ``BITFIELD``
    FAIL = "FAIL"

    #: Used by:
    #:
    #:  - ``BLMOVE``
    #:
    #:  - ``BLMOVE``
    #:
    #:  - ``BLMPOP``
    #:
    #:  - ``LMOVE``
    #:
    #:  - ``LMOVE``
    #:
    #:  - ``LMPOP``
    LEFT = "LEFT"

    #: Used by:
    #:
    #:  - ``BLMOVE``
    #:
    #:  - ``BLMOVE``
    #:
    #:  - ``BLMPOP``
    #:
    #:  - ``LMOVE``
    #:
    #:  - ``LMOVE``
    #:
    #:  - ``LMPOP``
    RIGHT = "RIGHT"

    #: Used by:
    #:
    #:  - ``BZMPOP``
    #:
    #:  - ``ZINTER``
    #:
    #:  - ``ZINTERSTORE``
    #:
    #:  - ``ZMPOP``
    #:
    #:  - ``ZUNION``
    #:
    #:  - ``ZUNIONSTORE``
    MIN = "MIN"

    #: Used by:
    #:
    #:  - ``BZMPOP``
    #:
    #:  - ``ZINTER``
    #:
    #:  - ``ZINTERSTORE``
    #:
    #:  - ``ZMPOP``
    #:
    #:  - ``ZUNION``
    #:
    #:  - ``ZUNIONSTORE``
    MAX = "MAX"

    #: Used by:
    #:
    #:  - ``CLIENT CACHING``
    #:
    #:  - ``SCRIPT DEBUG``
    YES = "YES"

    #: Used by:
    #:
    #:  - ``CLIENT CACHING``
    #:
    #:  - ``SCRIPT DEBUG``
    NO = "NO"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    #:
    #:  - ``CLIENT LIST``
    NORMAL = "NORMAL"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    #:
    #:  - ``CLIENT LIST``
    MASTER = "MASTER"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    SLAVE = "SLAVE"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    #:
    #:  - ``CLIENT LIST``
    REPLICA = "REPLICA"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    #:
    #:  - ``CLIENT LIST``
    PUBSUB = "PUBSUB"

    #: Used by:
    #:
    #:  - ``CLIENT NO-EVICT``
    #:
    #:  - ``CLIENT REPLY``
    #:
    #:  - ``CLIENT TRACKING``
    ON = "ON"

    #: Used by:
    #:
    #:  - ``CLIENT NO-EVICT``
    #:
    #:  - ``CLIENT REPLY``
    #:
    #:  - ``CLIENT TRACKING``
    OFF = "OFF"

    #: Used by:
    #:
    #:  - ``CLIENT PAUSE``
    WRITE = "WRITE"

    #: Used by:
    #:
    #:  - ``CLIENT PAUSE``
    ALL = "ALL"

    #: Used by:
    #:
    #:  - ``CLIENT REPLY``
    SKIP = "SKIP"

    #: Used by:
    #:
    #:  - ``CLIENT TRACKING``
    BCAST = "BCAST"

    #: Used by:
    #:
    #:  - ``CLIENT TRACKING``
    OPTIN = "OPTIN"

    #: Used by:
    #:
    #:  - ``CLIENT TRACKING``
    OPTOUT = "OPTOUT"

    #: Used by:
    #:
    #:  - ``CLIENT TRACKING``
    NOLOOP = "NOLOOP"

    #: Used by:
    #:
    #:  - ``CLIENT UNBLOCK``
    TIMEOUT = "TIMEOUT"

    #: Used by:
    #:
    #:  - ``CLIENT UNBLOCK``
    ERROR = "ERROR"

    #: Used by:
    #:
    #:  - ``CLUSTER FAILOVER``
    #:
    #:  - ``FAILOVER``
    #:
    #:  - ``SHUTDOWN``
    #:
    #:  - ``XCLAIM``
    FORCE = "FORCE"

    #: Used by:
    #:
    #:  - ``CLUSTER FAILOVER``
    TAKEOVER = "TAKEOVER"

    #: Used by:
    #:
    #:  - ``CLUSTER RESET``
    HARD = "HARD"

    #: Used by:
    #:
    #:  - ``CLUSTER RESET``
    SOFT = "SOFT"

    #: Used by:
    #:
    #:  - ``CLUSTER SETSLOT``
    STABLE = "STABLE"

    #: Used by:
    #:
    #:  - ``COPY``
    #:
    #:  - ``FUNCTION LOAD``
    #:
    #:  - ``FUNCTION RESTORE``
    #:
    #:  - ``MIGRATE``
    #:
    #:  - ``RESTORE``
    REPLACE = "REPLACE"

    #: Used by:
    #:
    #:  - ``EXPIRE``
    #:
    #:  - ``EXPIREAT``
    #:
    #:  - ``GEOADD``
    #:
    #:  - ``PEXPIRE``
    #:
    #:  - ``PEXPIREAT``
    #:
    #:  - ``SET``
    #:
    #:  - ``ZADD``
    NX = "NX"

    #: Used by:
    #:
    #:  - ``EXPIRE``
    #:
    #:  - ``EXPIREAT``
    #:
    #:  - ``GEOADD``
    #:
    #:  - ``PEXPIRE``
    #:
    #:  - ``PEXPIREAT``
    #:
    #:  - ``SET``
    #:
    #:  - ``ZADD``
    XX = "XX"

    #: Used by:
    #:
    #:  - ``EXPIRE``
    #:
    #:  - ``EXPIREAT``
    #:
    #:  - ``PEXPIRE``
    #:
    #:  - ``PEXPIREAT``
    #:
    #:  - ``ZADD``
    GT = "GT"

    #: Used by:
    #:
    #:  - ``EXPIRE``
    #:
    #:  - ``EXPIREAT``
    #:
    #:  - ``PEXPIRE``
    #:
    #:  - ``PEXPIREAT``
    #:
    #:  - ``ZADD``
    LT = "LT"

    #: Used by:
    #:
    #:  - ``FAILOVER``
    #:
    #:  - ``SHUTDOWN``
    ABORT = "ABORT"

    #: Used by:
    #:
    #:  - ``FLUSHALL``
    #:
    #:  - ``FLUSHDB``
    #:
    #:  - ``FUNCTION FLUSH``
    #:
    #:  - ``SCRIPT FLUSH``
    ASYNC = "ASYNC"

    #: Used by:
    #:
    #:  - ``FLUSHALL``
    #:
    #:  - ``FLUSHDB``
    #:
    #:  - ``FUNCTION FLUSH``
    #:
    #:  - ``SCRIPT DEBUG``
    #:
    #:  - ``SCRIPT FLUSH``
    SYNC = "SYNC"

    #: Used by:
    #:
    #:  - ``FUNCTION LIST``
    WITHCODE = "WITHCODE"

    #: Used by:
    #:
    #:  - ``FUNCTION RESTORE``
    FLUSH = "FLUSH"

    #: Used by:
    #:
    #:  - ``FUNCTION RESTORE``
    APPEND = "APPEND"

    #: Used by:
    #:
    #:  - ``GEOADD``
    #:
    #:  - ``ZADD``
    CHANGE = "CH"

    #: Used by:
    #:
    #:  - ``GEODIST``
    #:
    #:  - ``GEORADIUS``
    #:
    #:  - ``GEORADIUSBYMEMBER``
    #:
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:
    #:  - ``GEORADIUS_RO``
    #:
    #:  - ``GEOSEARCH``
    #:
    #:  - ``GEOSEARCH``
    #:
    #:  - ``GEOSEARCHSTORE``
    #:
    #:  - ``GEOSEARCHSTORE``
    M = "M"

    #: Used by:
    #:
    #:  - ``GEODIST``
    #:
    #:  - ``GEORADIUS``
    #:
    #:  - ``GEORADIUSBYMEMBER``
    #:
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:
    #:  - ``GEORADIUS_RO``
    #:
    #:  - ``GEOSEARCH``
    #:
    #:  - ``GEOSEARCH``
    #:
    #:  - ``GEOSEARCHSTORE``
    #:
    #:  - ``GEOSEARCHSTORE``
    KM = "KM"

    #: Used by:
    #:
    #:  - ``GEODIST``
    #:
    #:  - ``GEORADIUS``
    #:
    #:  - ``GEORADIUSBYMEMBER``
    #:
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:
    #:  - ``GEORADIUS_RO``
    #:
    #:  - ``GEOSEARCH``
    #:
    #:  - ``GEOSEARCH``
    #:
    #:  - ``GEOSEARCHSTORE``
    #:
    #:  - ``GEOSEARCHSTORE``
    FT = "FT"

    #: Used by:
    #:
    #:  - ``GEODIST``
    #:
    #:  - ``GEORADIUS``
    #:
    #:  - ``GEORADIUSBYMEMBER``
    #:
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:
    #:  - ``GEORADIUS_RO``
    #:
    #:  - ``GEOSEARCH``
    #:
    #:  - ``GEOSEARCH``
    #:
    #:  - ``GEOSEARCHSTORE``
    #:
    #:  - ``GEOSEARCHSTORE``
    MI = "MI"

    #: Used by:
    #:
    #:  - ``GEORADIUS``
    #:
    #:  - ``GEORADIUSBYMEMBER``
    #:
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:
    #:  - ``GEORADIUS_RO``
    #:
    #:  - ``GEOSEARCH``
    WITHCOORD = "WITHCOORD"

    #: Used by:
    #:
    #:  - ``GEORADIUS``
    #:
    #:  - ``GEORADIUSBYMEMBER``
    #:
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:
    #:  - ``GEORADIUS_RO``
    #:
    #:  - ``GEOSEARCH``
    WITHDIST = "WITHDIST"

    #: Used by:
    #:
    #:  - ``GEORADIUS``
    #:
    #:  - ``GEORADIUSBYMEMBER``
    #:
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:
    #:  - ``GEORADIUS_RO``
    #:
    #:  - ``GEOSEARCH``
    WITHHASH = "WITHHASH"

    #: Used by:
    #:
    #:  - ``GEORADIUS``
    #:
    #:  - ``GEORADIUSBYMEMBER``
    #:
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:
    #:  - ``GEORADIUS_RO``
    #:
    #:  - ``GEOSEARCH``
    #:
    #:  - ``GEOSEARCHSTORE``
    ANY = "ANY"

    #: Used by:
    #:
    #:  - ``GEORADIUS``
    #:
    #:  - ``GEORADIUSBYMEMBER``
    #:
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:
    #:  - ``GEORADIUS_RO``
    #:
    #:  - ``GEOSEARCH``
    #:
    #:  - ``GEOSEARCHSTORE``
    #:
    #:  - ``SORT``
    #:
    #:  - ``SORT_RO``
    ASC = "ASC"

    #: Used by:
    #:
    #:  - ``GEORADIUS``
    #:
    #:  - ``GEORADIUSBYMEMBER``
    #:
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:
    #:  - ``GEORADIUS_RO``
    #:
    #:  - ``GEOSEARCH``
    #:
    #:  - ``GEOSEARCHSTORE``
    #:
    #:  - ``SORT``
    #:
    #:  - ``SORT_RO``
    DESC = "DESC"

    #: Used by:
    #:
    #:  - ``GEOSEARCHSTORE``
    STOREDIST = "STOREDIST"

    #: Used by:
    #:
    #:  - ``GETEX``
    PERSIST = "PERSIST"

    #: Used by:
    #:
    #:  - ``HRANDFIELD``
    WITHVALUES = "WITHVALUES"

    #: Used by:
    #:
    #:  - ``LCS``
    LEN = "LEN"

    #: Used by:
    #:
    #:  - ``LCS``
    IDX = "IDX"

    #: Used by:
    #:
    #:  - ``LCS``
    WITHMATCHLEN = "WITHMATCHLEN"

    #: Used by:
    #:
    #:  - ``LINSERT``
    BEFORE = "BEFORE"

    #: Used by:
    #:
    #:  - ``LINSERT``
    AFTER = "AFTER"

    #: Used by:
    #:
    #:  - ``MIGRATE``
    EMPTY_STRING = ""

    #: Used by:
    #:
    #:  - ``MIGRATE``
    COPY = "COPY"

    #: Used by:
    #:
    #:  - ``RESTORE``
    ABSTTL = "ABSTTL"

    #: Used by:
    #:
    #:  - ``SET``
    KEEPTTL = "KEEPTTL"

    #: Used by:
    #:
    #:  - ``SET``
    GET = "GET"

    #: Used by:
    #:
    #:  - ``SHUTDOWN``
    NOSAVE = "NOSAVE"

    #: Used by:
    #:
    #:  - ``SHUTDOWN``
    SAVE = "SAVE"

    #: Used by:
    #:
    #:  - ``SHUTDOWN``
    NOW = "NOW"

    #: Used by:
    #:
    #:  - ``SORT``
    #:
    #:  - ``SORT_RO``
    SORTING = "ALPHA"

    #: Used by:
    #:
    #:  - ``XADD``
    NOMKSTREAM = "NOMKSTREAM"

    #: Used by:
    #:
    #:  - ``XADD``
    #:
    #:  - ``XTRIM``
    MAXLEN = "MAXLEN"

    #: Used by:
    #:
    #:  - ``XADD``
    #:
    #:  - ``XTRIM``
    MINID = "MINID"

    #: Used by:
    #:
    #:  - ``XADD``
    #:
    #:  - ``XTRIM``
    EQUAL = "="

    #: Used by:
    #:
    #:  - ``XADD``
    #:
    #:  - ``XTRIM``
    APPROXIMATELY = "~"

    #: Used by:
    #:
    #:  - ``XADD``
    AUTO_ID = "*"

    #: Used by:
    #:
    #:  - ``XAUTOCLAIM``
    #:
    #:  - ``XCLAIM``
    JUSTID = "JUSTID"

    #: Used by:
    #:
    #:  - ``XGROUP CREATE``
    #:
    #:  - ``XGROUP SETID``
    NEW_ID = "$"

    #: Used by:
    #:
    #:  - ``XGROUP CREATE``
    MKSTREAM = "MKSTREAM"

    #: Used by:
    #:
    #:  - ``XREADGROUP``
    NOACK = "NOACK"

    #: Used by:
    #:
    #:  - ``ZADD``
    INCREMENT = "INCR"

    #: Used by:
    #:
    #:  - ``ZDIFF``
    #:
    #:  - ``ZINTER``
    #:
    #:  - ``ZRANDMEMBER``
    #:
    #:  - ``ZRANGE``
    #:
    #:  - ``ZRANGEBYSCORE``
    #:
    #:  - ``ZREVRANGE``
    #:
    #:  - ``ZREVRANGEBYSCORE``
    #:
    #:  - ``ZUNION``
    WITHSCORES = "WITHSCORES"

    #: Used by:
    #:
    #:  - ``ZINTER``
    #:
    #:  - ``ZINTERSTORE``
    #:
    #:  - ``ZUNION``
    #:
    #:  - ``ZUNIONSTORE``
    SUM = "SUM"

    #: Used by:
    #:
    #:  - ``ZRANGE``
    #:
    #:  - ``ZRANGESTORE``
    BYSCORE = "BYSCORE"

    #: Used by:
    #:
    #:  - ``ZRANGE``
    #:
    #:  - ``ZRANGESTORE``
    BYLEX = "BYLEX"

    #: Used by:
    #:
    #:  - ``ZRANGE``
    #:
    #:  - ``ZRANGESTORE``
    REV = "REV"
