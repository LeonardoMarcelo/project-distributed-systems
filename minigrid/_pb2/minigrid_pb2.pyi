from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STATE_UNSPECIFIED: _ClassVar[State]
    RECEIVED: _ClassVar[State]
    RUNNING: _ClassVar[State]
    CHECKPOINT: _ClassVar[State]
    MIGRATED: _ClassVar[State]
    DONE: _ClassVar[State]
    FAILED: _ClassVar[State]
STATE_UNSPECIFIED: State
RECEIVED: State
RUNNING: State
CHECKPOINT: State
MIGRATED: State
DONE: State
FAILED: State

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SubmitRequest(_message.Message):
    __slots__ = ("recipe", "code", "data", "params", "requires", "client_id")
    class ParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    RECIPE_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    REQUIRES_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    recipe: str
    code: str
    data: bytes
    params: _containers.ScalarMap[str, str]
    requires: _containers.RepeatedScalarFieldContainer[str]
    client_id: str
    def __init__(self, recipe: _Optional[str] = ..., code: _Optional[str] = ..., data: _Optional[bytes] = ..., params: _Optional[_Mapping[str, str]] = ..., requires: _Optional[_Iterable[str]] = ..., client_id: _Optional[str] = ...) -> None: ...

class SubmitReply(_message.Message):
    __slots__ = ("task_id",)
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    def __init__(self, task_id: _Optional[str] = ...) -> None: ...

class TaskId(_message.Message):
    __slots__ = ("task_id",)
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    def __init__(self, task_id: _Optional[str] = ...) -> None: ...

class TaskStatus(_message.Message):
    __slots__ = ("task_id", "state", "recipe", "worker_id", "arch", "virt", "step", "resumed_from_step", "migrations", "error", "submitted_at", "updated_at")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    RECIPE_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    ARCH_FIELD_NUMBER: _ClassVar[int]
    VIRT_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    RESUMED_FROM_STEP_FIELD_NUMBER: _ClassVar[int]
    MIGRATIONS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    SUBMITTED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    state: State
    recipe: str
    worker_id: str
    arch: str
    virt: str
    step: int
    resumed_from_step: int
    migrations: int
    error: str
    submitted_at: float
    updated_at: float
    def __init__(self, task_id: _Optional[str] = ..., state: _Optional[_Union[State, str]] = ..., recipe: _Optional[str] = ..., worker_id: _Optional[str] = ..., arch: _Optional[str] = ..., virt: _Optional[str] = ..., step: _Optional[int] = ..., resumed_from_step: _Optional[int] = ..., migrations: _Optional[int] = ..., error: _Optional[str] = ..., submitted_at: _Optional[float] = ..., updated_at: _Optional[float] = ...) -> None: ...

class TaskResult(_message.Message):
    __slots__ = ("task_id", "state", "result", "meta")
    class MetaEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    state: State
    result: bytes
    meta: _containers.ScalarMap[str, str]
    def __init__(self, task_id: _Optional[str] = ..., state: _Optional[_Union[State, str]] = ..., result: _Optional[bytes] = ..., meta: _Optional[_Mapping[str, str]] = ...) -> None: ...

class TaskEvent(_message.Message):
    __slots__ = ("task_id", "state", "kind", "worker_id", "arch", "virt", "step", "reason", "error", "at")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    ARCH_FIELD_NUMBER: _ClassVar[int]
    VIRT_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    AT_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    state: State
    kind: str
    worker_id: str
    arch: str
    virt: str
    step: int
    reason: str
    error: str
    at: float
    def __init__(self, task_id: _Optional[str] = ..., state: _Optional[_Union[State, str]] = ..., kind: _Optional[str] = ..., worker_id: _Optional[str] = ..., arch: _Optional[str] = ..., virt: _Optional[str] = ..., step: _Optional[int] = ..., reason: _Optional[str] = ..., error: _Optional[str] = ..., at: _Optional[float] = ...) -> None: ...

class Worker(_message.Message):
    __slots__ = ("id", "arch", "virt", "status", "capabilities", "tags")
    ID_FIELD_NUMBER: _ClassVar[int]
    ARCH_FIELD_NUMBER: _ClassVar[int]
    VIRT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    id: str
    arch: str
    virt: str
    status: str
    capabilities: _containers.RepeatedScalarFieldContainer[str]
    tags: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, id: _Optional[str] = ..., arch: _Optional[str] = ..., virt: _Optional[str] = ..., status: _Optional[str] = ..., capabilities: _Optional[_Iterable[str]] = ..., tags: _Optional[_Iterable[str]] = ...) -> None: ...

class WorkerList(_message.Message):
    __slots__ = ("workers", "leader")
    WORKERS_FIELD_NUMBER: _ClassVar[int]
    LEADER_FIELD_NUMBER: _ClassVar[int]
    workers: _containers.RepeatedCompositeFieldContainer[Worker]
    leader: str
    def __init__(self, workers: _Optional[_Iterable[_Union[Worker, _Mapping]]] = ..., leader: _Optional[str] = ...) -> None: ...
