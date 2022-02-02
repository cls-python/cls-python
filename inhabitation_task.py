import abc
import datetime
import functools
import importlib
import json
from collections import deque

import cls_json
from typing import ClassVar, Any, TypeVar, Generic, Union
from typing import Type as PyType


from fcl import FiniteCombinatoryLogic, InhabitationResult, Tree, Combinator, Apply
from itypes import *
from subtypes import Subtypes
import luigi
from luigi.task_register import Register
from multiprocessing import Process

@dataclass
class TaskState(object):
    fcl: FiniteCombinatoryLogic = field(init=True)
    target: Type = field(init=True)
    result: InhabitationResult | None = field(init=False, default=None)
    position: int = field(init=False, default=-1)
    stopped: bool = field(init=False, default=False)
    processes: list[Process] = field(init=False, default_factory=lambda: [])
    worker_scheduler_factory: luigi.interface._WorkerSchedulerFactory = field(init=True, default_factory=luigi.interface._WorkerSchedulerFactory)


states: dict[str, TaskState] = dict()


class InhabitationTask(luigi.Task):
    accepts_messages = True

    def requires(self):
        return []

    def run(self):
        global states
        state = states[self.task_id]
        if state.result is None:
            state.result = state.fcl.inhabit(state.target)

        while True:
            if not self.scheduler_messages.empty():
                msg = self.scheduler_messages.get()
                content = msg.content
                if content == "stop":
                    msg.respond(f"Stopping: {self.task_id}")
                    for p in state.processes:
                        p.join()
                    state.stopped = True
                    break
                number = -1
                try:
                    number = int(content)
                except ValueError:
                    if content != "next":
                        msg.respond(f"Not understood: {content}")
                        continue
                state.position = number if number >= 0 else state.position + 1
                if not state.result.infinite and state.position >= state.result.size():
                    msg.respond(f"Inhabitant not present: {state.position}")
                    state.position = -1
                    continue
                next_task = state.result.evaluated[state.position]()
                try:
                    msg.respond(f"Running {state.position}")
                except BaseException as ex:
                    msg.respond(f"Error: {str(ex)}")
                    continue

                def p(next_task, factory):
                    luigi.build([next_task], worker_scheduler_factory=factory)
                task_process = Process(target=p, args=(next_task, state.worker_scheduler_factory))
                task_process.start()
                state.processes.append(task_process)

    def complete(self):
        return states[self.task_id].stopped


class InhabitationTest(luigi.Task):
    done = luigi.BoolParameter(default=False)
    id = luigi.IntParameter()

    accepts_messages = True

    def requires(self):
        return []

    def run(self):
        while True:
            if not self.scheduler_messages.empty():
                msg = self.scheduler_messages.get()
                break
        self.done = True

    def complete(self):
        return self.done




ConfigIndex = TypeVar("ConfigIndex")


class ClsParameter(luigi.Parameter, Generic[ConfigIndex]):

    @functools.cached_property
    def decoder(self):
        return CLSLuigiDecoder()

    @functools.cached_property
    def encoder(self):
        return CLSLugiEncoder()

    def __init__(self, tpe: Type | dict[ConfigIndex, Type], **kwargs):
        kwargs["positional"] = False
        super(ClsParameter, self).__init__(**kwargs)
        self.tpe = tpe

    def parse(self, serialized):
        return self.decoder.decode(serialized)

    def serialize(self, x):
        return self.encoder.encode(x)


class RepoMeta(Register):
    repository: dict[Any, Type] = {}
    subtypes: dict['RepoMeta.TaskCtor', set['RepoMeta.TaskCtor']] = {}

    @staticmethod
    def cls_tpe(cls):
        return f'{cls.__module__}.{cls.__qualname__}'

    @dataclass(frozen=True)
    class TaskCtor(object):
        tpe: PyType[Any] = field(init=True, compare=False)
        cls_tpe: str = field(init=False)

        def __post_init__(self):
            object.__setattr__(self, "cls_tpe", RepoMeta.cls_tpe(self.tpe))

        def __str__(self):
            return self.cls_tpe

    @dataclass(frozen=True)
    class ClassIndex(Generic[ConfigIndex]):
        tpe: PyType[Any] = field(init=True, compare=False)
        at_index: ConfigIndex = field(init=True)
        cls_tpe: str = field(init=False)

        def __post_init__(self):
            object.__setattr__(self, "cls_tpe", RepoMeta.cls_tpe(self.tpe))

        def __str__(self):
            return f"{self.cls_tpe}_{str(self.at_index)}"

    @dataclass(frozen=True)
    class WrappedTask(object):
        cls: PyType[Any] = field(init=True, compare=False)
        has_index: bool = field(init=True, compare=False)
        cls_params: tuple[Tuple[str, ClsParameter[Any]]] = field(init=True)
        reverse_arguments: tuple[Any] = field(init=True)
        name: str = field(init=False)

        def __post_init__(self):
            object.__setattr__(self, "name", RepoMeta.cls_tpe(self.cls))

        def __call__(self, *args, **kwargs) -> Union['RepoMeta.WrappedTask', 'LuigiCombinator[Any]']:
            expected = len(self.cls_params) + 1 if self.has_index else len(self.cls_params)
            if expected == len(self.reverse_arguments):
                cls_param_names = (name for name, _ in self.cls_params)
                all_params = ("config_index", *cls_param_names) if self.has_index else cls_param_names
                arg_dict = dict(zip(all_params, reversed(self.reverse_arguments)))
                return super(RepoMeta, self.cls).__call__(*args, **(kwargs | arg_dict))
            else:
                arg = args[0].at_index if self.has_index and not self.reverse_arguments else args[0]
                return RepoMeta.WrappedTask(self.cls, self.has_index, self.cls_params, (arg, *self.reverse_arguments))

        def __str__(self):
            return self.name

    def __init__(cls, name, bases, dct):
        super(RepoMeta, cls).__init__(name, bases, dct)
        # Make sure to skip LuigiCombinator base class
        if cls.__module__ == RepoMeta.__module__ and cls.__qualname__ == "LuigiCombinator":
            return

        # Update subtype information
        cls_tpe: str = RepoMeta.cls_tpe(cls)
        RepoMeta.subtypes[RepoMeta.TaskCtor(cls)] = \
            {RepoMeta.TaskCtor(b)
                for b in bases
                if issubclass(b, LuigiCombinator) and not issubclass(LuigiCombinator, b)}


        # Update repository
        cls_params = [(name, param) for name, param in cls.get_params() if isinstance(param, ClsParameter)]
        index_set = RepoMeta._index_set(cls_tpe, cls_params)
        combinator = RepoMeta.WrappedTask(cls, bool(index_set), tuple(cls_params), ())
        tpe = RepoMeta._combinator_tpe(cls, index_set, cls_params)
        if not cls.abstract:
            RepoMeta.repository[combinator] = tpe

        # Insert index combinators
        for idx in index_set:
            if not cls.abstract:
                RepoMeta.repository[RepoMeta.ClassIndex(cls, idx)] = Constructor(RepoMeta.ClassIndex(cls, idx))
            if not RepoMeta.ClassIndex(cls, idx) in RepoMeta.subtypes:
                RepoMeta.subtypes[RepoMeta.ClassIndex(cls, idx)] = set()
            for b in RepoMeta.subtypes[RepoMeta.TaskCtor(cls)]:
                if RepoMeta.ClassIndex(b.tpe, idx) in RepoMeta.subtypes:
                    RepoMeta.subtypes[RepoMeta.ClassIndex(cls, idx)].add(RepoMeta.ClassIndex(b.tpe, idx))

    @staticmethod
    def _combinator_tpe(cls: PyType[Any], index_set: set[Any], cls_params: list[Tuple[str, ClsParameter[Any]]]) -> Type:
        reverse_params = list(reversed(cls_params))
        if not index_set:
            tpe: Type = cls.return_type()
            for _, param in reverse_params:
                tpe = Arrow(param.tpe, tpe)
            return tpe
        else:
            def at_index(idx) -> Type:
                tpe: Type = cls.return_type(idx)
                for _, param in reverse_params:
                    if isinstance(param.tpe, Type):
                        tpe = Arrow(param.tpe, tpe)
                    else:
                        tpe = Arrow(param.tpe[idx], tpe)
                return Arrow(Constructor(RepoMeta.ClassIndex(cls, idx)), tpe)
            return Type.intersect(list(map(at_index, index_set)))

    @staticmethod
    def _index_set(cls_tpe: str, cls_params: list[Tuple[str, ClsParameter[Any]]]) -> set[Any]:
        index_set: set[Any] = set()
        for name, param in cls_params:
            if not index_set and isinstance(param.tpe, dict):
                index_set.update(param.tpe.keys())
                if not index_set:
                    raise IndexError(f"Error in parameter {cls_tpe}.{name}: cannot have empty index set")
            elif index_set and isinstance(param.tpe, dict):
                index_set.intersection_update(param.tpe.keys())
                if not index_set:
                    raise IndexError(f"Error in parameter {cls_tpe}.{name}: no index shared with all prior parameters")
        return index_set


class CLSLugiEncoder(cls_json.CLSEncoder):
    def __init__(self, **kwargs):
        super(CLSLugiEncoder, self).__init__(**kwargs)

    @staticmethod
    def _serialize_config_index(idx: RepoMeta.ClassIndex):
        return {"__type__": RepoMeta.cls_tpe(RepoMeta.ClassIndex),
                "module": idx.tpe.__module__,
                "task_class": idx.tpe.__qualname__,
                "index": idx.tpe.config_index.serialize(idx.at_index)}

    @staticmethod
    def _serialize_combinator(c: RepoMeta.WrappedTask):
        serialized_args = []
        args = list(c.reverse_arguments)
        if c.has_index and args:
            serialized_args.append({"config_index": c.cls.config_index.serialize(args.pop())})
        params = list(reversed(c.cls_params))
        while args:
            name, _ = params.pop()
            arg = args.pop()
            serialized_args.append({name: CLSLugiEncoder._serialize_combinator(arg)})

        return {"__type__": RepoMeta.cls_tpe(RepoMeta.WrappedTask),
                "module": c.cls.__module__,
                "task_class": c.cls.__qualname__,
                "arguments": serialized_args}

    @staticmethod
    def _serialize_task_ctor(ctor: RepoMeta.TaskCtor):
        return {"__type__": RepoMeta.cls_tpe(RepoMeta.TaskCtor),
                "module": ctor.tpe.__module__,
                "task_class": ctor.tpe.__qualname__}

    def combinator_hook(self, o):
        if isinstance(o, RepoMeta.WrappedTask):
            return CLSLugiEncoder._serialize_combinator(o)
        elif isinstance(o, RepoMeta.ClassIndex):
            return CLSLugiEncoder._serialize_config_index(o)
        else:
            return cls_json.CLSEncoder.combinator_hook(self, o)

    def constructor_hook(self, o):
        if isinstance(o, RepoMeta.TaskCtor):
            return CLSLugiEncoder._serialize_task_ctor(o)
        elif isinstance(o, RepoMeta.ClassIndex):
            return CLSLugiEncoder._serialize_config_index(o)
        else:
            return cls_json.CLSEncoder.constructor_hook(self, o)

    def default(self, o):
        if isinstance(o, RepoMeta.WrappedTask):
            return CLSLugiEncoder._serialize_combinator(o)
        else:
            return super(CLSLugiEncoder, self).default(o)


class CLSLuigiDecoder(cls_json.CLSDecoder):
    def __init__(self, **kwargs):
        super(CLSLuigiDecoder, self).__init__(**kwargs)

    @staticmethod
    def _deserialize_config_index(dct):
        module = importlib.import_module(dct["module"])
        task_class = getattr(module, dct["task_class"])
        return RepoMeta.ClassIndex(task_class, task_class.config_index.parse(dct["index"]))

    @staticmethod
    def _deserialize_combinator(dct):
        module = importlib.import_module(dct["module"])
        task_class = getattr(module, dct["task_class"])
        wrapped_task = None
        for c in RepoMeta.repository.keys():
            if isinstance(c, RepoMeta.WrappedTask) and c.name == RepoMeta.cls_tpe(task_class):
                wrapped_task = c
                break
        if not wrapped_task:
            raise RuntimeError(f"Cannot find WrappedTask for: {RepoMeta.cls_tpe(task_class)}")
        serialized_args = list(reversed(dct["arguments"]))
        if serialized_args and wrapped_task.has_index:
            arg = serialized_args.pop()
            wrapped_task = wrapped_task(wrapped_task.cls.config_index.parse(arg))
        while serialized_args:
            arg = CLSLuigiDecoder._deserialize_combinator(serialized_args.pop().values()[0])
            wrapped_task = wrapped_task(arg)
        return wrapped_task

    @staticmethod
    def _deserialize_task_ctor(dct):
        module = importlib.import_module(dct["module"])
        task_class = getattr(module, dct["task_class"])
        return RepoMeta.TaskCtor(task_class)

    def combinator_hook(self, dct):
        if "__type__" in dct:
            tpe = dct["__type__"]
            if tpe == RepoMeta.cls_tpe(RepoMeta.ClassIndex):
                return CLSLuigiDecoder._deserialize_config_index(dct)
            elif tpe == RepoMeta.cls_tpe(RepoMeta.WrappedTask):
                return CLSLuigiDecoder._deserialize_combinator(dct)
        return cls_json.CLSDecoder.combinator_hook(self, dct)

    def constructor_hook(self, dct):
        if "__type__" in dct:
            tpe = dct["__type__"]
            if tpe == RepoMeta.cls_tpe(RepoMeta.ClassIndex):
                return CLSLuigiDecoder._deserialize_config_index(dct)
            elif tpe == RepoMeta.cls_tpe(RepoMeta.TaskCtor):
                return CLSLuigiDecoder._deserialize_task_ctor(dct)
        return cls_json.CLSDecoder.combinator_hook(self, dct)

    def __call__(self, dct):
        if "__type__" in dct:
            tpe = dct["__type__"]
            if tpe == RepoMeta.cls_tpe(RepoMeta.WrappedTask):
                return CLSLuigiDecoder._deserialize_combinator(dct)
        return super(CLSLuigiDecoder, self).__call__(dct)



class LuigiCombinator(Generic[ConfigIndex], metaclass=RepoMeta):
    config_index = luigi.OptionalParameter(positional=False, default="")
    abstract: bool = False

    @classmethod
    def return_type(cls, idx: ConfigIndex = None) -> Type:
        if idx is None:
            return Constructor(RepoMeta.TaskCtor(cls))
        else:
            return Constructor(RepoMeta.TaskCtor(cls), Constructor(RepoMeta.ClassIndex(cls, idx)))


class Base1(luigi.Task, LuigiCombinator):

    def __init__(self, *args, **kwargs):
        super(Base1, self).__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return []

    def run(self):
        self.done = True

    def complete(self):
        return bool(self.done)


class Base2(luigi.Task, LuigiCombinator):
    p = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(Base2, self).__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return []

    def run(self):
        self.done = True

    def complete(self):
        return bool(self.done)

class Foo(luigi.Task, LuigiCombinator):
    x = ClsParameter(tpe=Base1.return_type())
    z = ClsParameter(tpe={"1": Base1.return_type(), "2": Base2.return_type()})

    def __init__(self, *args, **kwargs):
        super(Foo, self).__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return [self.x(), self.z() if self.config_index == "1" else self.z(p="base2")]

    def run(self):
        self.done = True

    def complete(self):
        return bool(self.done)


class Boo(luigi.Task, LuigiCombinator):
    y = ClsParameter(tpe={"1": Foo.return_type("1"), "2": Foo.return_type("2")})

    def __init__(self, *args, **kwargs):
        super(Boo, self).__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return [self.y()]

    def run(self):
        self.done = True

    def complete(self):
        return bool(self.done)


class Bar(Foo, Boo):
    def requires(self):
        return Foo.requires(self) + Boo.requires(self)

    def run(self):
        Foo.run(self)
        Boo.run(self)

class ABlubb(luigi.Task, LuigiCombinator):
    config_index = luigi.IntParameter(positional=False)
    x = ClsParameter(tpe={1: Base1.return_type(), 2: Base2.return_type()})
    abstract = True

class Blubb1(ABlubb):
    abstract = False

    def __init__(self, *args, **kwargs):
        super(Blubb1, self).__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return []

    def run(self):
        self.done = True

    def complete(self):
        return bool(self.done)

class Blubb2(ABlubb):
    abstract = False

    def __init__(self, *args, **kwargs):
        super(Blubb2, self).__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return []

    def run(self):
        self.done = True

    def complete(self):
        return bool(self.done)

# class ClsTask(object):
#     @dataclass(init=True, frozen=True)
#     class TaskInfo:
#         requires: Type = field(init=True)
#         cls: PyType[luigi.Task] = field(init=True)
#
#     known_tasks: ClassVar[set[TaskInfo]]
#
#     def __init__(self, *requirements: Type):
#         self.requirements = requirements
#
#     def __call__(self, cls: PyType[luigi.Task]):
#
#         @functools.wraps(cls)
#         def wrapper_singleton(*args, **kwargs):
#             if not wrapper_singleton.instance:
#                 wrapper_singleton.instance = cls(*args, **kwargs)
#             return wrapper_singleton.instance
#         wrapper_singleton.instance = None
#         return wrapper_singleton


from fcl import deep_str

if __name__ == '__main__':
    worker_scheduler_factory = luigi.interface._WorkerSchedulerFactory()
    #target = Constructor("Test")
    #repository = {InhabitationTest(id=0): target, InhabitationTest(id=1): target}
    #fcl = FiniteCombinatoryLogic(repository, Subtypes(dict()))
    target = Bar.return_type("1")
    repository = RepoMeta.repository
    print(deep_str(repository))
    fcl = FiniteCombinatoryLogic(repository, Subtypes(RepoMeta.subtypes))
    task = InhabitationTask()
    states[task.task_id] = TaskState(fcl, target, worker_scheduler_factory=worker_scheduler_factory)
    luigi.build([task], worker_scheduler_factory=states[task.task_id].worker_scheduler_factory)



