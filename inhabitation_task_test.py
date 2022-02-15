import luigi
from inhabitation_task import LuigiCombinator, ClsParameter, RepoMeta, InhabitationTask, TaskState, states
from fcl import FiniteCombinatoryLogic, Subtypes

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
    # target = Constructor("Test")
    # repository = {InhabitationTest(id=0): target, InhabitationTest(id=1): target}
    # fcl = FiniteCombinatoryLogic(repository, Subtypes(dict()))
    target = Bar.return_type("1")
    repository = RepoMeta.repository
    print(deep_str(repository))
    fcl = FiniteCombinatoryLogic(repository, Subtypes(RepoMeta.subtypes))
    task = InhabitationTask()
    states[task.task_id] = TaskState(fcl, target, worker_scheduler_factory=worker_scheduler_factory)
    luigi.build([task], worker_scheduler_factory=states[task.task_id].worker_scheduler_factory)
