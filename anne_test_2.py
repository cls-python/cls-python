import luigi

import inhabitation_task
from cls_python import FiniteCombinatoryLogic, Subtypes


class Test1(luigi.Task, inhabitation_task.LuigiCombinator):
    fu = luigi.IntParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return []

    def complete(self):
        return self.done

    def run(self):
        self.done = True


class Test3(Test1):
    abstract = False


class Test2(luigi.Task, inhabitation_task.LuigiCombinator):
    x = inhabitation_task.ClsParameter(tpe={1: Test1.return_type(), 2: Test3.return_type()})
    config_domain = {1, 2}
    config_index = luigi.IntParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return [self.x(3)]

    def complete(self):
        return self.done

    def run(self):
        print(self.config_index)
        self.done = True


if __name__ == '__main__':
    target = Test2.return_type(2)
    repository = inhabitation_task.RepoMeta.repository
    fcl = FiniteCombinatoryLogic(repository, Subtypes(inhabitation_task.RepoMeta.subtypes))
    task = inhabitation_task.InhabitationTask()
    inhabitation_task.states[task.task_id] = inhabitation_task.TaskState(fcl, target)
    luigi.build([task], worker_scheduler_factory=inhabitation_task.states[task.task_id].worker_scheduler_factory)