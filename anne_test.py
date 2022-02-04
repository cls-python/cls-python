import luigi

import inhabitation_task
from fcl import FiniteCombinatoryLogic, Subtypes


class Test1(luigi.Task, inhabitation_task.LuigiCombinator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return []

    def complete(self):
        return self.done

    def run(self):
        self.done = True


class Test2(luigi.Task, inhabitation_task.LuigiCombinator):
    x = inhabitation_task.ClsParameter(tpe=Test1.return_type())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return [self.x()]

    def complete(self):
        return self.done

    def run(self):
        self.done = True


if __name__ == '__main__':
    target = Test2.return_type()
    repository = inhabitation_task.RepoMeta.repository
    fcl = FiniteCombinatoryLogic(repository, Subtypes(inhabitation_task.RepoMeta.subtypes))
    inhabitation_result = fcl.inhabit(target)
    max_tasks_when_infinite = 10
    actual = inhabitation_result.size()
    max_results = max_tasks_when_infinite
    if not actual is None or actual == 0:
        max_results = actual
    results = [t() for t in inhabitation_result.evaluated[0:max_results]]
    if results:
        luigi.build(results, local_scheduler=True) # für luigid: local_scheduler = True weglassen!
    else:
        print("No results!")

    #task = inhabitation_task.InhabitationTask()
    #inhabitation_task.states[task.task_id] = inhabitation_task.TaskState(fcl, target)
    #luigi.build([task], worker_scheduler_factory=inhabitation_task.states[task.task_id].worker_scheduler_factory)
