import datetime

from fcl import FiniteCombinatoryLogic, InhabitationResult
from itypes import *
from subtypes import Subtypes
import luigi
from multiprocessing import Process

@dataclass
class TaskState(object):
    worker_scheduler_factory: luigi.interface._WorkerSchedulerFactory = field(init=True)

    fcl: FiniteCombinatoryLogic = field(init=True)
    target: Type = field(init=True)
    result: InhabitationResult | None = field(init=False, default=None)
    position: int = field(init=False, default=-1)
    stopped: bool = field(init=False, default=False)
    processes: list[Process] = field(init=False, default_factory=lambda: [])


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
                try:
                    next_task = state.result.evaluated[state.position]
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


if __name__ == '__main__':
    worker_scheduler_factory = luigi.interface._WorkerSchedulerFactory()
    target = Constructor("Test")
    repository = {InhabitationTest(id=0): target, InhabitationTest(id=1): target}
    fcl = FiniteCombinatoryLogic(repository, Subtypes(dict()))
    task = InhabitationTask()
    states[task.task_id] = TaskState(worker_scheduler_factory, fcl, target)
    luigi.build([task], worker_scheduler_factory=states[task.task_id].worker_scheduler_factory)



