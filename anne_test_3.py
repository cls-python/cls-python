import luigi
import inhabitation_task
from fcl import FiniteCombinatoryLogic, Subtypes

import time


class ReadDataTask(luigi.Task, inhabitation_task.LuigiCombinator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def output(self):
        print("ReadDataTask: output")
        return luigi.LocalTarget('pure_hello_world.txt')

    def run(self):
        print("====== ReadData: run")
        with self.output().open('w') as f:
            f.write("Hello World")


class SubstituteNameTask(luigi.Task, inhabitation_task.LuigiCombinator):
    abstract = True
    pass


class SubstituteNameByAnneTask(SubstituteNameTask):
    abstract = False
    read_data_task = inhabitation_task.ClsParameter(tpe=ReadDataTask.return_type())

    def requires(self):
        return self.read_data_task()

    def output(self):
        return luigi.LocalTarget('pure_hello_anne.txt')

    def run(self):
        print("============= NameSubstituter: run")
        with self.input().open() as infile:
            text = infile.read()

        with self.output().open('w') as outfile:
            text = text.replace('World', "Anne")
            outfile.write(text)
        time.sleep(3)


class SubstituteNameByJanTask(SubstituteNameTask):
    abstract = False
    read_data_task = inhabitation_task.ClsParameter(tpe=ReadDataTask.return_type())

    def requires(self):
        return self.read_data_task()

    def output(self):
        return luigi.LocalTarget('pure_hello_jan.txt')

    def run(self):
        print("============= NameSubstituter: run")
        with self.input().open() as infile:
            text = infile.read()

        with self.output().open('w') as outfile:
            text = text.replace('World', "Jan")
            outfile.write(text)
        time.sleep(3)


class FinalTask(luigi.Task, inhabitation_task.LuigiCombinator):
    substitute_name = inhabitation_task.ClsParameter(tpe=SubstituteNameTask.return_type())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return [self.substitute_name()]

    def complete(self):
        return self.done

    def run(self):
        print("========= Final Task: run")
        self.done = True


if __name__ == '__main__':
    target = FinalTask.return_type()
    repository = inhabitation_task.RepoMeta.repository
    fcl = FiniteCombinatoryLogic(repository, Subtypes(inhabitation_task.RepoMeta.subtypes))
    task = inhabitation_task.InhabitationTask()
    inhabitation_task.states[task.task_id] = inhabitation_task.TaskState(fcl, target)
    luigi.build([task], worker_scheduler_factory=inhabitation_task.states[task.task_id].worker_scheduler_factory)
