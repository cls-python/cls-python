import luigi
from inhabitation_task import LuigiCombinator, ClsParameter, RepoMeta, InhabitationTask, TaskState, states
from fcl import FiniteCombinatoryLogic, Subtypes

import time


class WriteFileTask(luigi.Task, LuigiCombinator):

    def output(self):
        return luigi.LocalTarget('pure_hello_world.txt')

    def run(self):
        print("====== WriteFileTask: run")
        with self.output().open('w') as f:
            f.write("Hello World")


class SubstituteWeltTask(luigi.Task, LuigiCombinator):
    write_file_task = ClsParameter(tpe=WriteFileTask.return_type())

    def requires(self):
        return self.write_file_task()

    def output(self):
        return luigi.LocalTarget('pure_hello_welt.txt')

    def run(self):
        print("============= NameSubstituterTask: run")
        with self.input().open() as infile:
            text = infile.read()

        with self.output().open('w') as outfile:
            text = text.replace('World', "Welt")
            outfile.write(text)



class SubstituteNameTask(luigi.Task, LuigiCombinator):
    abstract = True
    write_file_task = ClsParameter(tpe=WriteFileTask.return_type())

    def requires(self):
        return self.write_file_task()


class SubstituteNameByAnneTask(SubstituteNameTask):
    abstract = False

    def output(self):
        return luigi.LocalTarget('pure_hello_anne.txt')

    def run(self):
        print("============= NameSubstituter: run")
        with self.input().open() as infile:
            text = infile.read()

        with self.output().open('w') as outfile:
            text = text.replace('World', "Anne")
            outfile.write(text)


class SubstituteNameByJanTask(luigi.Task, LuigiCombinator):
    abstract = False

    def output(self):
        return luigi.LocalTarget('pure_hello_jan.txt')

    def run(self):
        print("============= NameSubstituter: run")
        with self.input().open() as infile:
            text = infile.read()

        with self.output().open('w') as outfile:
            text = text.replace('World', "Jan")
            outfile.write(text)


class SubstituteNameByAnneTask2(luigi.Task, LuigiCombinator):
    write_file_task = ClsParameter(tpe=WriteFileTask.return_type())

    def requires(self):
        return self.write_file_task()

    def output(self):
        return luigi.LocalTarget('pure_hello_anne2.txt')

    def run(self):
        print("============= NameSubstituter: run")
        with self.input().open() as infile:
            text = infile.read()

        with self.output().open('w') as outfile:
            text = text.replace('World', "Anne2")
            outfile.write(text)


class SubstituteNameByJanTask2(luigi.Task, LuigiCombinator):
    write_file_task = ClsParameter(tpe=WriteFileTask.return_type())

    def requires(self):
        return self.write_file_task()

    def output(self):
        return luigi.LocalTarget('pure_hello_jan2.txt')

    def run(self):
        print("============= NameSubstituter: run")
        with self.input().open() as infile:
            text = infile.read()

        with self.output().open('w') as outfile:
            text = text.replace('World', "Jan2")
            outfile.write(text)


class SubstituteNameByParameterTask(luigi.Task, LuigiCombinator):
    write_file_task = ClsParameter(tpe=WriteFileTask.return_type())
    name = luigi.Parameter()

    def requires(self):
        return self.write_file_task()

    def output(self):
        return luigi.LocalTarget('pure_hello_{}.txt'.format(self.name))

    def run(self):
        print("============= NameSubstituter: run")
        with self.input().open() as infile:
            text = infile.read()

        with self.output().open('w') as outfile:
            text = text.replace('World', self.name)
            outfile.write(text)


class SubstituteNameConfigTask(luigi.Task, LuigiCombinator):
    x = ClsParameter(tpe={1: SubstituteNameByJanTask2.return_type(),
                                            2: SubstituteNameByAnneTask2.return_type()})
    config_domain = {1, 2}
    config_index = luigi.IntParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return self.x()

    def complete(self):
        return self.done

    def run(self):
        print(self.config_index)
        self.done = True


class FinalTask(luigi.Task, LuigiCombinator):
    z = ClsParameter(tpe={"1": SubstituteNameByParameterTask.return_type(),
                                            "2": SubstituteNameByParameterTask.return_type()})

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return [self.z("uiuiui") if self.config_index == "1" else self.z("oioioi")]

    def complete(self):
        return self.done

    def run(self):
        print("========= Final Task: run")
        self.done = True


if __name__ == '__main__':
    target = FinalTask.return_type()
    repository = RepoMeta.repository
    fcl = FiniteCombinatoryLogic(repository, Subtypes(RepoMeta.subtypes))
    task = InhabitationTask()
    states[task.task_id] = TaskState(fcl, target)
    luigi.build([task], worker_scheduler_factory=states[task.task_id].worker_scheduler_factory)
