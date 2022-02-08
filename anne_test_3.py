import luigi
import inhabitation_task
from fcl import FiniteCombinatoryLogic, Subtypes

import time


class WriteFileTask(luigi.Task, inhabitation_task.LuigiCombinator):

    def output(self):
        return luigi.LocalTarget('pure_hello_world.txt')

    def run(self):
        print("====== WriteFileTask: run")
        with self.output().open('w') as f:
            f.write("Hello World")


class SubstituteWeltTask(luigi.Task, inhabitation_task.LuigiCombinator):
    write_file_task = inhabitation_task.ClsParameter(tpe=WriteFileTask.return_type())

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



class SubstituteNameTask(luigi.Task, inhabitation_task.LuigiCombinator):
    abstract = True
    write_file_task = inhabitation_task.ClsParameter(tpe=WriteFileTask.return_type())

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


class SubstituteNameByJanTask(luigi.Task, inhabitation_task.LuigiCombinator):
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


class SubstituteNameByAnneTask2(luigi.Task, inhabitation_task.LuigiCombinator):
    write_file_task = inhabitation_task.ClsParameter(tpe=WriteFileTask.return_type())

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


class SubstituteNameByJanTask2(luigi.Task, inhabitation_task.LuigiCombinator):
    write_file_task = inhabitation_task.ClsParameter(tpe=WriteFileTask.return_type())

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


class SubstituteNameByParameterTask(luigi.Task, inhabitation_task.LuigiCombinator):
    write_file_task = inhabitation_task.ClsParameter(tpe=WriteFileTask.return_type())
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


class SubstituteNameConfigTask(luigi.Task, inhabitation_task.LuigiCombinator):
    x = inhabitation_task.ClsParameter(tpe={1: SubstituteNameByJanTask2.return_type(),
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


class FinalTask(luigi.Task, inhabitation_task.LuigiCombinator):
    substitute_name = inhabitation_task.ClsParameter(tpe=SubstituteNameByParameterTask.return_type())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def requires(self):
        return [self.substitute_name("uiui")]

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
