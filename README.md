# cls-python



# cls-luigi

cls-luigi generates all feasible luigi pipelines for a given target based on a 
repository of luigi-tasks. 

A luigi tasks that not only inherit from ``luigi.Task`` but also 
from our LuigiCombinator ``inhabitation_task.LuigiCombinator`` are automatically considered as
luigi tasks that are part of the task repository.
All we need to implement is a pure luigi task, i.e., the methods ``run()``, ``output()`` and
``requires()``.

## Run a pipeline consisting of one task

The following task simply writes "Hello World" to a file ``pure_hello_world.txt``:

````python
import luigi
import inhabitation_task

class WriteFileTask(luigi.Task, inhabitation_task.LuigiCombinator):

    def output(self):
        print("WriteFileTask: output")
        return luigi.LocalTarget('pure_hello_world.txt')

    def run(self):
        print("====== WriteFileTask: run")
        with self.output().open('w') as f:
            f.write("Hello World")
````

If only this task exists, a pipeline simply consists of this task. 
However, we can run it as follows:

````python
import luigi
from anne_test_3 import WriteFileTask
from inhabitation_task import RepoMeta
from fcl import FiniteCombinatoryLogic, Subtypes

if __name__ == "__main__":
    target = WriteFileTask.return_type()
    repository = RepoMeta.repository
    fcl = FiniteCombinatoryLogic(repository, Subtypes(RepoMeta.subtypes))
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
````

We determine our WriteFileTask as target. 
The repository is automatically generated with all LuigiCombinator tasks.
With fcl.inhabit(target) we ask if an inhabitant for this target exists over this repo.
As the number of resulting variants can be infinite, we only store ``max_tasks_when_infinite`` 
number of inhabitants in the results lists.
All inhabitants (luigi pipelines) are scheduled.

## Define dependencies on other tasks

The following task depends on the WriteFileTask.
It reads the file written in the WriteFileTask and substitutes "world" by "welt" and writes
a new file.
To determine this dependency, we define a class variable write_file_class and return it
in the requires method.
The rest of this class is "pure luigi".

````python
class SubstituteWeltTask(luigi.Task, inhabitation_task.LuigiCombinator):
    write_file_task = inhabitation_task.ClsParameter(tpe=WriteFileTask.return_type())

    def requires(self):
        return self.write_file_task()

    def output(self):
        return luigi.LocalTarget('pure_hello_welt.txt')

    def run(self):
        print("============= NameSubstituter: run")
        with self.input().open() as infile:
            text = infile.read()

        with self.output().open('w') as outfile:
            text = text.replace('World', "Welt")
            outfile.write(text)
````

Let's now define our target as ``target = SubstituteWeltTask.return_type()`` in the scripting
before.
Then a pipeline with the two tasks is scheduled.

## Add variation points

To determine different pipelines, we have to add variation points.

### Using inheritance

Let's assume as an example that there exist two different variants for substituting the "world" 
string as variation point.
In this case, we define a task ``SubstituteNameTask`` as an abstract class, and propose two concrete 
implementations.

````python
class SubstituteNameTask(luigi.Task, inhabitation_task.LuigiCombinator):
    abstract = True
    
    def requires(self):
        return self.write_file_task()


class SubstituteNameByAnneTask(SubstituteNameTask):
    abstract = False
    write_file_task = inhabitation_task.ClsParameter(tpe=WriteFileTask.return_type())

    def output(self):
        return luigi.LocalTarget('pure_hello_anne.txt')

    def run(self):
        print("============= NameSubstituter: run")
        with self.input().open() as infile:
            text = infile.read()

        with self.output().open('w') as outfile:
            text = text.replace('World', "Anne")
            outfile.write(text)


class SubstituteNameByJanTask(SubstituteNameTask):
    abstract = False
    read_data_task = inhabitation_task.ClsParameter(tpe=WriteFileTask.return_type())

    def output(self):
        return luigi.LocalTarget('pure_hello_jan.txt')

    def run(self):
        print("============= NameSubstituter: run")
        with self.input().open() as infile:
            text = infile.read()

        with self.output().open('w') as outfile:
            text = text.replace('World', "Jan")
            outfile.write(text)
````
As target we use the abstract task ``target = SubstituteNameTask.return_type()``.

As a result three tasks are scheduled: SubstituteNameByAnneTask, SubstituteNameByJanTask and
WriteFileTask.
Note that the WriteFileTask is only scheduled once!

The output looks as follows:
````text
Scheduled 3 tasks of which:
* 3 ran successfully:
    - 1 SubstituteNameByAnneTask(config_index=, write_file_task={"__type__": "inhabitation_task.RepoMeta.WrappedTask", "module": "anne_test_3", "task_class": "WriteFileTask", "arguments": []})
    - 1 SubstituteNameByJanTask(config_index=, write_file_task={"__type__": "inhabitation_task.RepoMeta.WrappedTask", "module": "anne_test_3", "task_class": "WriteFileTask", "arguments": []})
    - 1 WriteFileTask(config_index=)

This progress looks :) because there were no failed tasks or missing dependencies
````

### Using different configurations




# Annes offene Fragen:
* Welche LuigiCombinators werden ins Repo aufgenommen und wie passiert das :)?
* Wie funktioniert ClsParameter? return_type() gibt ein Constructor zurück;



