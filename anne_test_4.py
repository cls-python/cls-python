import luigi
from anne_test_3 import WriteFileTask, FinalTask, SubstituteWeltTask, SubstituteNameTask, \
    SubstituteNameConfigTask, SubstituteNameByJanTask2
from inhabitation_task import RepoMeta
from fcl import FiniteCombinatoryLogic, Subtypes

if __name__ == "__main__":
    target = FinalTask.return_type() # hier könnte ich auch den Index wieder übergeben, z.B. return_type(2)
    repository = RepoMeta.repository
    fcl = FiniteCombinatoryLogic(repository, Subtypes(RepoMeta.subtypes))
    inhabitation_result = fcl.inhabit(target)
    max_tasks_when_infinite = 10
    actual = inhabitation_result.size()
    max_results = max_tasks_when_infinite
    if not actual is None or actual == 0:
        max_results = actual
    results = [t() for t in inhabitation_result.evaluated[0:max_results]]
    for r in results:
        print(r)
    if results:
        luigi.build(results, local_scheduler=True)  # für luigid: local_scheduler = True weglassen!
    else:
        print("No results!")
