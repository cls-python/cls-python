import unittest

from cls.fcl import Apply, Combinator, Failed, FiniteCombinatoryLogic, Rule
from cls.subtypes import Subtypes
from cls.types import Arrow, Constructor

# This tests for an old subtle bug that once ocured in cls-scala.
repository = {
    "mkFail": Arrow(
        Constructor("Ok"),  # schedule targets Always, Ok
        Arrow(
            Constructor("Fail"),  # fail, schedule nothing
            Constructor(
                "PoisonOk"
            ),  # fail: remove Always from schedule, forget Ok in grammar, or grammar is poisoned
        ),
    ),
    "mkPoisonOk": Constructor("PoisonOk"),
    "mkDone": Arrow(
        Constructor("Ok"),  # schedule Always, Ok
        Arrow(Constructor("AlsoOk"), Constructor("Done")),
    ),
    "mkOk": Arrow(Constructor("Always"), Constructor("Ok")),
    "mkOkOk": Arrow(Constructor("Ok"), Constructor("Ok")),
    "mkAlsoOk": Constructor("AlsoOk"),
    "mkAlways": Constructor("Always"),
}


class TestFailureCache(unittest.TestCase):
    def setUp(self) -> None:
        self.gamma = FiniteCombinatoryLogic(repository, Subtypes({}), processes=1)

    def test_cache_poisoning(self):
        tgt1 = Constructor("PoisonOk")
        tgt2 = Constructor("Done")
        results = self.gamma.inhabit(tgt1, tgt2)

        expected_results: set[Rule] = {
            Combinator(Constructor("PoisonOk"), "mkPoisonOk"),
            Combinator(Constructor("Always"), "mkAlways"),
            Combinator(Constructor("AlsoOk"), "mkAlsoOk"),
            Apply(
                Constructor("Ok"),
                Arrow(Constructor("Always"), Constructor("Ok")),
                Constructor("Always"),
            ),
            Apply(
                Constructor("Ok"),
                Arrow(Constructor("Ok"), Constructor("Ok")),
                Constructor("Ok"),
            ),
            Combinator(Arrow(Constructor("Always"), Constructor("Ok")), "mkOk"),
            Combinator(Arrow(Constructor("Ok"), Constructor("Ok")), "mkOkOk"),
            Apply(
                Constructor("Done"),
                Arrow(Constructor("AlsoOk"), Constructor("Done")),
                Constructor("AlsoOk"),
            ),
            Apply(
                Arrow(Constructor("AlsoOk"), Constructor("Done")),
                Arrow(
                    Constructor("Ok"), Arrow(Constructor("AlsoOk"), Constructor("Done"))
                ),
                Constructor("Ok"),
            ),
            Combinator(
                Arrow(
                    Constructor("Ok"), Arrow(Constructor("AlsoOk"), Constructor("Done"))
                ),
                "mkDone",
            ),
            Failed(Constructor("Fail")),
            Combinator(
                Arrow(
                    Constructor("Ok"),
                    Arrow(Constructor("Fail"), Constructor("PoisonOk")),
                ),
                "mkFail",
            ),
            Apply(
                Arrow(Constructor("Fail"), Constructor("PoisonOk")),
                Arrow(
                    Constructor("Ok"),
                    Arrow(Constructor("Fail"), Constructor("PoisonOk")),
                ),
                Constructor("Ok"),
            ),
        }

        self.assertEqual(results.rules, expected_results)


if __name__ == "__main__":
    unittest.main()
