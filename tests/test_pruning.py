import unittest

from cls.fcl import Apply, Combinator, FiniteCombinatoryLogic, Tree
from cls.subtypes import Subtypes
from cls.types import Arrow, Constructor, Intersection

garbage_repo = {
    "f": Arrow(Constructor("Int"), Constructor("Goal")),
    "x": Constructor("Int"),
    "garbage1": Arrow(
        Constructor("Garbage1"),
        Intersection(Constructor("Int"), Constructor("Garbage2")),
    ),
    "garbage2": Arrow(Constructor("Garbage2"), Constructor("Garbage1")),
}


class TestPrune(unittest.TestCase):
    def setUp(self) -> None:
        self.gamma = FiniteCombinatoryLogic(garbage_repo, Subtypes({}), processes=1)

    def test_prune(self):
        tgt = Constructor("Goal")
        results = self.gamma.inhabit(tgt)

        for rule in results.rules:
            self.assertTrue(
                rule.target != Constructor("Goal")
                or rule
                == Apply(
                    Constructor("Goal"),
                    Arrow(Constructor("Int"), Constructor("Goal")),
                    Constructor("Int"),
                )
            )
            self.assertTrue(
                rule.target != Constructor("Int")
                or rule == Combinator(Constructor("Int"), "x")
            )

        self.assertEqual(1, results.size())
        self.assertEqual(
            Tree(
                Apply(
                    Constructor("Goal"),
                    Arrow(Constructor("Int"), Constructor("Goal")),
                    Constructor("Int"),
                ),
                (
                    Tree(
                        Combinator(Arrow(Constructor("Int"), Constructor("Goal")), "f")
                    ),
                    Tree(Combinator(Constructor("Int"), "x")),
                ),
            ),
            results.raw[0],
        )


if __name__ == "__main__":
    unittest.main()
