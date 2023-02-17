import unittest

from cls.fcl import Apply, Combinator, FiniteCombinatoryLogic, Tree
from cls.subtypes import Subtypes
from cls.types import Arrow, Constructor

A = Constructor("A")
B = Constructor("B")
C = Constructor("C")
D = Constructor("D")
E = Constructor("E")
F = Constructor("F")
G = Constructor("G")

repository = {
    "bac": Arrow(B, Arrow(A, C)),
    "dea": Arrow(D, Arrow(E, A)),
    "dea2": Arrow(D, Arrow(E, A)),
    "fgdb": Arrow(F, Arrow(G, Arrow(D, B))),
    "d": D,
    "e": E,
    "f": F,
    "f2": F,
    "g": G,
}


class TestParameterOrder(unittest.TestCase):
    def setUp(self) -> None:
        self.gamma = FiniteCombinatoryLogic(repository, Subtypes({}), processes=1)

    def test_parameter_order(self):
        results = self.gamma.inhabit(C)
        self.assertTrue(results.non_empty)
        self.assertFalse(results.infinite)
        self.assertEqual(results.size(), 4)

        bac = Tree(Combinator(Arrow(B, Arrow(A, C)), "bac"))
        dea = Tree(Combinator(Arrow(D, Arrow(E, A)), "dea"))
        dea2 = Tree(Combinator(Arrow(D, Arrow(E, A)), "dea2"))
        fgdb = Tree(Combinator(Arrow(F, Arrow(G, Arrow(D, B))), "fgdb"))
        d = Tree(Combinator(D, "d"))
        e = Tree(Combinator(E, "e"))
        f = Tree(Combinator(F, "f"))
        f2 = Tree(Combinator(F, "f2"))
        g = Tree(Combinator(G, "g"))

        ea = Tree(Apply(Arrow(E, A), Arrow(D, Arrow(E, A)), D), (dea, d))
        ea2 = Tree(Apply(Arrow(E, A), Arrow(D, Arrow(E, A)), D), (dea2, d))
        a = Tree(Apply(A, Arrow(E, A), E), (ea, e))
        a2 = Tree(Apply(A, Arrow(E, A), E), (ea2, e))

        gdb = Tree(
            Apply(Arrow(G, Arrow(D, B)), Arrow(F, Arrow(G, Arrow(D, B))), F), (fgdb, f)
        )
        db = Tree(Apply(Arrow(D, B), Arrow(G, Arrow(D, B)), G), (gdb, g))
        b = Tree(Apply(B, Arrow(D, B), D), (db, d))

        gdb2 = Tree(
            Apply(Arrow(G, Arrow(D, B)), Arrow(F, Arrow(G, Arrow(D, B))), F), (fgdb, f2)
        )
        db2 = Tree(Apply(Arrow(D, B), Arrow(G, Arrow(D, B)), G), (gdb2, g))
        b2 = Tree(Apply(B, Arrow(D, B), D), (db2, d))

        ac = Tree(Apply(Arrow(A, C), Arrow(B, Arrow(A, C)), B), (bac, b))
        c = Tree(Apply(C, Arrow(A, C), A), (ac, a))
        c2 = Tree(Apply(C, Arrow(A, C), A), (ac, a2))

        ac2 = Tree(Apply(Arrow(A, C), Arrow(B, Arrow(A, C)), B), (bac, b2))
        c3 = Tree(Apply(C, Arrow(A, C), A), (ac2, a))
        c4 = Tree(Apply(C, Arrow(A, C), A), (ac2, a2))

        got = set(results.raw[0:4])
        expected = {c, c2, c3, c4}
        self.assertEqual(got, expected)


if __name__ == "__main__":
    unittest.main()
