import unittest
from collections import deque
from collections.abc import Callable

from cls.enumeration import Enumeration
from cls.fcl import Apply, Combinator, Failed, FiniteCombinatoryLogic, Rule, Tree
from cls.subtypes import Subtypes
from cls.types import Arrow, Constructor, Intersection, Omega, Product, Type


class Succ:
    def __call__(self, x: int) -> int:
        return 1 + x

    def __repr__(self):
        return "Succ()"

    def __str__(self):
        return "succ"

    def __eq__(self, other):
        return isinstance(other, Succ)

    def __hash__(self):
        return 999


class DummyHead:
    def __call__(self, x: list[int]) -> int:
        return x[0]

    def __repr__(self):
        return "DummyHead()"

    def __eq__(self, other):
        return isinstance(other, DummyHead)

    def __hash__(self):
        return 9999


class DummyAppend:
    def __call__(self, x: list[int]) -> Callable[[list[int]], list[int]]:
        return lambda y: x + y

    def __repr__(self):
        return "DummyAppend()"

    def __eq__(self, other):
        return isinstance(other, DummyAppend)

    def __hash__(self):
        return 99999


class StrApp:
    def __init__(self, x: int):
        self.x = x

    def __call__(self, x: str) -> Callable[[str], str]:
        return lambda r: x + r

    def __repr__(self):
        return "+"

    def __eq__(self, other):
        return isinstance(other, StrApp) and other.x == self.x

    def __hash__(self):
        return 23487 + 3 * self.x


even = Constructor("Even")
odd = Constructor("Odd")
integer = Constructor("Integer")
nat = Constructor("Nat")
negative = Constructor("Neg")
zero = Constructor("zero")
hello = Constructor("hello")
goodbye = Constructor("goodbye")
world = Constructor("world")
unclear = Constructor("unclear")
emphasis = Constructor("!")


def msg(t: Type):
    return Constructor("msg", t)


succ = Succ()
dummyHead = DummyHead()
dummyAppend = DummyAppend()
str_app0 = StrApp(0)
str_app1 = StrApp(1)
unary_nat_and_strings = {
    succ: Type.intersect(
        [
            Arrow(Intersection(nat, even), Intersection(nat, odd)),
            Arrow(Intersection(nat, odd), Intersection(nat, even)),
            # Rest is redundant / impossible / unnecessary, but triggers code branches in tests
            Arrow(integer, integer),
            Arrow(even, odd),
            Arrow(odd, even),
            Arrow(nat, Omega()),
            Arrow(Omega(), Omega()),
            Omega(),
            Intersection(Omega(), Intersection(Arrow(even, odd), Omega())),
            Arrow(Type.intersect([nat, integer, even]), Intersection(nat, odd)),
            Arrow(Type.intersect([nat, odd, even]), Type.intersect([nat, odd, even])),
            Arrow(Type.intersect([nat, odd, even]), Type.intersect([nat, odd])),
            Arrow(Type.intersect([nat, odd, even]), Type.intersect([nat, even])),
            Arrow(Type.intersect([nat, odd, even]), Constructor("List", integer)),
            Arrow(Constructor("List", integer), Constructor("List", nat)),
            Arrow(
                Omega(), Arrow(Constructor("List", integer), Constructor("List", nat))
            ),
            Arrow(
                Constructor("Seq", integer),
                Arrow(Constructor("Seq", nat), Type.intersect([nat, even, odd])),
            ),
        ]
    ),
    # Unused except for Omega
    dummyHead: Type.intersect(
        [
            Arrow(Constructor("List", nat), nat),
            Arrow(Constructor("List", even), even),
            Arrow(Constructor("List", odd), odd),
            Arrow(Constructor("List", integer), integer),
            Arrow(Constructor("List", negative), negative),
        ]
    ),
    # Unused except for Omega
    dummyAppend: Type.intersect(
        [
            Arrow(
                Constructor("List", nat),
                Arrow(Constructor("List", nat), Constructor("List", nat)),
            ),
            Arrow(
                Constructor("List", even),
                Arrow(Constructor("List", even), Constructor("List", even)),
            ),
            Arrow(
                Constructor("List", odd),
                Arrow(Constructor("List", odd), Constructor("List", odd)),
            ),
            Arrow(
                Constructor("List", integer),
                Arrow(Constructor("List", integer), Constructor("List", integer)),
            ),
            Arrow(
                Constructor("List", negative),
                Arrow(Constructor("List", negative), Constructor("List", negative)),
            ),
        ]
    ),
    0: Type.intersect([integer, nat, even, zero]),
    str_app0: Type.intersect(
        [
            Arrow(Intersection(hello, goodbye), Arrow(world, msg(unclear))),
            Arrow(hello, Arrow(world, msg(hello))),
            Arrow(goodbye, Arrow(world, msg(goodbye))),
            Arrow(msg(hello), Arrow(emphasis, msg(Product(hello, emphasis)))),
        ]
    ),
    str_app1: Arrow(
        msg(Product(hello, emphasis)),
        Arrow(emphasis, msg(Product(hello, Product(emphasis, emphasis)))),
    ),
    "hello ": hello,
    "goodbye ": goodbye,
    "world": world,
    "!": emphasis,
}
subtypes = {nat.name: {integer.name}}


def st_equals_rule(st: Subtypes, r1: Rule, r2: Rule) -> bool:
    def steq(t1, t2):
        return st.check_subtype(t1, t2) and st.check_subtype(t2, t1)

    match r1, r2:
        case Combinator(tgt1, c1), Combinator(tgt2, c2):
            return steq(tgt1, tgt2) and c1 == c2
        case Apply(tgt1, f1, arg1), Apply(tgt2, f2, arg2):
            return steq(tgt1, tgt2) and steq(f1, f2) and steq(arg1, arg2)
        case Failed(tgt1), Failed(tgt2):
            return steq(tgt1, tgt2)
        case _:
            raise TypeError(f"Unsuppored type in rules: {str(r1)}, {str(r2)}")


def st_equals_tree(st: Subtypes, t1: Tree, t2: Tree) -> bool:
    todo = deque([(t1, t2)])
    while todo:
        l, r = todo.popleft()
        if not st_equals_rule(st, l.rule, r.rule) or len(l.children) != len(r.children):
            return False
        todo.extendleft(zip(l.children, r.children))
    return True


class TestFCL(unittest.TestCase):
    def setUp(self) -> None:
        self.gamma = FiniteCombinatoryLogic(
            unary_nat_and_strings, Subtypes(subtypes), processes=1
        )

    def test_inhabit_nat(self):
        results = self.gamma.inhabit(nat)
        self.assertTrue(results)
        self.assertTrue(results.infinite)
        self.assertLess(results.size(), 0)
        self.assertIn(Combinator(nat, 0), results.rules)
        self.assertIn(
            Apply(nat, Arrow(Intersection(nat, even), nat), Intersection(nat, even)),
            results.rules,
        )
        self.assertIn("@(Nat & Even -> Nat, Nat & Even) : Nat", map(str, results.rules))
        self.assertIn(
            Combinator(Arrow(Intersection(nat, even), nat), succ), results.rules
        )
        self.assertEqual(Tree(Combinator(nat, 0), ()), results.raw[0])

        odd_even = Apply(
            Intersection(nat, even),
            Arrow(Intersection(nat, odd), Intersection(nat, even)),
            Intersection(nat, odd),
        )
        even_odd = Apply(
            Intersection(nat, odd),
            Arrow(Intersection(nat, even), Intersection(nat, odd)),
            Intersection(nat, even),
        )
        odd_nat = Apply(nat, Arrow(Intersection(nat, odd), nat), Intersection(nat, odd))
        self.assertTrue(
            st_equals_tree(
                self.gamma.subtypes,
                Tree(
                    odd_nat,
                    (
                        Tree(Combinator(odd_nat.function_type, succ)),  # 3 -> 4
                        Tree(
                            even_odd,
                            (
                                Tree(
                                    Combinator(even_odd.function_type, succ)
                                ),  # 2 -> 3
                                Tree(
                                    odd_even,
                                    (
                                        Tree(
                                            Combinator(odd_even.function_type, succ)
                                        ),  # 1 -> 2
                                        Tree(
                                            even_odd,
                                            (
                                                Tree(
                                                    Combinator(
                                                        even_odd.function_type, succ
                                                    )
                                                ),  # 0 -> 1
                                                Tree(
                                                    Combinator(
                                                        Intersection(nat, even), 0
                                                    )
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                results.raw[4],
            )
        )
        self.assertEqual("Combinator(Nat, 0)", str(results.raw[0]))
        self.assertEqual(
            "Combinator(Nat & Even -> Nat, succ)(Combinator(Nat & Even, 0))",
            str(results.raw[1]),
        )
        self.assertEqual(
            "Combinator(Nat & Odd -> Nat, succ)(Combinator(Nat & Even -> Nat & Odd, succ)(Combinator(Nat & Even, 0)))",
            str(results.raw[2]),
        )

        for x in range(0, 100):
            self.assertEqual(x, results.evaluated[x])

    def test_inhabit_not_present(self):
        results = self.gamma.inhabit(Constructor("List", nat))
        self.assertFalse(results)
        self.assertFalse(results.infinite)
        self.assertEqual(0, results.size())

        results = self.gamma.inhabit(Constructor("Nat", Constructor("Even")))
        self.assertFalse(results)
        self.assertFalse(results.infinite)
        self.assertEqual(0, results.size())

        results = self.gamma.inhabit(Type.intersect([nat, even, odd]))
        self.assertFalse(results)
        self.assertFalse(results.infinite)
        self.assertEqual(0, results.size())

        results = self.gamma.inhabit(
            Constructor("Seq", integer),
            Constructor("Seq", nat),
            Constructor("Seq", negative),
            Constructor("Seq", nat),
            Constructor("Seq", integer),
        )
        self.assertFalse(results)
        self.assertFalse(results.infinite)
        self.assertEqual(0, results.size())

    def test_omega(self):
        nat_omega = Arrow(nat, Omega())
        results = self.gamma.inhabit(Omega(), nat_omega)
        self.assertTrue(results)
        self.assertTrue(results.infinite)
        self.assertLess(results.size(), 0)

        self.assertIn(Combinator(Omega(), 0), results.rules)
        self.assertIn(Combinator(Omega(), succ), results.rules)
        self.assertIn(Apply(Omega(), Omega(), Omega()), results.rules)

        self.assertIn(Combinator(nat_omega, 0), results.rules)
        self.assertIn(Combinator(nat_omega, succ), results.rules)
        self.assertIn(Apply(nat_omega, nat_omega, nat_omega), results.rules)

    def test_even(self):
        results = self.gamma.inhabit(Intersection(nat, even))
        self.assertTrue(results)
        self.assertTrue(results.infinite)
        self.assertLess(results.size(), 0)
        for x in range(0, 100):
            self.assertEqual(x * 2, results.evaluated[x])

    def test_odd(self):
        results = self.gamma.inhabit(Intersection(nat, odd))
        self.assertTrue(results)
        self.assertTrue(results.infinite)
        self.assertLess(results.size(), 0)
        for x in range(0, 100):
            self.assertEqual(x * 2 + 1, results.evaluated[x])

    def test_even_odd(self):
        results = self.gamma.inhabit(Intersection(nat, even), Intersection(nat, odd))
        self.assertTrue(results)
        self.assertTrue(results.infinite)
        self.assertLess(results.size(), 0)

        even_odd = (Enumeration.ints() * Enumeration.ints()).map(
            lambda xy: [xy[0] * 2, xy[1] * 2 + 1]
        )
        for x in range(0, 100):
            self.assertEqual(even_odd[x], results.evaluated[x])

    def test_nothing(self):
        results = self.gamma.inhabit()
        self.assertFalse(results)
        self.assertFalse(results.infinite)
        self.assertEqual(0, results.size())

    def test_finite(self):
        results = self.gamma.inhabit(zero)
        self.assertTrue(results)
        self.assertFalse(results.infinite)
        self.assertEqual(1, results.size())
        self.assertEqual(0, results.evaluated[0])

    def test_invalid_rule(self):
        with self.assertRaises(TypeError):
            self.gamma._inhabitation_step(set(), deque([deque([Failed(Omega())])]))

        omega_tree = Tree(
            Failed(Omega()), (Tree(Failed(Omega()), ()), Tree(Failed(Omega()), ()))
        )
        with self.assertRaises(TypeError):
            omega_tree.evaluate()

        self.assertEqual(
            "Failed(omega) @ (Failed(omega) @ (), Failed(omega) @ ())", str(omega_tree)
        )

    def test_empty_step(self):
        self.assertFalse(self.gamma._inhabitation_step(set(), deque()))
        self.assertFalse(self.gamma._inhabitation_step(set(), deque([deque()])))

    def test_messages(self):
        result = self.gamma.inhabit(msg(Product(hello, emphasis)), msg(goodbye))
        self.assertTrue(result)
        self.assertEqual(1, result.size())
        self.assertEqual(["hello world!", "goodbye world"], result.evaluated[0])

        hello_part = result[msg(hello)]
        self.assertFalse(result.check_empty(msg(hello)))
        hello_evaluated = hello_part.map(lambda t: t.evaluate())
        self.assertEqual("hello world", hello_evaluated[0])

        emph_msg = result[msg(emphasis)]
        self.assertTrue(result.check_empty(msg(emphasis)))
        self.assertEqual(0, emph_msg.unsafe_max_size())

        result = self.gamma.inhabit(msg(Product(hello, Product(emphasis, emphasis))))
        self.assertTrue(result)
        self.assertEqual(1, result.size())
        self.assertEqual("hello world!!", result.evaluated[0])

        result = self.gamma.inhabit(msg(Constructor("insult")))
        self.assertFalse(result)

        result = self.gamma.inhabit(msg(Type.intersect([hello, goodbye, unclear])))
        self.assertFalse(result)

        result = self.gamma.inhabit(msg(Type.intersect([hello, nat])))
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
