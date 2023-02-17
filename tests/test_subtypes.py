import unittest
from dataclasses import dataclass, field

from cls.subtypes import Subtypes
from cls.types import Arrow, Constructor, Intersection, Omega, Product, Type

Int = Constructor("Int")
a = Constructor("a")
b = Constructor("b")
c = Constructor("c")
d = Constructor("d")
e = Constructor("e")
f = Constructor("f")
x = Constructor("x")
y = Constructor("y")
z = Constructor("z")
loop = Constructor("loop")


class TestSubtypes(unittest.TestCase):

    only_int = Subtypes({Int.name: set()})
    abcd = Subtypes(
        {
            c.name: {a.name},
            d.name: {b.name},
            x.name: {b.name},
            y.name: {loop.name, x.name},
            z.name: {loop.name, x.name},
            loop.name: {y.name, z.name},
        }
    )

    def test_only_int(self):
        self.assertTrue(self.only_int.check_subtype(Int, Int))
        self.assertTrue(self.only_int.check_subtype(Arrow(Int, Int), Arrow(Int, Int)))

    def test_omega(self):
        self.assertTrue(
            self.abcd.check_subtype(Omega(), Intersection(Omega(), Omega()))
        )
        self.assertTrue(
            self.abcd.check_subtype(Intersection(Omega(), Omega()), Omega())
        )

        self.assertTrue(self.abcd.check_subtype(a, Omega()))
        self.assertTrue(self.abcd.check_subtype(b, Omega()))
        self.assertTrue(self.abcd.check_subtype(c, Omega()))
        self.assertTrue(self.abcd.check_subtype(Arrow(a, a), Omega()))
        self.assertTrue(self.abcd.check_subtype(Arrow(a, Omega()), Omega()))
        self.assertTrue(self.abcd.check_subtype(Omega(), Arrow(a, Omega())))
        self.assertTrue(
            self.abcd.check_subtype(Arrow(Omega(), Intersection(a, Omega())), Omega())
        )

        self.assertFalse(self.abcd.check_subtype(Omega(), a))
        self.assertFalse(self.abcd.check_subtype(Omega(), b))
        self.assertFalse(self.abcd.check_subtype(Omega(), c))
        self.assertFalse(self.abcd.check_subtype(Omega(), Arrow(a, a)))
        self.assertFalse(
            self.abcd.check_subtype(Omega(), Arrow(Omega(), Intersection(a, Omega())))
        )

        self.assertFalse(Omega().organized)
        self.assertFalse(Arrow(a, Omega()).organized)

        self.assertEqual(self.abcd.cast(Omega(), a), [Omega()])
        self.assertEqual(self.abcd.cast(Omega(), Arrow(a, b)), [Omega()])
        self.assertEqual(
            self.abcd.cast(Arrow(a, Arrow(b, Omega())), a), [(Omega(), Omega())]
        )
        self.assertEqual(
            self.abcd.cast(Arrow(a, Arrow(b, Omega())), Arrow(a, b)),
            [(Omega(), Omega())],
        )

    def test_constructors(self):
        self.assertTrue(self.abcd.check_subtype(c, a))
        self.assertTrue(self.abcd.check_subtype(d, b))

        self.assertTrue(
            self.abcd.check_subtype(
                Constructor(c.name, Product(Omega(), Omega())),
                Constructor(a.name, Product(Omega(), Omega())),
            )
        )
        self.assertTrue(
            self.abcd.check_subtype(
                Constructor(d.name, Product(Omega(), Omega())),
                Constructor(b.name, Product(Omega(), Omega())),
            )
        )

        self.assertTrue(
            self.abcd.check_subtype(
                Intersection(
                    Constructor(a.name, Product(d, Omega())),
                    Constructor(a.name, Product(Omega(), d)),
                ),
                Constructor(a.name, Product(b, d)),
            )
        )
        self.assertTrue(
            self.abcd.check_subtype(
                Constructor(a.name, Product(d, d)),
                Intersection(
                    Constructor(a.name, Product(d, Omega())),
                    Constructor(a.name, Product(Omega(), d)),
                ),
            )
        )

        self.assertEqual(
            Constructor(
                a.name, Product(b, Intersection(Constructor(d.name, e), e))
            ).organized,
            {
                Constructor(a.name, Product(b, Omega())),
                Constructor(a.name, Product(Omega(), Constructor(d.name, e))),
                Constructor(a.name, Product(Omega(), e)),
            },
        )

        self.assertTrue(
            self.abcd.check_subtype(
                Arrow(Arrow(Constructor(x.name, c), a), Intersection(e, f)),
                Arrow(Arrow(Constructor(b.name, a), c), f),
            )
        )

        self.assertTrue(self.abcd.check_subtype(x, b))
        self.assertTrue(self.abcd.check_subtype(y, x))
        self.assertTrue(self.abcd.check_subtype(y, b))
        self.assertTrue(self.abcd.check_subtype(y, y))
        self.assertTrue(self.abcd.check_subtype(y, z))
        self.assertTrue(self.abcd.check_subtype(y, loop))

        self.assertTrue(self.abcd.check_subtype(z, x))
        self.assertTrue(self.abcd.check_subtype(z, b))
        self.assertTrue(self.abcd.check_subtype(z, y))
        self.assertTrue(self.abcd.check_subtype(z, z))
        self.assertTrue(self.abcd.check_subtype(z, loop))

        self.assertTrue(self.abcd.check_subtype(loop, x))
        self.assertTrue(self.abcd.check_subtype(loop, b))
        self.assertTrue(self.abcd.check_subtype(loop, y))
        self.assertTrue(self.abcd.check_subtype(loop, z))
        self.assertTrue(self.abcd.check_subtype(loop, loop))

        self.assertFalse(self.abcd.check_subtype(b, x))
        self.assertFalse(self.abcd.check_subtype(b, y))
        self.assertFalse(self.abcd.check_subtype(b, z))
        self.assertFalse(self.abcd.check_subtype(b, loop))

        self.assertFalse(self.abcd.check_subtype(x, y))
        self.assertFalse(self.abcd.check_subtype(x, z))
        self.assertFalse(self.abcd.check_subtype(x, loop))

    def test_products(self):
        self.assertTrue(
            self.abcd.check_subtype(
                Product(Omega(), Omega()), Product(Omega(), Omega())
            )
        )

        self.assertTrue(
            self.abcd.check_subtype(
                Intersection(Product(a, Omega()), Product(b, c)),
                Product(Intersection(a, b), c),
            )
        )

        self.assertTrue(
            self.abcd.check_subtype(
                Intersection(Product(a, Omega()), Product(b, c)),
                Product(Intersection(a, b), c),
            )
        )

        self.assertEqual(
            Product(
                a, Product(Intersection(Product(e, e), f), Product(Omega(), Omega()))
            ).organized,
            {
                Product(a, Omega()),
                Product(Omega(), Product(Product(e, Omega()), Omega())),
                Product(Omega(), Product(Product(Omega(), e), Omega())),
                Product(Omega(), Product(f, Omega())),
                Product(Omega(), Product(Omega(), Product(Omega(), Omega()))),
            },
        )

        self.assertTrue(
            self.abcd.check_subtype(
                Arrow(Arrow(Product(d, c), a), Product(c, f)),
                Arrow(Arrow(Product(b, a), c), Product(a, f)),
            )
        )

    def test_arrows(self):
        self.assertTrue(self.abcd.check_subtype(Arrow(a, d), Arrow(c, b)))
        self.assertTrue(self.abcd.check_subtype(Arrow(a, d), Arrow(c, d)))

        self.assertTrue(
            self.abcd.check_subtype(
                Intersection(Arrow(a, c), Arrow(c, c)), Arrow(Intersection(a, b), c)
            )
        )

        self.assertTrue(
            self.abcd.check_subtype(
                Arrow(a, Arrow(b, Arrow(e, c))), Arrow(c, Arrow(d, Arrow(e, a)))
            )
        )
        self.assertTrue(
            self.abcd.check_subtype(Arrow(Arrow(c, b), c), Arrow(Arrow(a, d), a))
        )

        self.assertTrue(
            self.abcd.check_subtype(
                Intersection(
                    Product(a, b),
                    Intersection(
                        Intersection(Arrow(a, x), Product(y, Omega())),
                        Product(Omega(), x),
                    ),
                ),
                Intersection(Product(Intersection(a, y), x), Arrow(a, b)),
            )
        )

    def test_minimized_path_sets(self):
        original = [x, y, z, a, b, c, d, e]
        original_type = Omega()
        for t in reversed(original):
            original_type = Intersection(t, original_type)
        minimized = self.abcd.minimize(original_type.organized)
        self.assertTrue(
            self.abcd.check_subtype(original_type, Type.intersect(list(minimized)))
        )
        self.assertTrue(
            self.abcd.check_subtype(Type.intersect(list(minimized)), original_type)
        )
        for p in minimized:
            self.assertIsNotNone(p.path)
            for op in minimized:
                self.assertFalse(p != op and self.abcd.check_subtype(p, op))

    def test_fail(self):
        self.assertRaises(TypeError, self.abcd.check_subtype, x, X())
        self.assertRaises(TypeError, self.abcd.cast, X(), x)


@dataclass(frozen=True)
class X(Type):
    is_omega: bool = field(init=False, compare=False)
    size: bool = field(init=False, compare=False)
    organized: set[Type] = field(init=False, compare=False)
    path: tuple[list["Type"], "Type"] | None = field(init=False, compare=False)

    def __post_init__(self):
        super().__init__(
            is_omega=False,
            size=self._size(),
            organized=self._organized(),
            path=self._path(),
        )

    def _path(self) -> tuple[list["Type"], "Type"] | None:
        return [], self

    def _organized(self) -> set["Type"]:
        return {self}

    def _size(self) -> int:
        return 1

    def _is_omega(self) -> bool:
        return False

    def _str_prec(self, prec: int) -> str:
        return "X"


if __name__ == "__main__":
    unittest.main()
