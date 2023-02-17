import unittest

from cls.types import Arrow, Constructor, Intersection, Omega, Product, Type

a = Constructor("a")
b = Constructor("b")
c = Constructor("c")

complicated = Type.intersect(
    [
        Intersection(a, b),
        c,
        Arrow(Intersection(a, c), b),
        Arrow(a, Intersection(c, b)),
        Product(Intersection(a, b), c),
        Product(a, Intersection(b, c)),
        Product(a, Product(b, c)),
        Product(Product(a, b), c),
        Arrow(Arrow(a, b), Arrow(c, a)),
        Constructor("List", a),
        Omega(),
    ]
)


class TestTypes(unittest.TestCase):
    def test_pretty_print(self):
        self.assertEqual(
            str(complicated),
            "a & b & c & (a & c -> b) & (a -> c & b) & (a & b * c) & (a * b & c) & (a * (b * c)) & (a * b * c) & ((a -> b) -> c -> a) & List(a) & omega",
        )

    def test_mul(self):
        self.assertEqual(a * b, Product(a, b))
        self.assertEqual(a * b * c, Product(Product(a, b), c))

    def test_state(self):
        s1 = Intersection(a, Arrow(b, c))
        s2 = Intersection(c, Arrow(a, b))
        x = s1.__getstate__()
        s2.__setstate__(x)
        self.assertEqual(s1, s2)


if __name__ == "__main__":
    unittest.main()
