from fcl import Arrow, Constructor, Product, FiniteCombinatoryLogic, Subtypes, deep_str, Intersection


class Left(object):
    def __call__(self, x):
        return f"{x} then go left"


class Right(object):
    def __call__(self, x):
        return f"{x} then go right"


class Up(object):
    def __call__(self, x):
        return f"{x} then go up"


class Down(object):
    def __call__(self, x):
        return f"{x} then go down"


if __name__ == "__main__":
    # Example from https://arxiv.org/pdf/1811.10815v1.pdf

    left = Left()
    down = Down()

    lab = {left: Intersection(
                        Arrow(Constructor("Pos", Product(Constructor("1"), Constructor("1"))),
                                    Constructor("Pos", Product(Constructor("0"), Constructor("1")))),
                        Arrow(Constructor("Pos", Product(Constructor("2"), Constructor("1"))),
                                    Constructor("Pos", Product(Constructor("1"), Constructor("1")))),
                        # Arrow(Constructor("Pos", Product(Constructor("1"), Constructor("3"))),
                        #             Constructor("Pos", Product(Constructor("0"), Constructor("3")))),
                        # Arrow(Constructor("Pos", Product(Constructor("2"), Constructor("3"))),
                        #             Constructor("Pos", Product(Constructor("1"), Constructor("3"))))
                    ),
           down: Arrow(Constructor("Pos", Product(Constructor("0"), Constructor("1"))),
                       Constructor("Pos", Product(Constructor("0"), Constructor("2")))
                       ),
           "start": Constructor("Pos", Product(Constructor("1"), Constructor("1")))}
    inhab = FiniteCombinatoryLogic(lab, Subtypes({}))
    result = inhab.inhabit(Constructor("Pos", Product(Constructor("0"), Constructor("2"))))
    if not result.infinite:
        print(deep_str([r for r in result.raw[
                                   0:result.size()]]))  # Bäume (deep_str weil print auf den listen elementen .repr statt .str aufruft)
        print(deep_str([i for i in result.evaluated[0:result.size()]]))
