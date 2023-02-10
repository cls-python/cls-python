from collections.abc import Callable, Iterator, Sequence
from typing import TypeAlias

from .types import Arrow, Constructor, Intersection, Omega, Product, Type

SubtypeInstruction: TypeAlias = Callable[
    [list[bool | Type], list["SubtypeInstruction"]], None
]


class Subtypes:
    """The `Subtypes` class performs subtype checking and casting in a type environment.

    :param environment: A dictionary mapping objects to sets of objects.
    :type environment: dict[object, set]
    """

    def __init__(self, environment: dict[object, set]):
        """Initialize the `Subtypes` class.

        The `environment` argument is used to initialize the `environment` instance variable.
        """
        self.environment = self._transitive_closure(
            self._reflexive_closure(environment)
        )

    def _tgt_for_srcs(
        self, gte: Type, inseq: Sequence[tuple[Type, Type]]
    ) -> Iterator[Type]:
        """Generator for targets for a given source.

        :param gte: A type to use for the comparison.
        :type gte: Type
        :param inseq: A sequence of tuples of types.
        :type inseq: Sequence[tuple[Type, Type]]
        :return: An iterator of types.
        :rtype: Iterator[Type]
        """
        for src, tgt in inseq:
            if self.check_subtype(gte, src):
                yield tgt

    def check_subtype(self, subtype: Type, supertype: Type) -> bool:
        """Check if `subtype` is a subtype of `supertype`.

        :param subtype: The subtype to check.
        :type subtype: Type
        :param supertype: The supertype to check.
        :type supertype: Type
        :return: True if `subtype` is a subtype of `supertype`, False otherwise.
        :rtype: bool
        """
        match supertype:
            case Omega():
                return True
            case Constructor(_, arg):
                casted = self.cast(supertype, subtype)
                return len(casted) > 0 and self.check_subtype(
                    Type.intersect(casted), arg
                )
            case Arrow(src, tgt):
                return tgt.is_omega or self.check_subtype(
                    Type.intersect(
                        list(self._tgt_for_srcs(src, self.cast(supertype, subtype)))
                    ),
                    tgt,
                )
            case Product(l, r):
                casted = self.cast(supertype, subtype)
                if casted:
                    (ls, rs) = tuple(zip(*casted))
                    return (
                        len(ls) > 0
                        and self.check_subtype(Type.intersect(ls), l)
                        and self.check_subtype(Type.intersect(rs), r)
                    )
                else:
                    return False
            case Intersection(l, r):
                return self.check_subtype(subtype, l) and self.check_subtype(subtype, r)
            case _:
                raise TypeError(f"Unsupported type: {supertype}")

    def cast(self, to: Type, castee: Type) -> Sequence:
        """Cast the given type `castee` to the type `to`.

        :param to: The target type to cast `castee` to.
        :type to: Type
        :param castee: The type to be cast.
        :type castee: Type
        :return: A sequence of pairs of types, representing the casting path from `castee` to `to`.
        :rtype: Sequence
        :raise TypeError: If `to` is unsupported.
        """
        match to:
            case Omega():
                return [to]
            case Arrow(_, tgt) if tgt.is_omega:
                return [(Omega(), Omega())]
            case Arrow(_, _):

                def cast_rec(other, delta):
                    match other:
                        case Arrow(osrc, otgt):
                            return [(osrc, otgt), *delta]
                        case Intersection(l, r):
                            return cast_rec(l, cast_rec(r, delta))
                        case _:
                            return delta

                return cast_rec(castee, [])
            case Constructor(name, _):

                def cast_rec(other, delta):
                    match other:
                        case Constructor(oname, oarg):
                            if name in self.environment.get(oname, {oname}):
                                return [oarg, *delta]
                            return delta
                        case Intersection(l, r):
                            return cast_rec(l, cast_rec(r, delta))
                        case _:
                            return delta

                return cast_rec(castee, [])
            case Product(_, _):

                def cast_rec(other, delta):
                    match other:
                        case Product(oleft, oright):
                            return [(oleft, oright), *delta]
                        case Intersection(l, r):
                            return cast_rec(l, cast_rec(r, delta))
                        case _:
                            return delta

                return cast_rec(castee, [])
            case _:
                raise TypeError(f"Unsupported type: {to}")

    @staticmethod
    def _reflexive_closure(env: dict[object, set]) -> dict[object, set]:
        """Compute the reflexive closure of a given subtype environment.

        :param env: The input subtype environment.
        :type env: dict[object, set]
        :return: The reflexive closure of the input subtype environment.
        :rtype: dict[object, set]
        """
        all_types: set[object] = set(env.keys())
        for v in env.values():
            all_types.update(v)
        result: dict[object, set] = {
            subtype: {subtype}.union(env.get(subtype, set())) for subtype in all_types
        }
        return result

    @staticmethod
    def _transitive_closure(env: dict[object, set]) -> dict[object, set]:
        """Compute the transitive closure of a given subtype environment.

        :param env: The input subtype environment.
        :type env: dict[object, set]
        :return: The transitive closure of the input subtype environment.
        :rtype: dict[object, set]
        """
        result: dict[object, set] = {
            subtype: supertypes.copy() for (subtype, supertypes) in env.items()
        }
        has_changed = True

        while has_changed:
            has_changed = False
            for (subtype, known_supertypes) in result.items():
                for supertype in known_supertypes.copy():
                    to_add: set = {
                        new_supertype
                        for new_supertype in result[supertype]
                        if new_supertype not in known_supertypes
                    }
                    if to_add:
                        has_changed = True
                    known_supertypes.update(to_add)

        return result

    def minimize(self, tys: set[Type]) -> set[Type]:
        """Minimize a set of types.

        :param tys: The set of types to be minimized.
        :type tys: set[Type]
        :return: The minimized set of types.
        :rtype: set[Type]
        """
        result: set[Type] = set()
        for ty in tys:
            if all(map(lambda ot: not self.check_subtype(ot, ty), result)):
                result = {ty, *(ot for ot in result if not self.check_subtype(ty, ot))}
        return result
