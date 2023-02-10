from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Type(ABC):
    """Abstract base class for a type.

    This class serves as an abstract base class for all types in the system. It provides the basic
    structure for all types, including properties and methods common to all types.

    :ivar is_omega: Indicates whether the type is an `Omega` type.
    :type is_omega: bool
    :ivar size: The size of the type.
    :type size: int
    :ivar organized: A set of `Type`s that are organized in this type.
    :type organized: set[Type]
    :ivar path: A tuple representing the path to a `Type` in this type.
    :type path: tuple[list[Type], Type] | None

    """

    is_omega: bool = field(init=True, kw_only=True, compare=False)
    size: int = field(init=True, kw_only=True, compare=False)
    organized: set["Type"] = field(init=True, kw_only=True, compare=False)
    path: tuple[list["Type"], "Type"] | None = field(
        init=True, kw_only=True, compare=False
    )

    def __str__(self) -> str:
        """Return a string representation of the type.

        :return: The string representation of the type.
        :rtype: str
        """
        return self._str_prec(0)

    def __mul__(self, other: "Type") -> "Type":
        """Return the product of two types.


        :param other: The other type to form the product with
        :type other: Type
        :return: The product of the two types.
        :rtype: Type
        """
        return Product(self, other)

    @abstractmethod
    def _path(self) -> tuple[list["Type"], "Type"] | None:
        """Return the path to the type.

        :return: The path to the type, represented as a list of types and the type itself.
        :rtype: tuple
        """

    @abstractmethod
    def _organized(self) -> set["Type"]:
        """Return the set of types that the type is organized into.

        :return: The set of types that the type is organized into.
        :rtype: set
        """

    @abstractmethod
    def _size(self) -> int:
        """Return the size of the type.

        :return: The size of the type.
        :rtype: int
        """

    @abstractmethod
    def _is_omega(self) -> bool:
        """Return a flag indicating whether the type is omega.

        :return: A flag indicating whether the type is omega.
        :rtype: bool
        """

    @abstractmethod
    def _str_prec(self, prec: int) -> str:
        """Return a string representation of the type with a given precedence.

        :param prec: The precedence to use when generating the string representation.
        :type prec: int
        :return: The string representation of the type with the given precedence.
        :rtype: str
        """

    @staticmethod
    def _parens(s: str) -> str:
        """Return a string with parentheses.

        :param s: The string to wrap in parentheses.
        :type s: str
        :return: The string with parentheses.
        :rtype: str
        """
        return f"({s})"

    @staticmethod
    def intersect(types: Sequence["Type"]) -> "Type":
        """Returns the intersection of the given sequence of types.

        :param types: The sequence of types to be intersected.
        :type types: Sequence["Type"]
        :return: The result of the intersection of the given types.
        :rtype: Type
        """
        if len(types) > 0:
            rtypes = reversed(types)
            result: "Type" = next(rtypes)
            for ty in rtypes:
                result = Intersection(ty, result)
            return result
        else:
            return Omega()

    def __getstate__(self):
        """Returns a pickleable state for the instance of the class.

        :return: The state of the class.
        :rtype: dict
        """
        state = self.__dict__.copy()
        del state["is_omega"]
        del state["size"]
        del state["organized"]
        del state["path"]
        return state

    def __setstate__(self, state):
        """Restores the state of the instance of the class.

        :param state: The state to be restored.
        :type state: dict
        """
        self.__dict__.update(state)
        self.__dict__["is_omega"] = self._is_omega()
        self.__dict__["size"] = self._size()
        self.__dict__["path"] = self._path()
        self.__dict__["organized"] = self._organized()


@dataclass(frozen=True)
class Omega(Type):
    """Represents the omega type in type theory. This class extends the abstract class `Type`."""

    is_omega: bool = field(init=False, compare=False)
    size: bool = field(init=False, compare=False)
    organized: set[Type] = field(init=False, compare=False)
    path: tuple[list["Type"], "Type"] | None = field(init=False, compare=False)

    def __post_init__(self):
        """Initialize the attributes of this type after object creation."""
        super().__init__(
            is_omega=self._is_omega(),
            size=self._size(),
            organized=self._organized(),
            path=self._path(),
        )

    def _is_omega(self) -> bool:
        return True

    def _size(self) -> int:
        return 1

    def _path(self) -> tuple[list["Type"], "Type"] | None:
        return None

    def _organized(self) -> set["Type"]:
        return set()

    def _str_prec(self, prec: int) -> str:
        return "omega"


@dataclass(frozen=True)
class Constructor(Type):
    """A type constructor that is used to build more complex types.

    :param name: The name of the constructor.
    :type name: object
    :param arg: The argument of the constructor.
    :type arg: Type
    :ivar is_omega: Indicates whether the type is an `Omega` type.
    :type is_omega: bool
    :ivar size: The size of the type.
    :type size: int
    :ivar organized: A set of `Type`s that are organized in this type.
    :type organized: set[Type]
    :ivar path: A tuple representing the path to a `Type` in this type.
    :type path: tuple[list[Type], Type] | None
    """

    name: object = field(init=True)
    arg: Type = field(default=Omega(), init=True)
    is_omega: bool = field(init=False, compare=False)
    size: int = field(init=False, compare=False)
    organized: set[Type] = field(init=False, compare=False)
    path: tuple[list["Type"], "Type"] | None = field(init=False, compare=False)

    def __post_init__(self):
        """Initialize the type after it has been created."""
        super().__init__(
            is_omega=self._is_omega(),
            size=self._size(),
            organized=self._organized(),
            path=self._path(),
        )

    def _is_omega(self) -> bool:
        return False

    def _size(self) -> int:
        return 1 + self.arg.size

    def _path(self) -> tuple[list["Type"], "Type"] | None:
        return ([], self) if self.arg.path or self.arg == Omega() else None

    def _organized(self) -> set["Type"]:
        return (
            {self}
            if self._path()
            else set(map(lambda ap: Constructor(self.name, ap), self.arg.organized))
        )

    def _str_prec(self, prec: int) -> str:
        if self.arg == Omega():
            return str(self.name)
        else:
            return f"{str(self.name)}({str(self.arg)})"


@dataclass(frozen=True)
class Product(Type):
    """Class representing the product type in the type theory.

    :param Type left: The left-hand side type.
    :param Type right: The right-hand side type.
    :param bool is_omega: Indicates whether this type is omega.
    :param int size: The size of this type.
    :param set[Type] organized: The organized forms of this type.
    :param tuple[list[Type], Type] | None path: The path to the root of the organized forms of this type.
    """

    left: Type = field(init=True)
    right: Type = field(init=True)
    is_omega: bool = field(init=False, compare=False)
    size: int = field(init=False, compare=False)
    organized: set[Type] = field(init=False, compare=False)
    path: tuple[list["Type"], "Type"] | None = field(init=False, compare=False)

    def __post_init__(self):
        """Initialize the Product type with the values of its attributes."""
        super().__init__(
            is_omega=self._is_omega(),
            size=self._size(),
            organized=self._organized(),
            path=self._path(),
        )

    def _is_omega(self) -> bool:
        return False

    def _size(self) -> int:
        return 1 + self.left.size + self.right.size

    def _path(self) -> tuple[list["Type"], "Type"] | None:
        return (
            ([], self)
            if (
                (self.left == Omega() and self.right == Omega())
                or (self.left.path and self.right == Omega())
                or (self.left == Omega() and self.right.path)
            )
            else None
        )

    def _organized(self) -> set["Type"]:
        if self._path():
            return {self}
        else:
            return set.union(
                set(map(lambda lp: Product(lp, Omega()), self.left.organized)),
                set(map(lambda rp: Product(Omega(), rp), self.right.organized)),
            )

    def _str_prec(self, prec: int) -> str:
        product_prec: int = 9

        def product_str_prec(other: Type) -> str:
            match other:
                case Product(_, _):
                    return other._str_prec(product_prec)
                case _:
                    return other._str_prec(product_prec + 1)

        result: str = (
            f"{product_str_prec(self.left)} * {self.right._str_prec(product_prec + 1)}"
        )
        return Type._parens(result) if prec > product_prec else result


@dataclass(frozen=True)
class Arrow(Type):
    """The `Arrow` class represents the type of a function from a source type to a target type.

    :param Type source: The source type.
    :param Type target: The target type.
    :param bool is_omega: Indicates whether this type is omega.
    :param int size: The size of this type.
    :param set[Type] organized: The organized forms of this type.
    :param tuple[list[Type], Type] | None path: The path to the root of the organized forms of this type.
    """

    source: Type = field(init=True)
    target: Type = field(init=True)
    is_omega: bool = field(init=False, compare=False)
    size: int = field(init=False, compare=False)
    organized: set[Type] = field(init=False, compare=False)
    path: tuple[list["Type"], "Type"] | None = field(init=False, compare=False)

    def __post_init__(self):
        """Initialize the Arrow class."""
        super().__init__(
            is_omega=self._is_omega(),
            size=self._size(),
            organized=self._organized(),
            path=self._path(),
        )

    def _is_omega(self) -> bool:
        return self.target.is_omega

    def _size(self) -> int:
        return 1 + self.source.size + self.target.size

    def _path(self) -> tuple[list["Type"], "Type"] | None:
        return (
            ([self.source, *(self.target.path[0])], self.target.path[1])
            if self.target.path
            else None
        )

    def _organized(self) -> set["Type"]:
        return (
            {self}
            if self._path()
            else set(map(lambda tp: Arrow(self.source, tp), self.target.organized))
        )

    def _str_prec(self, prec: int) -> str:
        arrow_prec: int = 8
        result: str
        match self.target:
            case Arrow(_, _):
                result = f"{self.source._str_prec(arrow_prec + 1)} -> {self.target._str_prec(arrow_prec)}"
            case _:
                result = f"{self.source._str_prec(arrow_prec + 1)} -> {self.target._str_prec(arrow_prec + 1)}"
        return Type._parens(result) if prec > arrow_prec else result


@dataclass(frozen=True)
class Intersection(Type):
    """A type representing the intersection of two other types.

    :param Type left: The left type in the intersection.
    :param Type right: The right type in the intersection.
    :param bool is_omega: Indicates if both `left` and `right` are equal to omega.
    :param int size: The number of distinct primitive types in the intersection.
    :param set[Type] organized: A set of primitive types in the intersection.
    :param tuple[list[Type], Type] | None path: A path to the root of the intersection, or None if there is no such path.
    """

    left: Type = field(init=True)
    right: Type = field(init=True)
    is_omega: bool = field(init=False, compare=False)
    size: int = field(init=False, compare=False)
    organized: set[Type] = field(init=False, compare=False)
    path: tuple[list["Type"], "Type"] | None = field(init=False, compare=False)

    def __post_init__(self):
        """Initialize the attributes of the intersection type."""
        super().__init__(
            is_omega=self._is_omega(),
            size=self._size(),
            organized=self._organized(),
            path=self._path(),
        )

    def _is_omega(self) -> bool:
        return self.left.is_omega and self.right.is_omega

    def _size(self) -> int:
        return 1 + self.left.size + self.right.size

    def _path(self) -> tuple[list["Type"], "Type"] | None:
        return None

    def _organized(self) -> set["Type"]:
        return set.union(self.left.organized, self.right.organized)

    def _str_prec(self, prec: int) -> str:
        intersection_prec: int = 10

        def intersection_str_prec(other: Type) -> str:
            match other:
                case Intersection(_, _):
                    return other._str_prec(intersection_prec)
                case _:
                    return other._str_prec(intersection_prec + 1)

        result: str = (
            f"{intersection_str_prec(self.left)} & {intersection_str_prec(self.right)}"
        )
        return Type._parens(result) if prec > intersection_prec else result
