import json

from .fcl import (
    Apply,
    Combinator,
    Failed,
    FiniteCombinatoryLogic,
    InhabitationResult,
    Tree,
)
from .subtypes import Subtypes
from .types import Arrow, Constructor, Intersection, Omega, Product


class CLSEncoder(json.JSONEncoder):
    """A custom JSON encoder for classes related to Finite Combinatory Logic.

    This class extends `json.JSONEncoder` and provides a custom implementation of the
    `default` method to handle objects of the classes `Tree`, `Combinator`, `Apply`,
    `Failed`, `Arrow`, `Intersection`, `Product`, `Omega`, `Constructor`, `Subtypes`,
    `InhabitationResult`, and `FiniteCombinatoryLogic`.

    The output of the encoding process is a dictionary with a special key `__type__` that
    indicates the type of the original object. The remaining keys in the dictionary store
    the attributes of the original object.
    """

    def __init__(self, **kwargs):
        """Initialize the JSON encoder.

        This method calls the `__init__` method of the parent class `json.JSONEncoder`
        with the same `kwargs` to properly initialize the object.
        """
        super().__init__(**kwargs)

    def combinator_hook(self, o):
        """Handle the encoding of `Combinator` objects.

        This method delegates the encoding of a `Combinator` object to the `default`
        method of `json.JSONEncoder`.

        :param o: The `Combinator` object to be encoded.
        :type o: Combinator
        :return: The encoded `Combinator` object.
        :rtype: dict
        """
        return json.JSONEncoder.default(self, o)

    def constructor_hook(self, o):
        """Handle the encoding of `Constructor` objects.

        This method delegates the encoding of a `Constructor` object to the `default`
        method of `json.JSONEncoder`.

        :param o: The `Constructor` object to be encoded.
        :type o: Constructor
        :return: The encoded `Constructor` object.
        :rtype: dict
        """

        return json.JSONEncoder.default(self, o)

    @staticmethod
    def tpe(o):
        """Return a string representation of the type of an object.

        This method returns a string of the form `module_name.class_name` for a given
        object.

        :param o: The object whose type is to be returned.
        :type o: object
        :return: A string representation of the type of `o`.
        :rtype: str
        """

        return f"{o.__class__.__module__}.{o.__class__.__qualname__}"

    def default(self, o):
        """Handle the encoding of objects.

        This method provides a custom implementation of the `default` method to handle
        objects of various types related to Finite Combinatory Logic. If the object is
        not one of the supported types, the method delegates the encoding to the `default`
        method of `json.JSONEncoder`.

        :param o: The object to be encoded.
        :type o: object
        :return: The encoded object.
        :rtype: dict
        """
        if isinstance(o, Tree):
            return {
                "__type__": CLSEncoder.tpe(o),
                "rule": self.default(o.rule),
                "children": [self.default(c) for c in o.children],
            }
        elif isinstance(o, Combinator):
            return {
                "__type__": CLSEncoder.tpe(o),
                "target": self.default(o.target),
                "combinator": self.combinator_hook(o.combinator),
            }
        elif isinstance(o, Apply):
            return {
                "__type__": CLSEncoder.tpe(o),
                "target": self.default(o.target),
                "function_type": self.default(o.function_type),
                "argument_type": self.default(o.argument_type),
            }
        elif isinstance(o, Failed):
            return {"__type__": CLSEncoder.tpe(o), "target": self.default(o.target)}
        elif isinstance(o, Arrow):
            return {
                "__type__": CLSEncoder.tpe(o),
                "source": self.default(o.source),
                "target": self.default(o.target),
            }
        elif isinstance(o, Intersection):
            return {
                "__type__": CLSEncoder.tpe(o),
                "left": self.default(o.left),
                "right": self.default(o.right),
            }
        elif isinstance(o, Product):
            return {
                "__type__": CLSEncoder.tpe(o),
                "left": self.default(o.left),
                "right": self.default(o.right),
            }
        elif isinstance(o, Omega):
            return {"__type__": CLSEncoder.tpe(o)}
        elif isinstance(o, Constructor):
            return {
                "__type__": CLSEncoder.tpe(o),
                "name": self.constructor_hook(o.name),
                "arg": self.default(o.arg),
            }
        elif isinstance(o, Subtypes):
            return {
                "__type__": CLSEncoder.tpe(o),
                "environment": {
                    self.constructor_hook(k): [self.constructor_hook(c) for c in v]
                    for k, v in o.environment.items()
                },
            }
        elif isinstance(o, InhabitationResult):
            return {
                "__type__": CLSEncoder.tpe(o),
                "targets": [t for t in self.default(o.targets)],
                "rules": [self.default(r) for r in o.rules],
            }
        elif isinstance(o, FiniteCombinatoryLogic):
            return {
                "__type__": CLSEncoder.tpe(o),
                "repository": {
                    self.combinator_hook(c): self.default(t) for c, t in o.repository
                },
                "subtypes": self.default(o.subtypes),
                "processes": self.default(o.processes),
            }
        else:
            return json.JSONEncoder.default(self, o)


class CLSDecoder(json.JSONDecoder):
    """A custom JSON decoder for decoding objects of different types from a JSON string related to Finite Combinatory Logic.

    The decoder uses the `__type__` field in the JSON string to determine the type of the object being decoded. The object is then constructed using the information in the JSON string.

    The decoder supports several different types of objects including `Tree`, `Combinator`, `Apply`, `Failed`, `Arrow`, `Intersection`, `Product`, `Omega`, `Constructor`, `Subtypes`, `InhabitationResult`, and `FiniteCombinatoryLogic`
    """

    def __init__(self, **kwargs):
        """Initializes the `CLSDecoder` class with the `object_hook` set to `self`.

        Any additional keyword arguments passed to the constructor will be passed to the parent `JSONDecoder` constructor.
        """
        super().__init__(object_hook=self, **kwargs)

    def combinator_hook(self, dct):
        """A hook for processing the `combinator` field in the JSON string.

        By default, the hook simply returns the `combinator` field without any processing.

        :param dct: The dictionary representing the JSON object being decoded.
        :type dct: dict
        :return: The `combinator` field.
        :rtype: dict
        """
        return dct

    def constructor_hook(self, dct):
        """Return the input dictionary `dct` unmodified.

        This method is a hook for `CLSDecoder` class to handle `Constructor` objects
        during JSON decoding. It simply returns the input dictionary `dct` unmodified.

        :param dct: A dictionary representing a JSON object.
        :type dct: dict
        :return: The input dictionary `dct` unmodified.
        :rtype: dict
        """
        return dct

    @staticmethod
    def tpe(cls):
        """A static method that returns a string representation of the class object.

        :return: A string representation of the class object, in the format `{module}.{qualname}`.
        :rtype: str
        """
        return f"{cls.__module__}.{cls.__qualname__}"

    def __call__(self, dct):
        """The `__call__` method of the `CLSDecoder` class is used to parse a dictionary, `dct`,
        into an object of one of several possible subclasses of the `CLS` class.

        The class of the returned object is determined by the `__type__` key in `dct`, which
        specifies the type of the object. The type is represented as a string of the form
        `module_name.class_name`. The method uses the `tpe` static method to convert a class
        object into such a string.

        The method uses several `if` statements to check the value of `dct["__type__"]` against
        the string representation of each possible `CLS` subclass, and returns an object of the
        corresponding class if a match is found. The constructor arguments of the returned object
        are extracted from `dct` and possibly processed by the `constructor_hook` and
        `combinator_hook` methods of the `CLSDecoder` class.

        If the value of `dct["__type__"]` does not match any of the expected values, the original
        dictionary `dct` is returned as is.


        :param dct: The dictionary to be parsed into an object of a `CLS` subclass.
        :type dct: dict
        :return: An object of a subclass of the `CLS` class, or the original `dct` if no match was found.
        :rtype: object
        """
        if "__type__" in dct:
            tpe = dct["__type__"]
            if tpe == CLSDecoder.tpe(Tree):
                return Tree(rule=dct["rule"], children=tuple(dct["children"]))
            elif tpe == CLSDecoder.tpe(Combinator):
                return Combinator(
                    target=dct["target"],
                    combinator=self.combinator_hook(dct["combinator"]),
                )
            elif tpe == CLSDecoder.tpe(Apply):
                return Apply(
                    target=dct["target"],
                    function_type=dct["function_type"],
                    argument_type=dct["argument_type"],
                )
            elif tpe == CLSDecoder.tpe(Failed):
                return Failed(target=dct["target"])
            elif tpe == CLSDecoder.tpe(Arrow):
                return Arrow(source=dct["source"], target=dct["target"])
            elif tpe == CLSDecoder.tpe(Intersection):
                return Intersection(left=dct["left"], right=dct["right"])
            elif tpe == CLSDecoder.tpe(Product):
                return Product(left=dct["left"], right=dct["right"])
            elif tpe == CLSDecoder.tpe(Omega):
                return Omega()
            elif tpe == CLSDecoder.tpe(Constructor):
                return Constructor(
                    name=self.constructor_hook(dct["name"]), arg=dct["arg"]
                )
            elif tpe == CLSDecoder.tpe(Subtypes):
                return Subtypes(
                    environment={
                        self.constructor_hook(k): {
                            self.constructor_hook(st) for st in v
                        }
                        for k, v in dct["environment"].items()
                    }
                )
            elif tpe == CLSDecoder.tpe(InhabitationResult):
                return InhabitationResult(
                    targets=dct["targets"], rules={r for r in dct["rules"]}
                )
            elif tpe == CLSDecoder.tpe(FiniteCombinatoryLogic):
                return FiniteCombinatoryLogic(
                    repository={
                        self.combinator_hook(k): v for k, v in dct["repository"].items()
                    },
                    subtypes=dct["subtypes"],
                    processes=dct["processes"],
                )
            else:
                return dct
        return dct
