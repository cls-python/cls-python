import os
from abc import ABC
from collections import deque
from collections.abc import Callable, Iterator
from functools import cached_property, partial
from multiprocessing import Pool
from typing import Any, TypeAlias

from .enumeration import ComputationStep, EmptyStep, Enumeration
from .subtypes import Subtypes
from .types import Arrow, Intersection, Sequence, Type, dataclass, field

MultiArrow: TypeAlias = tuple[list[Type], Type]
State: TypeAlias = list["MultiArrow"]
CoverMachineInstruction: TypeAlias = Callable[
    [State], tuple[State, list["CoverMachineInstruction"]]
]


@dataclass(frozen=True)
class Rule(ABC):
    """Abstract base class for rules.

    A `Rule` is an abstract representation of a specific aspect of a type in a type system. It has two attributes:

    :param Type target: The target type the rule applies to.
    :param bool is_combinator: Whether the rule is a combinator.
    """

    target: Type = field(init=True, kw_only=True)
    is_combinator: bool = field(init=True, kw_only=True)


@dataclass(frozen=True)
class Failed(Rule):
    """A representation of a failed rule.

    A `Failed` rule represents a situation in which the application of the rule has failed.

    :param Type target: The target type the rule failed to apply to.
    :param bool is_combinator: indicates if this is a combinator.
    """

    target: Type = field()
    is_combinator: bool = field(default=False, init=False)

    def __str__(self):
        return f"Failed({str(self.target)})"


@dataclass(frozen=True)
class Combinator(Rule):
    """A representation of a combinator rule.

    A `Combinator` has two attributes:

    :param Type target: The target type.
    :param bool is_combinator: indicates if this is a combinator.
    :param object combinator: The combinator.
    """

    target: Type = field()
    is_combinator: bool = field(default=True, init=False)
    combinator: object = field(init=True)

    def __str__(self):
        return f"Combinator({str(self.target)}, {str(self.combinator)})"


@dataclass(frozen=True)
class Apply(Rule):
    """The `Apply` class represents a type inference rule that applies a function to its argument.

    :param Type target: The resulting type after applying the function.
    :param Type function_type: The type of the function being applied.
    :param Type argument_type: The type of the argument being passed to the function.
    """

    target: Type = field()
    is_combinator: bool = field(default=False, init=False)
    function_type: Type = field(init=True)
    argument_type: Type = field(init=True)

    def __str__(self):
        return (
            f"@({str(self.function_type)}, {str(self.argument_type)}) : {self.target}"
        )


@dataclass(frozen=True)
class Tree:
    """A class representing a tree of type rules.

    :param Rule rule: The root rule of the tree.
    :param tuple children: The children of the tree, represented as a tuple of Tree objects. Default value is an empty tuple.
    """

    rule: Rule = field(init=True)
    children: tuple["Tree", ...] = field(init=True, default_factory=lambda: ())

    class Evaluator(ComputationStep):
        """The `Evaluator` class implements a computation step that evaluates a `Tree` instance.

        :param Tree outer: The `Tree` instance being evaluated.
        :param list results: A list to store the result of the evaluation.
        """

        def __init__(self, outer: "Tree", results: list[Any]):
            self.outer: "Tree" = outer
            self.results = results

        def __iter__(self) -> Iterator[ComputationStep]:
            """
            Yields the next computation step to be performed.
            """
            match self.outer.rule:
                case Combinator(_, c):
                    self.results.append(c)
                case Apply(_, _, _):
                    f_arg = list()
                    yield Tree.Evaluator(self.outer.children[0], f_arg)
                    yield Tree.Evaluator(self.outer.children[1], f_arg)
                    self.results.append(partial(f_arg[0])(f_arg[1]))
                case _:
                    raise TypeError(f"Cannot apply rule: {self.outer.rule}")
            yield EmptyStep()

    def evaluate(self) -> Any:
        """
        Evaluates the `Tree` instance and returns the result.
        """
        result: list[Any] = []
        self.Evaluator(self, result).run()
        return result[0]

    def __str__(self):
        """
        Returns a string representation of the `Tree` instance.
        """
        match self.rule:
            case Combinator(_, _):
                return str(self.rule)
            case Apply(_, _, _):
                return f"{str(self.children[0])}({str(self.children[1])})"
            case _:
                return f"{str(self.rule)} @ ({', '.join(map(str, self.children))})"


@dataclass(frozen=True)
class InhabitationResult:
    """The `InhabitationResult` class is used to represent the inhabitation result, which is a process of finding terms that have a specific type. It stores a list of `Type` objects (`targets`) and a set of `Rule` objects (`rules`) that define the types and the terms.

    The class provides several properties and methods to work with the result.
    """

    targets: list[Type] = field(init=True)
    rules: set[Rule] = field(init=True)

    @cached_property
    def grouped_rules(self) -> dict[Type, set[Rule]]:
        """The `grouped_rules` property is a cached property that groups the rules based on their target type.

        :return: It returns a dictionary where the keys are the target types, and the values are the sets of rules
        that have the same target type.
        :rtype: dict[Type, set[Rule]]
        """
        result: dict[Type, set[Rule]] = dict()
        for rule in self.rules:
            group: set[Rule] = result.get(rule.target)
            if group:
                group.add(rule)
            else:
                result[rule.target] = {rule}
        return result

    def check_empty(self, target: Type) -> bool:
        """The `check_empty` method checks if a target type is in the inhabitation result.

        :param target: is the target to check.
        :type target: Type
        :return: `True` if there are no terms with the type, and `False` otherwise.
        :rtype: bool
        """
        for rule in self.grouped_rules.get(target, {Failed(target)}):
            if isinstance(rule, Failed):
                return True
        return False

    @cached_property
    def non_empty(self) -> bool:
        """This property shows if the inhabitation result has solutions for the targets.

        :return: `True` if there are any terms for the targets types, and `False` otherwise.
        :rtype: bool
        """
        for target in self.targets:
            if self.check_empty(target):
                return False
        return bool(self.targets)

    def __bool__(self) -> bool:
        return self.non_empty

    @cached_property
    def infinite(self) -> bool:
        """This property signals whether the result is infinite.

        :return: `True` if the result is infinite, meaning that the terms with the target types can generate more terms of the same type, and `False` otherwise.
        :rtype: bool
        """
        if not self:
            return False

        reachable: dict[Type, set[Type]] = dict()
        for (target, rules) in self.grouped_rules.items():
            entry: set[Type] = set()
            for rule in rules:
                match rule:
                    case Apply(target, lhs, rhs):
                        next_reached: set[Type] = {lhs, rhs}
                        entry.update(next_reached)
                    case _:
                        pass
            reachable[target] = entry

        changed: bool = True
        to_check: set[Type] = set(self.targets)
        while changed:
            changed = False
            next_to_check = set()
            for target in to_check:
                can_reach = reachable[target]
                if target in can_reach:
                    return True
                newly_reached = set().union(
                    *(reachable[reached] for reached in can_reach)
                )
                for new_target in newly_reached:
                    if target == new_target:
                        return True
                    elif new_target not in to_check:
                        changed = True
                        next_to_check.add(new_target)
                        can_reach.add(new_target)
                    elif new_target not in can_reach:
                        changed = True
                        can_reach.add(new_target)
            to_check.update(next_to_check)
        return False

    def size(self) -> int:
        """The `size` method returns the size of the result.

        :return: If the result is infinite, it returns -1. Otherwise, it returns the number of terms that have the target types.
        :rtype: int
        """
        if self.infinite:
            return -1
        maximum = self.raw.unsafe_max_size()
        size = 0
        values = self.raw.all_values()
        for i in range(0, maximum + 1):
            trees = next(values)
            size += len(trees)
        return size

    def __getitem__(self, target: Type) -> Enumeration[Tree]:
        """The `__getitem__` method is used to access the trees of type rules for a specific target type.
        It takes a `Type` object as an argument and returns an `Enumeration` object that contains trees of type rules.


        :param target: the target you want to access terms in the result.
        :type target: Type
        :return: a enumeration of type rule trees.
        :rtype: Enumeration[Tree]
        """
        if target in self.enumeration_map:
            return self.enumeration_map[target]
        else:
            return Enumeration.empty()

    @staticmethod
    def combinator_result(r: Combinator) -> Enumeration[Tree]:
        """The `combinator_result` method is a static methods that is used to create `Enumeration` objects for `Combinator` rules."""
        return Enumeration.singleton(Tree(r, ()))

    @staticmethod
    def apply_result(
        result: dict[Type, Enumeration[Tree]], r: Apply
    ) -> Enumeration[Tree]:
        """The `apply_result` method is a static methods that is used to create `Enumeration` objects for `Apply` rules."""

        def mkapp(left_and_right):
            return Tree(r, (left_and_right[0], left_and_right[1]))

        def apf():
            return (result[r.function_type] * result[r.argument_type]).map(mkapp).pay()

        applied = Enumeration.lazy(apf)
        return applied

    @cached_property
    def enumeration_map(self) -> dict[Type, Enumeration[Tree]]:
        """The `enumeration_map` property is a cached property that returns a dictionary where the keys are the target types,
        and the values are the `Enumeration` objects that contain the terms of the specific types.

        :return: a dictionary.
        :rtype: dict[Type, Enumeration[Tree]]
        """
        result: dict[Type, Enumeration[Tree]] = dict()
        for (target, rules) in self.grouped_rules.items():
            _enum: Enumeration[Tree] = Enumeration.empty()
            for rule in rules:
                match rule:
                    case Combinator(_, _) as r:
                        _enum = _enum + InhabitationResult.combinator_result(r)
                    case Apply(_, _, _) as r:
                        _enum = _enum + InhabitationResult.apply_result(result, r)
                    case _:
                        pass
            result[target] = _enum
        return result

    @cached_property
    def raw(self) -> Enumeration[Tree | list[Tree]]:
        """The `raw` property is a cached property that returns an `Enumeration` object
        that contains either a `Tree` or a list of `Tree` objects. The terms are either single terms or lists of terms.

        :return: Enumeration of resulting terms.
        :rtype: Enumeration[Tree | list[Tree]]
        """
        if not self:
            return Enumeration.empty()
        if len(self.targets) == 1:
            return self.enumeration_map[self.targets[0]]
        else:
            result: Enumeration[list[Tree]] = Enumeration.singleton([])
            for target in self.targets:
                result = (result * self.enumeration_map[target]).map(
                    lambda x: [*x[0], x[1]]
                )
            return result

    @cached_property
    def evaluated(self) -> Enumeration[Any | list[Any]]:
        """The `evaluated`property is a cached property that returns the evaluated result of the raw targets.

        :return: If the number of targets is 1, the raw target is evaluated and returned as a single value.
        Otherwise, the raw targets are evaluated and returned as a list of values.
        :rtype: Enumeration[Any | list[Any]]
        """
        if len(self.targets) == 1:
            return self.raw.map(lambda t: t.evaluate())
        else:
            return self.raw.map(lambda l: list(map(lambda t: t.evaluate(), l)))


class FiniteCombinatoryLogic:
    """A class to represent finite combinatory logic.

    repository : dict[object, Type]
        The repository of objects and their respective types.

    subtypes : Subtypes
        The subtypes of the objects in the repository.

    processes : int, optional
        The number of processes to use when splitting the repository.
        Defaults to the number of CPU cores.
    """

    def __init__(
        self,
        repository: dict[object, Type],
        subtypes: Subtypes,
        processes=os.cpu_count(),
    ):
        self.processes = processes

        self.repository = repository
        with Pool(processes) as pool:
            self.splitted_repository: dict[object, list[list[MultiArrow]]] = dict(
                pool.starmap(
                    FiniteCombinatoryLogic._split_repo,
                    self.repository.items(),
                    chunksize=max(len(self.repository) // processes, 10),
                )
            )
        self.subtypes = subtypes

    @staticmethod
    def _split_repo(c: object, ty: Type) -> tuple[object, list[list[MultiArrow]]]:
        return c, FiniteCombinatoryLogic.split_ty(ty)

    @staticmethod
    def split_ty(ty: Type) -> list[list[MultiArrow]]:
        def safe_split(
            xss: list[list[MultiArrow]],
        ) -> tuple[list[MultiArrow], list[list[MultiArrow]]]:
            return (xss[0] if xss else []), xss[1:]

        def split_rec(
            to_split: Type, srcs: list[Type], delta: list[list[MultiArrow]]
        ) -> list[list[MultiArrow]]:
            match to_split:
                case Arrow(src, tgt):
                    xs, xss = safe_split(delta)
                    next_srcs = [src, *srcs]
                    return [[(next_srcs, tgt), *xs], *split_rec(tgt, next_srcs, xss)]
                case Intersection(sigma, tau) if sigma.is_omega:
                    return split_rec(tau, srcs, delta)
                case Intersection(sigma, tau) if tau.is_omega:
                    return split_rec(sigma, srcs, delta)
                case Intersection(sigma, tau):
                    return split_rec(sigma, srcs, split_rec(tau, srcs, delta))
                case _:
                    return delta

        return [] if ty.is_omega else [[([], ty)], *split_rec(ty, [], [])]

    """
    ####################################################################################################################
    ####################################################################################################################
    ####################################################################################################################
    #                                                                                                                  #
    #                                           COVER MACHINE (AND HELPERS)                                            #
    #                                                                                                                  #
    ####################################################################################################################
    ####################################################################################################################
    ####################################################################################################################
    """

    def _dcap(self, sigma: Type, tau: Type) -> Type:
        """Given two types, checks if one is a subtype of the other. If yes,
        returns the more specific type, e.g. the subtype. If no, returns the
        intersection of the two.

        :param sigma: The first type to check.
        :param tau: The second type to check.
        :return: The resulting type (for merging that part of the MultiArrow).
        """
        if self.subtypes.check_subtype(sigma, tau):
            return sigma
        elif self.subtypes.check_subtype(tau, sigma):
            return tau
        else:
            return Intersection(sigma, tau)

    @staticmethod
    def _partition_cover(
        covered: set[Type], to_cover: set[Type]
    ) -> tuple[set[Type], set[Type]]:
        """This helper function partitions the elements of to_cover into two
        lists: If they are in covered, they get added to in_covered, if they
        aren't, they get added to not_in_covered. The idea is that elements
        that have already been covered are handled different then those that
        still need to be covered. This function computes exactly those sets.
        (p.45)

        :param covered: The set of types that are currently already
        covered. :param to_cover: The set of types that still needs to
        be covered. :return: The partitioned sets based on the
        membership in covered.
        """
        in_covered: set[Type] = set()
        not_in_covered: set[Type] = set()
        for ty in to_cover:
            if ty in covered:
                in_covered.add(ty)
            else:
                not_in_covered.add(ty)
        return in_covered, not_in_covered

    @staticmethod
    def _still_possible(
        splits: list[tuple[MultiArrow, set[Type]]], to_cover: set[Type]
    ) -> bool:
        """This helper function checks each element of to_cover with regard to
        if it still could be covered by the content of splits. This is very
        intuitive, we check for the existence of a MultiArrow whose target is
        the type to be covered, with the left part not mattering. (p.46)

        :param splits: The list of MultiArrows (rules if you will) that
        can be used to cover the types in to_cover.
        :param to_cover: The list of Types to check coverability.
        :return: False, if there is any type in to_cover that can not be
        produced by any MultiArrow in splits at all. Else, True. (All types
        could at this step still be covered)
        """
        for ty in to_cover:
            if not any(ty in covered for _, covered in splits):
                return False
        return True

    def _merge_multi_arrow(self, arrow1: MultiArrow, arrow2: MultiArrow) -> MultiArrow:
        """Merges two MultiArrows into a single MultiArrow with correct types.
        The MultiArrows have the same length. Each pair of types at the same
        spot is merged by _dcap, which selects the most specific type possible
        at that position (the subtype, or their intersection if there is no
        inheritance). (p.46)

        :param arrow1: The first MultiArrow.
        :param arrow2: The second MultiArrow.
        :return: The resulting MultiArrow from merging arrow1 and arrow2.
        """
        return list(map(self._dcap, arrow1[0], arrow2[0])), self._dcap(
            arrow1[1], arrow2[1]
        )

    def _check_cover(
        self, splits: list[tuple[MultiArrow, set[Type]]], to_cover: set[Type]
    ) -> CoverMachineInstruction:
        """Contains all the parts of the Step Relation where CheckCover is the
        current head. (p.46 f.)

        :param splits:
        :param to_cover:
        :return:
        """

        def instr(
            state: list[MultiArrow],
        ) -> tuple[State, list[CoverMachineInstruction]]:
            # STEP_CheckOk (p.46) (because still possible to cover)
            if FiniteCombinatoryLogic._still_possible(splits, to_cover):
                return state, [self._cover(splits, to_cover)]
            # STEP_CheckPrune (p.46) (because not still possible to cover)
            else:
                return state, []

        return instr

    def _check_continue_cover(
        self,
        splits: list[tuple[MultiArrow, set[Type]]],
        to_cover: set[Type],
        current_result: MultiArrow,
    ) -> CoverMachineInstruction:
        """Contains all the parts of the StepRelation where CheckContinueCover
        is the current head. (p.46 f.)

        :param splits:
        :param to_cover:
        :param current_result:
        :return:
        """

        def instr(
            state: list[MultiArrow],
        ) -> tuple[State, list[CoverMachineInstruction]]:
            # STEP_CheckContinueOk (p.46) (because still possible to continue)
            if FiniteCombinatoryLogic._still_possible(splits, to_cover):
                return state, [self._continue_cover(splits, to_cover, current_result)]

            else:
                # STEP_CheckContinuePrune (p.46) (because no longer possible)
                return state, []

        return instr

    def _continue_cover(
        self,
        splits: list[tuple[MultiArrow, set[Type]]],
        to_cover: set[Type],
        current_result: MultiArrow,
    ) -> CoverMachineInstruction:
        """Contains all the parts of the Step Relation where ContinueCover is
        the current head. (p.46 f.)

        :param splits:
        :param to_cover:
        :param current_result:
        :return:
        """

        def instr(
            state: list[MultiArrow],
        ) -> tuple[State, list[CoverMachineInstruction]]:
            # STEP_DoneContinue (p.47)
            if not splits:
                return state, []
            # The covered type is the one that the right of the MultiArrow specifies (rather intuitive)
            m, covered = splits[0]
            _splits = splits[1:]
            # Calculate the Guard
            freshly_covered, uncovered = FiniteCombinatoryLogic._partition_cover(
                covered, to_cover
            )
            # STEP_SkipContinue (p.47)
            if not freshly_covered:
                return state, [self._continue_cover(_splits, to_cover, current_result)]
            merged = self._merge_multi_arrow(current_result, m)
            # STEP_MergeDone (p.47) (uncovered is empty)
            if not uncovered:
                return [merged, *state], [
                    self._continue_cover(_splits, to_cover, current_result)
                ]
            # STEP_ContinueMergeAlways (p.47) (merge_multi_arrow and current_result match)
            elif merged[0] == current_result[0]:
                return state, [self._continue_cover(_splits, uncovered, merged)]
            # STEP_ContinueMergeOptions (p.47) (They don't match, and neither freshly_covered nor uncovered is empty)
            else:
                return state, [
                    self._continue_cover(_splits, uncovered, merged),
                    self._check_continue_cover(_splits, to_cover, current_result),
                ]

        return instr

    def _cover(
        self, splits: list[tuple[MultiArrow, set[Type]]], to_cover: set[Type]
    ) -> CoverMachineInstruction:
        """Contains all the parts of the Step Relation where Cover is the
        current head. (p.46 f.)

        :param splits:
        :param to_cover:
        :return:
        """

        def instr(
            state: list[MultiArrow],
        ) -> tuple[State, list[CoverMachineInstruction]]:
            # STEP_Done (p.46) (splits is empty)
            if not splits:
                return state, []
            # Compute guard again
            m, covered = splits[0]  # Parses tuple into separate vars
            _splits = splits[1:]
            freshly_covered, uncovered = FiniteCombinatoryLogic._partition_cover(
                covered, to_cover
            )
            # STEP_Skip (p.46) (because freshly_covered is an empty list)
            if not freshly_covered:
                return state, [self._cover(_splits, to_cover)]
            # STEP_AddDone (p.46) (because freshly_covered is not empty and uncovered is empty)
            elif not uncovered:
                return [m, *state], [self._check_cover(_splits, to_cover)]
            # STEP_Continue
            else:
                return state, [
                    self._continue_cover(_splits, uncovered, m),
                    self._check_cover(_splits, to_cover),
                ]

        return instr

    @staticmethod
    def _cover_machine(state: State, program: list[CoverMachineInstruction]) -> State:
        """Receives an initial state (cover) and a list of program
        instructions. These get processed in turn, by popping the head
        instruction. Each instruction itself is a callable, and the
        instructions implement the step relation. This function serves as a
        loop that keeps executing the instructions and manages the program
        stack and output state. (p.46 f.)

        :param state: A list of MultiArrows
        :param program: The stack of program instructions. Every Instruction
        is a callable and contains the actual step relation for the cover
        machine (p.46f.). The step relation is given across the functions _cover,
        _continue_cover, _check_continue_cover and _check_cover.
        :return: The output stack, which is a List of MultiArrows, which
        represents the computed cover.
        """
        instructions: deque[Iterator[CoverMachineInstruction]] = deque([iter(program)])
        while instructions:
            head = instructions.popleft()
            try:
                instruction = next(head)
                instructions.appendleft(head)
                state, next_instructions = instruction(state)
                instructions.appendleft(iter(next_instructions))
            except StopIteration:
                pass
        return state

    def _reduce_multi_arrows(self, ms: list[MultiArrow]) -> list[MultiArrow]:
        """This function eliminates all MultiArrows from the given set (cover)
        that are redundant when considering subtyping.

        :param ms: The List of MultiArrows that we want to reduce to a minimal necessary size.
        :return: The reduced List.
        """

        def check(lesser_arg_vect: MultiArrow, greater_arg_vect: MultiArrow) -> bool:
            """Checks if lesser as a MultiArrow is a subtype of greater as a
            MultiArrow. A MultiArrow is a subtype of another if it has the same
            length, and each index within the left part of the lesser
            MultiArrow (representing one Type) is a subtype of the greater
            MultiArrow. (p.64, code lines 5-6)

            :param lesser_arg_vect: The MultiArrow to check for being a subtype of greater_arg_vect.
            :param greater_arg_vect: The MultiArrow to check against.
            :return: True if lesser_arg_vect is a subtype of greater_arg_vect, else False.
            """
            (lesser_args, greater_args) = (lesser_arg_vect[0], greater_arg_vect[0])
            return len(lesser_args) == len(greater_args) and all(
                self.subtypes.check_subtype(lesser_arg, greater_arg)
                for (lesser_arg, greater_arg) in zip(lesser_args, greater_args)
            )

        def average_arguments_type_size(m: MultiArrow) -> int:
            """Metric to sort on.

            :param m:
            :return:
            """
            size: int = 0
            for ty in m[0]:
                size += ty.size
            return size / len(m[0]) if m[0] else 0

        result: list[MultiArrow] = []
        for multi_arrow in sorted(ms, key=average_arguments_type_size):
            # Check the current MultiArrow to potentially add against all MultiArrows currently in result.
            # If it is not a Subtype of any other MultiArrow in the result, add it to the Results.
            if all(not check(multi_arrow, in_result) for in_result in result):
                # Whenever adding a new MultiArrow to the result, check if any of the current MultiArrows in the result
                # are a subtype of the newly added arrow. Remove those from the result.
                # (Necessary, we know we are not a subtype of any previous arrows present, but we do not know if adding
                # the new arrow makes previously added ones unnecessary, by being a subtype of the new arrow.)
                # (Implemented in reverse by filtering)
                result = [
                    multi_arrow,
                    *(
                        in_result
                        for in_result in result
                        if not check(in_result, multi_arrow)
                    ),
                ]
        return result

    """
    ####################################################################################################################
    ####################################################################################################################
    ####################################################################################################################
    #                                                                                                                  #
    #                                     INHABITATION MACHINE (AND HELPERS)                                           #
    #                                                                                                                  #
    ####################################################################################################################
    ####################################################################################################################
    ####################################################################################################################
    """

    def _compute_fail_existing(
        self, rules: set[Rule], target: Type
    ) -> tuple[bool, bool]:
        """Computes if the target or a supertype of it have already failed/been
        uninhabited [1]. Also computes if the target has already been a target
        in the set of rules or has already failed [2]. A supertype being
        uninhabited also proves that all of its subtypes are uninhabitable.

        If a Failed(target) is found, that means it was failed and existing.
        If a Failed(supertype of target) is found, that means it failed, but we must check for existence.
        If neither was the case, we know it was not failed so far, but must still check for existence.
        The last case is the remaining combination.

        :param rules: This is the set of stable rules, referred to as G_stable in the algorithm.
        :param target: This is the type to compute for.
        :return: Tuple of Booleans, consisting of ([1], [2]) / (failed, existing).
        """
        rest_of_rules: Iterator[Rule] = iter(rules)
        while to_check := next(rest_of_rules, None):
            match to_check:
                case Failed(t) if t == target:
                    return True, True
                case Failed(t) if self.subtypes.check_subtype(target, t):
                    return True, Failed(target) in rest_of_rules
                case Apply(_, _, arg) if arg == target:
                    return False, True
                case _:
                    continue
        return False, False

    @staticmethod
    def _commit_multi_arrow(combinator: object, m: MultiArrow) -> deque[Rule]:
        """Converts a MultiArrow into a set of rules that the inhabitation
        machine can process. This is done by expanding the MultiArrow to nested
        Arrows and creating the corresponding "Apply" rules. Finally, after all
        Apply rules the combinator entails have been computed, the combinator
        itself is added as a rule as well.

        Note: This ordering created here is the reason why popLeft is equivalent to dropTargets.

        :param combinator: The combinator being evaluated.
        :param m: The type of the combinator being evaluated.
        :return: The resulting rules for the inhabitation machine to process.
        """
        result: deque[Rule] = deque()
        srcs, tgt = m
        for src in srcs:
            # If I have A -> B -> C, this makes Arrow(B,C), followed by Arrow(A,Arrow(B,C)) and so on.
            # This notion is all that's happening in this loop.
            arr = Arrow(src, tgt)
            # The target type of Apply is tgt, the function of Apply are the expanded MultiArrows, and the source type
            # is the left side index that is currently being expanded
            result.appendleft(Apply(tgt, arr, src))
            # Quintessentially moves target on set of brackets further outwards.
            tgt = arr
        result.appendleft(Combinator(tgt, combinator))
        return result

    @staticmethod
    def _commit_updates(
        target: Type, combinator: object, covers: Sequence[MultiArrow]
    ) -> deque[deque[Rule]]:
        """Takes the output produced by the cover machine and converts it into
        a list of lists, with each sublist representing an ordered set of Apply
        rules and a Combinator rule, which can be added to the G_targets set.
        The ordering is the ordering given on the bottom of page 69.

        The MultiArrows that get expanded to Rules are not just the contents of the cover,
        but use the given target instead of their own target. (Lemma 27, p. 63)

        :param target: The target type to create new rules for.
        :param combinator: The combinator object that will become a rule and is associated with the created rules.
        :param covers: The list of MultiArrows computed by the cover machine.
        :return: The resulting list of lists, representing the segmented rules.
        """
        result: deque[deque[Rule]] = deque()
        for cover in covers:
            result.append(
                # Convert the MultiArrow to Rules (expand it)
                FiniteCombinatoryLogic._commit_multi_arrow(
                    combinator, (cover[0], target)
                )
            )
        return result

    def _accumulate_covers(
        self,
        target: Type,
        to_cover: set[Type],
        combinator: object,
        combinator_type: list[list[MultiArrow]],
    ) -> tuple[deque[deque[Rule]], bool]:
        """Computes all the covers necessary for a given combinators type, a
        set of types that need to be covered and a target Type that needs to be
        inhabitated and converts the covers into a segmented list of lists of
        rules that the inhabitation machine can process. Note: The partial from
        _inhabit_cover fixes the first two arguments. This can intuitively be
        understood as us checking all combinators in the repository against the
        target and using the cover machine to find out if the combinator can
        allow us to cover the target.

        :param target: The Type to compute covers for.
        :param to_cover: A list of types for the target to be covered. Recursively computed by organized.
        :param combinator: The combinator that we are preparing for the cover machine.
        :param combinator_type: The type of the combinator.
        :return:
        """

        def cover_instr(ms: list[MultiArrow]) -> CoverMachineInstruction:
            splits: list[(MultiArrow, set[Type])] = list(
                map(
                    lambda m: (
                        # The first part of splits is of course just the MultiArrow
                        m,
                        # The type covered by the MultiArrow initially is itself and all its supertypes
                        # of course only w.r.t. the types we are seeking to cover, to_cover
                        set(
                            filter(
                                lambda b: self.subtypes.check_subtype(m[1], b), to_cover
                            )
                        ),
                    ),
                    ms,
                )
            )
            return self._cover(splits, to_cover)

        # For each part of the combinator_type create a corresponding cover machine instruction as given above
        # Call the cover_machine with that set of computed instructions
        covers: list[MultiArrow] = self._cover_machine(
            [], list(map(cover_instr, combinator_type))
        )
        # Eliminate redundant MultiArrows from the computed cover and feed the cover to the inhabitation machine as
        # its next targets
        next_rules: deque[deque[Rule]] = FiniteCombinatoryLogic._commit_updates(
            target, combinator, self._reduce_multi_arrows(covers)
        )
        return next_rules, not covers

    def _inhabit_cover(self, target: Type, todo_rules: deque[deque[Rule]]) -> bool:
        # Remove all types that reduce to Omega under subtyping (p.28 f)
        # For what organized returns, check Arrows etc. implementation of it
        # E.g. Arrow propagates, Arrow(A, B).organized is Arrow(A, B.organized) etc.
        prime_factors: set[Type] = self.subtypes.minimize(target.organized)
        with Pool(self.processes) as pool:
            results = pool.starmap(
                # The partial calls accumulate_covers with  target and prime factors always the same
                partial(self._accumulate_covers, target, prime_factors),
                # This is the iterable that contains tuples that each provide combinator and combinator_type
                # In practice, this is the dict that is used as a repo, but in splitted
                self.splitted_repository.items(),
                # This is just the chunksize (I think the missing keyword causes an error here)
                max(len(self.splitted_repository) // self.processes, 10),
            )
        failed: bool = True
        # All accumulated covers from checking against every combinator
        for rules, local_fail in results:
            # We add the rules coming from those combinators that were able to cover the target
            if not local_fail:
                failed = False
                todo_rules.extend(rules)
        # If none could cover the target, we fail.
        return failed

    def _omega_rules(self, target: Type) -> set[Rule]:
        return {
            Apply(target, target, target),
            *map(lambda c: Combinator(target, c), self.splitted_repository.keys()),
        }

    def _inhabitation_step(
        self, stable: set[Rule], targets: deque[deque[Rule]]
    ) -> bool:
        """Implements the inhabitation machine's state transition function
        (p.68).

        :param stable:
        :param targets:
        :return:
        """
        if targets:
            if targets[0]:
                target = targets[0].popleft()
                match target:
                    # Implements rule group 1 (p.68)
                    # Simply explained, using a Combinator is trivially always a valid rule
                    case Combinator(_, _):
                        stable.add(target)
                    case Apply(_, _, _) if target in stable:
                        pass
                    case Apply(_, _, target_type):
                        # Compute the value of fail_existing that the state transition bases its actions on (p.68)
                        failed, existing = self._compute_fail_existing(
                            stable, target_type
                        )
                        # Implement lines 8-11 of Pseudocode on page 68.
                        if failed:
                            # If target_type is a failure but not yet marked in stable, mark it.
                            if not existing:
                                stable.add(Failed(target_type))
                        # Implement lines 12-13 from page 68.
                        # If an Application already existed with the same target, then this one is also fine.
                        elif existing:
                            stable.add(target)
                        # Implement lines 14-15, page 68.
                        # If the target of the application doesn't matter, we can definitely add it to the stable rules.
                        elif target_type.is_omega:
                            # omega_rules generates the derived subtype rules for omega and is a pure helper function,
                            # as explained in the middle of page 70.
                            stable |= self._omega_rules(target_type)
                            stable.add(target)
                        else:
                            # Implements group 5 from pseudocode on page 68.
                            # Note that inhabit_cover can modify targets as it computed additional inhabitation targets.
                            inhabit_failed = self._inhabit_cover(target_type, targets)
                            # Case 1, inhabitation for some part of the sequence belonging to a MultiArrow plus
                            # Combinator failed.
                            if inhabit_failed:
                                # Mark the failure in stable, so that compute_failed_existing can improve efficiency
                                stable.add(Failed(target_type))
                                # Drop targets up to next combinator
                                targets.popleft()
                            else:
                                # Else add rule to the result rules (stable)
                                stable.add(target)
                    case _:
                        raise TypeError(f"Invalid type of rule: {target}")
            else:
                # This is dropTargets (p.67) and represents a target being done processing
                # Note that this seems to differ from the formal definition given.
                # This is due to the different data structure compared to page 68.
                # Each target here is a subsequence of G_targets, that has been split by occurrence of Combinators.
                # Thus popping a whole subsequence indeed represents dropping everything until the next combinator.
                # This is possible because of targets internal order, as described on the bottom of page 69.
                targets.popleft()
        return bool(targets)

    def _inhabitation_machine(self, stable: set[Rule], targets: deque[deque[Rule]]):
        """Implements the actual inhabitation machine. This calls
        _inhabitation_step until the list of candidate rules to include in the
        tree grammar is empty (targets). Variable stable contains the rules
        that are part of the final computed tree grammar. The contents of
        stable are, in fact, stable, as in the list is only appended too, and
        previously committed members of the list do not get modified or
        removed. (p.69 f.)

        :param stable:
        :param targets:
        :return:
        """
        while self._inhabitation_step(stable, targets):
            pass

    def inhabit(self, *targets: Type) -> InhabitationResult:
        result: set[Rule] = set()
        all_targets = list(targets)
        todo_rules: deque[deque[Rule]] = deque()
        for target in all_targets:
            if target.is_omega:
                result |= self._omega_rules(target)
            else:
                failed, existing = self._compute_fail_existing(result, target)
                if failed:
                    if not existing:
                        result.add(Failed(target))
                else:
                    inhabit_failed = self._inhabit_cover(target, todo_rules)
                    if inhabit_failed:
                        result.add(Failed(target))
                    else:
                        self._inhabitation_machine(result, todo_rules)
        return InhabitationResult(
            targets=all_targets, rules=FiniteCombinatoryLogic._prune(result)
        )

    @staticmethod
    def _ground_types_of(rules: set[Rule]) -> set[Type]:
        ground: set[Type] = set()
        next_ground: set[Type] = {rule.target for rule in rules if rule.is_combinator}

        while next_ground:
            ground |= next_ground
            next_ground = set()
            for rule in rules:
                match rule:
                    case Apply(target, function_type, argument_type) if (
                        function_type in ground
                        and argument_type in ground
                        and target not in ground
                    ):
                        next_ground.add(target)
                    case _:
                        pass
        return ground

    @staticmethod
    def _prune(rules: set[Rule]) -> set[Rule]:
        ground_types: set[Type] = FiniteCombinatoryLogic._ground_types_of(rules)
        result = set()
        for rule in rules:
            match rule:
                case Apply(target, _, _) if target not in ground_types:
                    result.add(Failed(target))
                case Apply(_, function_type, argument_type) if not (
                    function_type in ground_types and argument_type in ground_types
                ):
                    continue
                case _:
                    result.add(rule)
        return result
