import functools
import itertools
from typing import TypeAlias, Callable, Tuple
from multiprocessing import Pool
import os
from collections.abc import Iterator
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from itertools import chain
from functools import partial

from itypes import *
from subtypes import Subtypes

MultiArrow: TypeAlias = Tuple[list[Type], Type]
State: TypeAlias = list['MultiArrow']
CoverMachineInstruction: TypeAlias = Callable[[State], Tuple[State, list['CoverMachineInstruction']]]


@dataclass(frozen=True)
class Rule(ABC):
    target: Type = field(init=True, kw_only=True)
    is_combinator: bool = field(init=True, kw_only=True)


@dataclass(frozen=True)
class Failed(Rule):
    target: Type = field()
    is_combinator: bool = field(default=False, init=False)

    def __str__(self):
        return f"Failed({str(self.target)})"

@dataclass(frozen=True)
class Combinator(Rule):
    target: Type = field()
    is_combinator: bool = field(default=True, init=False)
    combinator: object = field(init=True)

    def __str__(self):
        return f"Combinator({str(self.combinator)}, {str(self.target)})"


@dataclass(frozen=True)
class Apply(Rule):
    target: Type = field()
    is_combinator: bool = field(default=False, init=False)
    function_type: Type = field(init=True)
    argument_type: Type = field(init=True)

    def __str__(self):
        return f"@({str(self.function_type)}, {str(self.argument_type)}) : {self.target}"


def _split_repo(c: object, ty: Type) -> Tuple[object, list[list[MultiArrow]]]:
    return c, FiniteCombinatoryLogic.split_ty(ty)


class FiniteCombinatoryLogic(object):

    def __init__(self, repository: dict[object, Type], subtypes: Subtypes, processes=os.cpu_count()):
        self.processes = processes

        self.repository = repository
        with Pool(processes) as pool:
            self.splitted_repository: dict[object, list[list[MultiArrow]]] = \
                dict(pool.starmap(_split_repo,
                     self.repository.items(),
                     chunksize=max(len(self.repository) // processes, 10)))
        self.subtypes = subtypes

    @staticmethod
    def split_ty(ty: Type) -> list[list[MultiArrow]]:
        def safe_split(xss: list[list[MultiArrow]]) -> (list[MultiArrow], list[list[MultiArrow]]):
            return (xss[0] if xss else []), xss[1:]

        def split_rec(to_split: Type, srcs: list[Type], delta: list[list[MultiArrow]]) -> list[list[MultiArrow]]:
            match to_split:
                case Arrow(src, tgt):
                    (xs, xss) = safe_split(delta)
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

    def _dcap(self, sigma: Type, tau: Type) -> Type:
        if self.subtypes.check_subtype(sigma, tau):
            return sigma
        elif self.subtypes.check_subtype(tau, sigma):
            return tau
        else:
            return Intersection(tau, sigma)

    @staticmethod
    def _partition_cover(covered: set[Type], to_cover: set[Type]) -> (set[Type], set[Type]):
        in_covered: set[Type] = set()
        not_in_covered: set[Type] = set()
        for ty in to_cover:
            if ty in covered:
                in_covered.add(ty)
            else:
                not_in_covered.add(ty)
        return in_covered, not_in_covered

    @staticmethod
    def _still_possible(splits: list[(MultiArrow, set[Type])], to_cover: set[Type]) -> bool:
        for ty in to_cover:
            if not filter(lambda covered: ty in covered[1], splits):
                return False
        return True

    def _merge_multi_arrow(self, arrow1: MultiArrow, arrow2: MultiArrow) -> MultiArrow:
        return list(map(self._dcap, arrow1[0], arrow2[0])), self._dcap(arrow1[1], arrow2[1])

    def _check_cover(self, splits: list[(MultiArrow, set[Type])], to_cover: set[Type]) -> CoverMachineInstruction:
        def instr(state: list[MultiArrow]) -> (State, list[CoverMachineInstruction]):
            if FiniteCombinatoryLogic._still_possible(splits, to_cover):
                return state, [self._cover(splits, to_cover)]
            else:
                return state, []

        return instr

    def _check_continue_cover(self, splits: list[(MultiArrow, set[Type])],
                              to_cover: set[Type],
                              current_result: MultiArrow) -> CoverMachineInstruction:
        def instr(state: list[MultiArrow]) -> (State, list[CoverMachineInstruction]):
            if FiniteCombinatoryLogic._still_possible(splits, to_cover):
                return state, [self._continue_cover(splits, to_cover, current_result)]
            else:
                return state, []

        return instr

    def _continue_cover(self, splits: list[(MultiArrow, set[Type])],
                        to_cover: set[Type],
                        current_result: MultiArrow) -> CoverMachineInstruction:
        def instr(state: list[MultiArrow]) -> (State, list[CoverMachineInstruction]):
            if not splits:
                return state, []
            m, covered = splits[0]
            _splits = splits[1:]
            freshly_covered, uncovered = FiniteCombinatoryLogic._partition_cover(covered, to_cover)
            if not freshly_covered:
                return state, [self._continue_cover(_splits, to_cover, current_result)]
            merged = self._merge_multi_arrow(current_result, m)
            if not uncovered:
                return [merged, *state], [self._continue_cover(_splits, to_cover, current_result)]
            elif merged[0] == current_result[0]:
                return state, [self._continue_cover(_splits, uncovered, merged)]
            else:
                return state, [self._continue_cover(_splits, uncovered, merged),
                               self._check_continue_cover(_splits, to_cover, current_result)]

        return instr

    def _cover(self, splits: list[(MultiArrow, set[Type])], to_cover: set[Type]) -> CoverMachineInstruction:
        def instr(state: list[MultiArrow]) -> (State, list[CoverMachineInstruction]):
            if not splits:
                return state, []
            m, covered = splits[0]
            _splits = splits[1:]
            freshly_covered, uncovered = FiniteCombinatoryLogic._partition_cover(covered, to_cover)
            if not freshly_covered:
                return state, [self._cover(_splits, to_cover)]
            elif not uncovered:
                return [m, *state], [self._check_cover(_splits, to_cover)]
            else:
                return state, [self._continue_cover(_splits, uncovered, m),
                               self._check_cover(_splits, to_cover)]

        return instr

    @staticmethod
    def _cover_machine(state: State, program: list[CoverMachineInstruction]) -> State:
        instructions: Iterator[CoverMachineInstruction] = iter(program)
        while instruction := next(instructions, None):
            state, next_instructions = instruction(state)
            instructions = chain(iter(next_instructions), instructions)
        return state

    def _reduce_multi_arrows(self, ms: list[MultiArrow]) -> list[MultiArrow]:
        def check(lesser_arg_vect: MultiArrow, greater_arg_vect: MultiArrow) -> bool:
            (lesser_args, greater_args) = (lesser_arg_vect[0], greater_arg_vect[0])
            return (len(lesser_args) == len(greater_args)
                    and all(self.subtypes.check_subtype(lesser_arg, greater_arg)
                            for (lesser_arg, greater_arg) in zip(lesser_args, greater_args)))

        def average_arguments_type_size(m: MultiArrow) -> int:
            size: int = 0
            for ty in m[0]:
                size += ty.size
            return size / len(m[0]) if m[0] else 0

        result: list[MultiArrow] = []
        for multi_arrow in sorted(ms, key=average_arguments_type_size):
            if all(not check(multi_arrow, in_result) for in_result in result):
                result = [multi_arrow, *(in_result for in_result in result if not check(in_result, multi_arrow))]
        return result

    def _compute_fail_existing(self, rules: set[Rule], target: Type) -> (bool, bool):
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
                    pass
        return False, False

    @staticmethod
    def _commit_multi_arrow(combinator: object, m: MultiArrow) -> Iterator[Rule]:
        def rev_result() -> Iterator[Rule]:
            srcs, tgt = m
            for src in srcs:
                arr = Arrow(src, tgt)
                yield Apply(tgt, arr, src)
                tgt = arr
            yield Combinator(tgt, combinator)
        return reversed(list(rev_result()))

    @staticmethod
    def _commit_updates(target: Type,
                        combinator: object,
                        covers: Sequence[MultiArrow]) -> Iterator[Rule]:
        return chain.from_iterable(map(
            lambda cover: FiniteCombinatoryLogic._commit_multi_arrow(combinator, (cover[0], target)),
            covers))

    @staticmethod
    def drop_targets(rules: Iterator[Rule]) -> Iterator[Rule]:
        return itertools.dropwhile(lambda r: r.is_combinator, rules)

    def _accumulate_covers(self,
                           target: Type,
                           to_cover: set[Type],
                           combinator: object,
                           combinator_type: list[list[MultiArrow]]) -> Tuple[list[Rule], bool]:
        def cover_instr(ms: list[MultiArrow]) -> CoverMachineInstruction:
            splits: list[(MultiArrow, set[Type])] = \
                list(map(lambda m: (m, set(filter(lambda b: self.subtypes.check_subtype(m[1], b), to_cover))), ms))
            return self._cover(splits, to_cover)

        covers: list[MultiArrow] = self._cover_machine([], list(map(cover_instr, combinator_type)))
        next_rules: Iterator[Rule] = \
            FiniteCombinatoryLogic._commit_updates(target, combinator, self._reduce_multi_arrows(covers))
        return list(next_rules), not covers

    def _inhabit_cover(self, target: Type) -> (bool, Iterator[Rule]):
        prime_factors: set[Type] = self.subtypes.minimize(target.organized)
        with Pool(self.processes) as pool:
            results =\
                pool.starmap(
                    partial(self._accumulate_covers, target, prime_factors),
                    self.splitted_repository.items(),
                    max(len(self.splitted_repository) // self.processes, 10))
        failed: bool = True
        new_rules: Iterator[Rule] = iter(())
        for rules, local_fail in results:
            if not local_fail:
                failed = False
                new_rules = chain(new_rules, rules)
        return failed, new_rules

    def _omega_rules(self, target: Type) -> set[Rule]:
        return {Apply(target, target, target), *map(lambda c: Combinator(c, target), self.splitted_repository.keys())}

    def _inhabitation_step(self, stable: set[Rule], target: Rule, targets: Iterator[Rule]) -> Iterator[Rule]:
        match target:
            case Combinator(_, _):
                stable.add(target)
                return targets
            case Apply(_, _, _) if target in stable:
                return targets
            case Apply(_, _, target_type):
                failed, existing = self._compute_fail_existing(stable, target_type)
                if failed:
                    if not existing:
                        stable.add(Failed(target_type))
                    return self.drop_targets(targets)
                elif existing:
                    stable.add(target)
                    return targets
                elif target_type.is_omega:
                    stable |= self._omega_rules(target_type)
                    stable.add(target)
                    return targets
                else:
                    inhabitFailed, nextTargets = self._inhabit_cover(target_type)
                    if inhabitFailed:
                        stable.add(Failed(target_type))
                        return self.drop_targets(targets)
                    else:
                        stable.add(target)
                        return chain(nextTargets, targets)
            case _:
                return self.drop_targets(targets)

    def _inhabitation_machine(self, stable: set[Rule], targets: Iterator[Rule]):
        while target := next(targets, None):
            targets = self._inhabitation_step(stable, target, targets)

    def inhabit(self, *targets: Type) -> set[Rule]:
        result: set[Rule] = set()
        for target in targets:
            if target.is_omega:
                result |= self._omega_rules(target)
            else:
                failed, existing = self._compute_fail_existing(result, target)
                if failed:
                    if not existing:
                        result.add(Failed(target))
                else:
                    inhabit_failed, targets = self._inhabit_cover(target)
                    if inhabit_failed:
                        result.add(Failed(target))
                    else:
                        self._inhabitation_machine(result, targets)
        return FiniteCombinatoryLogic._prune(result)

    @staticmethod
    def _ground_types_of(rules: set[Rule]) -> set[Type]:
        ground: set[Type] = set()
        next_ground: set[Type] = set(rule.target for rule in rules if rule.is_combinator)

        while next_ground:
            ground |= next_ground
            next_ground = set()
            for rule in rules:
                match rule:
                    case Apply(target, function_type, argument_type) if (function_type in ground
                                                                         and argument_type in ground
                                                                         and not target in ground):
                        next_ground.add(target)
                    case _: pass
        return ground

    @staticmethod
    def _prune(rules: set[Rule]) -> set[Rule]:
        ground_types: set[Type] = FiniteCombinatoryLogic._ground_types_of(rules)
        result = set()
        for rule in rules:
            match rule:
                case Apply(target, _, _) if not target in ground_types:
                    result.add(Failed(target))
                case Apply(_, function_type, argument_type) if not (function_type in ground_types
                                                                    and argument_type in ground_types):
                    pass
                case _:
                    result.add(rule)
        return result

if __name__ == "__main__":
    repo = {"id": Intersection(Arrow(Constructor("Int"), Constructor("Int")),
                               Arrow(Constructor("Foo"), Intersection(Constructor("Bar"), Constructor("Baz")))),
            "x": Intersection(Constructor("Int"), Constructor("Foo2")),
            "pruned": Arrow(Constructor("Impossible"), Intersection(Constructor("Int"), Constructor("Foo"))),
            "loop" : Arrow(Constructor("Impossible"), Constructor("Impossible"))
            }
    inhab = FiniteCombinatoryLogic(repo, Subtypes({"Foo2": {"Foo"}}))
    result = inhab.inhabit(Intersection(Constructor("Int"), Constructor("Baz")))
    for rule in result:
        print(rule)
