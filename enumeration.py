from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Callable, Sized
from dataclasses import dataclass, field
from itertools import chain
from typing import TypeVar, Tuple, Union, Generic, Optional
from functools import cached_property

A = TypeVar('A', covariant=True)
B = TypeVar('B') #, covariant=True)
C = TypeVar('C')

class State(Generic[A], ABC):
    @abstractmethod
    def done(self) -> Optional['Return[A]']:
        pass

    @abstractmethod
    def step(self) -> 'State[A]':
        pass

    @abstractmethod
    def _step_dispatch(self, outer: 'FlatMap[C, A]') -> 'State[C]':
        pass

    @abstractmethod
    def flat_map(self, f: Callable[[A], 'State[C]']) -> 'State[C]':
        pass


@dataclass()
class Return(State[A]):
    result: A = field(init=True)

    def done(self) -> Optional['Return[A]']:
        return self

    def step(self) -> State[A]:
        return self

    def _step_dispatch(self, outer: 'FlatMap[C, A]') -> 'State[C]':
        return outer.function(self.result)

    def __bool__(self):
        return True

    def flat_map(self, f: Callable[[A], 'State[C]']) -> 'State[C]':
        return FlatMap(self, f)

@dataclass(frozen=True)
class FlatMap(Generic[A, C], State[A]):
    other_value: State[C] = field(init=True)
    function: Callable[[C], State[A]] = field(init=True)

    def done(self) -> Optional['Return[A]']:
        return None

    def step(self) -> State[A]:
        return self.other_value._step_dispatch(self)

    def _step_dispatch(self, outer: 'FlatMap[C, A]') -> 'State[C]':
        return FlatMap(self.other_value, lambda r: FlatMap(self.function(r), outer.function))

    def flat_map(self, f: Callable[[A], 'State[C]']) -> 'State[C]':
        return FlatMap(self, f)
        #return FlatMap(self.other_value, lambda r: self.function(r).flat_map(f))


@dataclass
class Box(Generic[A]):
    value: State[A] = field(init=True)

class Lazy(Generic[A]):
    #state: State[A] = field(init=True)
    def __init__(self, state: Box[State[A]]):
        self.state = state

    def value(self) -> A:
        done: Optional[Return[A]] = self.state.value.done()
        while not done:
            self.state.value = self.state.value.step()
            done = self.state.value.done()
        return done.result

    @staticmethod
    def once(x: Callable[[], A]) -> 'Lazy[A]':
        return Lazy(Box(FlatMap(Return(()), lambda u: Return(x()))))

    @staticmethod
    def of(x: C) -> 'Lazy[C]':
        return Lazy(Box(Return(x)))

    def map(self, f: Callable[[A], B]) -> 'Lazy[B]':
        return Lazy(Box(FlatMap(Return(self), lambda s: s.state.value).flat_map(lambda x: Return(f(x)))))
        #return Lazy(FlatMap(self.state, lambda x: Return(f(x))))

    def flat_map(self, f: Callable[[A], 'Lazy[B]']) -> 'Lazy[B]':
        return Lazy(Box(FlatMap(Return(self), lambda s: s.state.value).flat_map(lambda x: f(x).state.value)))
        #return Lazy(FlatMap(self.state, lambda x: f(x).state))


class LazyStream(Iterable[A], ABC):
    @abstractmethod
    def map(self, f: Callable[[A], B]) -> 'LazyStream[B]':
        pass

    @abstractmethod
    def lazy_map(self, f: Callable[[Lazy[A]], Lazy[B]]) -> 'LazyStream[B]':
        pass

    @abstractmethod
    def non_empty(self) -> Optional['Cons[A]']:
        pass

    @abstractmethod
    def append(self, other: Lazy['LazyStream[B]']) -> 'LazyStream[Union[A, B]]':
        pass

    @abstractmethod
    def tails(self) -> 'LazyStream[LazyStream[A]]':
        pass

    @property
    def head(self):
        raise AttributeError("head")

    @property
    def tail(self):
        raise AttributeError("tail")

    @abstractmethod
    def __iter__(self) -> Iterator[A]:
        pass

    @abstractmethod
    def lazy_iter(self) -> Iterator[Lazy[A]]:
        pass

    @abstractmethod
    def zip(self, other: 'LazyStream[B]') -> 'LazyStream[Tuple[A, B]]':
        pass


@dataclass(frozen=True)
class Nil(LazyStream[A]):

    def __iter__(self) -> Iterator[A]:
        return iter(())

    def lazy_iter(self) -> Iterator[Lazy[A]]:
        return iter(())

    def map(self, f: Callable[[A], B]) -> LazyStream[B]:
        return Nil()

    def lazy_map(self, f: Callable[[Lazy[A]], Lazy[B]]) -> 'LazyStream[B]':
        return Nil()

    def non_empty(self) -> Optional['Cons[A]']:
        return None

    def append(self, other: Lazy[LazyStream[B]]) -> LazyStream[Union[A, B]]:
        return other.value()

    def tails(self):
        return Nil()

    def zip(self, other: LazyStream[B]) -> LazyStream[Tuple[A, B]]:
        return Nil()


@dataclass(frozen=True, eq=False, order=False, repr=False)
class Cons(LazyStream[A]):
    lazy_head: Lazy[A] = field(init=True)
    lazy_tail: Lazy[LazyStream[A]] = field(init=True)
    #def __init__(self, lazy_head: Lazy[A], lazy_tail: Lazy[LazyStream[A]]):
    #    self.lazy_head = lazy_head
    #    self.lazy_tail = lazy_tail

    @property
    def head(self) -> A:
        return self.lazy_head.value()

    @property
    def tail(self) -> LazyStream[A]:
        return self.lazy_tail.value()

    def __iter__(self) -> Iterator[A]:
        next = self.non_empty()
        while next:
            yield next.lazy_head.value()
            next = next.lazy_tail.value().non_empty()

    def lazy_iter(self) -> Iterator[Lazy[A]]:
        next = self.non_empty()
        while next:
            yield next.lazy_head
            next = next.lazy_tail.value().non_empty()

    def map(self, f: Callable[[A], B]) -> LazyStream[B]:
        return Cons(self.lazy_head.map(f), self.lazy_tail.map(lambda t: t.map(f)))

    def lazy_map(self, f: Callable[[Lazy[A]], Lazy[B]]) -> 'LazyStream[B]':
        return Cons(f(self.lazy_head), self.lazy_tail.map(lambda t: t.lazy_map(f)))

    def non_empty(self) -> 'Cons[A]':
        return self

    def __bool__(self):
        return True

    def append(self, other: Lazy[LazyStream[B]]) -> LazyStream[Union[A, B]]:
        return Cons(self.lazy_head, self.lazy_tail.map(lambda t: t.append(other)))

    def tails(self) -> LazyStream[LazyStream[A]]:
        return Cons(Lazy.of(self), self.lazy_tail.map(lambda t: t.tails()))

    def zip(self, other: LazyStream[B]) -> LazyStream[Tuple[A, B]]:
        cons_other = other.non_empty()
        if cons_other:
            return Cons(self.lazy_head.flat_map(lambda l: cons_other.lazy_head.map(lambda r: (l, r))),
                        self.lazy_tail.flat_map(lambda l: cons_other.lazy_tail.map(lambda r: l.zip(r))))
        else:
            return Nil()


@dataclass(frozen=True, order=False, eq=False, repr=False)
class Finite(Iterable[A], Sized):
    _cardinality: Lazy[int] = field(init=True)
    _get_checked: Callable[[int], Lazy[A]] = field(init=True, hash=False)

    #def __init__(self, cardinality: Lazy[int], get_checked: Callable[[int], Lazy[A]]):
    #    self._cardinality = cardinality
    #    self._get_checked = get_checked

    def get(self, index: int) -> A:
        if 0 <= index < len(self):
            return self._get_checked(index).value()
        else:
            raise IndexError(index)

    def map_lazy(self, f: Callable[[A], Lazy[B]]) -> 'Finite[B]':
        return Finite(self._cardinality, lambda index: self._get_checked(index).flat_map(f))

    def map(self, f: Callable[[A], B]) -> 'Finite[B]':
        return Finite(self._cardinality, lambda index: self._get_checked(index).map(f))

    def __len__(self) -> int:
        return self._cardinality.value()

    def __add__(self, right: 'Finite[B]') -> 'Finite[Union[A, B]]':
        def getc(index: int) -> Lazy[Union[A, B]]:
            return self._cardinality.flat_map(
                        lambda len_self: (self._get_checked(index) if index < len_self else
                                          right._get_checked(index))
                    )

        return Finite(self._cardinality.flat_map(lambda l: right._cardinality.map(lambda r: l + r)), getc)

    def __mul__(self, right: 'Finite[B]') -> 'Finite[Tuple[A, B]]':
        def getc(index: int) -> Lazy[Tuple[A, B]]:
            return self._cardinality.flat_map(
                        lambda len_self:
                            self._get_checked(index // len_self).flat_map(
                                lambda l: right._get_checked(index % len_self).map(lambda r: (l, r))
                            ))

        return Finite(self._cardinality.flat_map(lambda l: right._cardinality.map(lambda r: l * r)), getc)

    def __iter__(self) -> Iterator[A]:
        for idx in range(0, len(self)):
            yield self._get_checked(idx).value()

    def lazy_iter(self) -> Iterator[Lazy[A]]:
        for idx in range(0, len(self)):
            yield Lazy.of(idx).flat_map(lambda idx: self._get_checked(idx))

    def values(self) -> LazyStream[A]:
        len_self = len(self)

        def mk(index: int) -> LazyStream[A]:
            if index < len_self:
                return Cons(self._get_checked(index), Lazy.of(index + 1).map(lambda next_index: mk(next_index)))
            else:
                return Nil()
        return mk(0)

    @staticmethod
    def singleton(x: C) -> 'Finite[C]':
        return Finite(Lazy.of(1), lambda index: Lazy.of(x))

    @staticmethod
    def empty() -> 'Finite[C]':
        def fail(index) -> Lazy[C]:
            raise IndexError(index)
        return Finite(Lazy.of(0), fail)


@dataclass(frozen=True)
class Enumeration(Iterable[A]):
    _parts: Lazy[LazyStream[Finite[A]]] = field(init=True)

    #def __init__(self, parts: Lazy[LazyStream[Finite[A]]]):
    #    self._parts = parts

    @staticmethod
    def empty():
        return Enumeration(Lazy.of(Nil()))

    @staticmethod
    def singleton(value: C) -> 'Enumeration[C]':
        return Enumeration(Lazy.of(Cons(Lazy.of(Finite.singleton(value)), Lazy.of(Nil()))))

    @property
    def parts(self) -> LazyStream[Finite[A]]:
        return self._parts.value()

    def get(self, index: int) -> A:
        if index < 0:
            raise IndexError(index)
        for part in self.parts:
            len_part = len(part)
            if index < len_part:
                return part.get(index)
            else:
                index -= len_part
        raise IndexError(index)

    def values(self) -> LazyStream[Tuple[int, LazyStream[A]]]:
        return self.parts.map(lambda part: (len(part), part.values()))

    def __iter__(self) -> Iterator[A]:
        for part in self.parts:
            for elem in part:
                yield elem

    def lazy_iter(self) -> Iterator[Lazy[A]]:
        for part in self.parts:
            for elem in part.lazy_iter():
                yield elem

    @staticmethod
    def _unfold_parts(left: LazyStream[Finite[A]], right: LazyStream[Finite[B]]) -> LazyStream[Finite[Union[A, B]]]:
        left_cons: Optional[Cons[Finite[A]]] = left.non_empty()
        if left_cons:
            right_cons: Optional[Cons[Finite[B]]] = right.non_empty()
            if right_cons:
                return Cons(left_cons.lazy_head.flat_map(lambda l: right_cons.lazy_head.map(lambda r: l + r)),
                            left_cons.lazy_tail.flat_map(
                                lambda l: right_cons.lazy_tail.map(lambda r: Enumeration._unfold_parts(l, r))
                            ))

        return left.append(Lazy.of(right))

    @staticmethod
    def _go_reversals(reversals: Lazy[LazyStream[Finite[A]]],
                      xs: LazyStream[Finite[A]]) -> LazyStream[Lazy[LazyStream[Finite[A]]]]:
        xs_cons: Optional[Cons[Finite[A]]] = xs.non_empty()
        if xs_cons:
            reversals_with_x: Lazy[LazyStream[Finite[A]]] = Lazy.once(lambda: Cons(xs_cons.lazy_head, reversals))
            reversals_with_tl: Lazy[LazyStream[Lazy[LazyStream[Finite[A]]]]] = \
                xs_cons.lazy_tail.map(lambda tl: Enumeration._go_reversals(reversals_with_x, tl))
            return Cons(Lazy.once(lambda: reversals_with_x), reversals_with_tl)
        else:
            return Nil()

    def union(self, other: Lazy['Enumeration[B]']) -> 'Enumeration[Union[A, B]]':
        return Enumeration(
            self._parts.flat_map(
                lambda self_parts:
                    other.flat_map(
                        lambda other_value: other_value._parts.map(
                                                lambda other_parts: Enumeration._unfold_parts(self_parts, other_parts)
                        ))
            ))

    def map(self, f: Callable[[A], B]) -> 'Enumeration[B]':
        return Enumeration(self._parts.map(lambda parts: parts.map(lambda part: part.map(f))))

    def reversals(self) -> Lazy[LazyStream['Enumeration[A]']]:
        return self._parts.map(lambda parts: Enumeration._go_reversals(Lazy.of(Nil()), parts).map(Enumeration))

    @staticmethod
    def convolute(tail: Lazy[LazyStream[Finite[A]]], reverse_other: 'Enumeration[B]') -> Lazy[Finite[Tuple[A, B]]]:
        def run(zipped: LazyStream[Tuple[Finite[A], Finite[B]]], result: Lazy[Finite[Tuple[A, B]]]) -> Lazy[Finite[Tuple[A, B]]]:
            cons_zipped = zipped.non_empty()
            if cons_zipped:
                return cons_zipped.lazy_head.flat_map(
                    lambda xy: cons_zipped.lazy_tail.flat_map(lambda tl: run(tl, result.map(lambda r: r + (xy[0] * xy[1])))))
            return result
        return tail.flat_map(lambda _tail: reverse_other._parts.flat_map(lambda other_parts: run(_tail.zip(other_parts), Lazy.of(Finite.empty()))))

    @staticmethod
    def _go_product_rev(
            parts: LazyStream[Finite[A]],
            ry: Lazy['Enumeration[B]'],
            rys: Lazy[LazyStream['Enumeration[B]']]) -> LazyStream[Finite[Tuple[A, B]]]:
        def rest(_rys: LazyStream['Enumeration[B]']) -> Lazy[LazyStream[Finite[Tuple[A, B]]]]:
            cons_rys = _rys.non_empty()
            if cons_rys:
                return Lazy.once(lambda: Enumeration._go_product_rev(parts, cons_rys.lazy_head, cons_rys.lazy_tail))
            else:
                return ry.map(lambda _ry: parts.tail.tails().lazy_map(lambda x: Enumeration.convolute(x, _ry)))

        return Cons(ry.flat_map(lambda _ry: Enumeration.convolute(Lazy.of(parts), _ry)),
                    rys.flat_map(rest))

    def _product_rev(self, reverse_others: Lazy[LazyStream['Enumeration[B]']]) -> 'Enumeration[Tuple[A, B]]':
        def run(parts: LazyStream[Finite[A]], other_parts: LazyStream['Enumeration[B]']) -> LazyStream[Finite[Tuple[A, B]]]:
            cons_parts = parts.non_empty()
            if cons_parts:
                cons_other_parts = other_parts.non_empty()
                if cons_other_parts:
                    return Enumeration._go_product_rev(parts, cons_other_parts.lazy_head, cons_other_parts.lazy_tail)
            return Nil()
        return Enumeration(self._parts.flat_map(lambda parts: reverse_others.map(lambda other_parts: run(parts, other_parts))))

    def product(self, other: Lazy['Enumeration[B]']) -> 'Enumeration[Tuple[A, B]]':
        return self._product_rev(other.flat_map(lambda other_enum: other_enum.reversals()))

    def pay(self) -> 'Enumeration[A]':
        return Enumeration(Lazy.once(lambda: Cons(Lazy.of(Finite.empty()), self._parts)))

    @staticmethod
    def ints() -> 'Enumeration[int]':
        def nums(start: int) -> LazyStream[Finite[int]]:
            return Cons(Lazy.of(Finite.singleton(start)), Lazy.once(lambda: nums(start+1)))
        return Enumeration(Lazy.of(nums(0)))



def fibo(n: int, k: int) -> LazyStream[int]:
    print(f"fibo({n}, {k})")
    return Cons(Lazy.of(n + k), Lazy.once(lambda: fibo(k, n+k)))


if __name__ == "__main__":
    fibs: LazyStream[int] = Cons(Lazy.of(1), Lazy.once(lambda: Cons(Lazy.of(1), Lazy.once(lambda: fibo(1, 1)))))
    fib_iter = iter(fibs)
    for i in range(3):
        print(next(fib_iter))
    fib_iter = iter(fibs)
    for i in range(3):
        print(next(fib_iter))
    ints = Enumeration.ints()
    print(ints.get(99999))

    print(ints.product(Lazy.of(ints)).get(99999))
    xs = Enumeration.ints().union(Lazy.once(Enumeration.ints))
    for x in range(0, 10):
        print(xs.get(x))


