from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Callable
from dataclasses import dataclass, field
from itertools import chain
from typing import TypeVar, Tuple, Union
from functools import cache

A = TypeVar('A')
B = TypeVar('B')


class LazyStream(Iterable[A], ABC):
    pass


@dataclass(frozen=True)
class Nil(LazyStream[A]):
    def __iter__(self) -> Iterator[A]:
        return iter(())


@dataclass(frozen=True)
class Cons(LazyStream[A]):
    _head: Callable[[], A] = field(init=True)
    _tail: Callable[[], LazyStream[A]] = field(init=True)

    @property
    @cache
    def head(self) -> A:
        return self._head()

    @property
    @cache
    def tail(self) -> LazyStream[A]:
        return self._tail()

    def __iter__(self) -> Iterator[A]:
        def start() -> Iterator[A]:
            yield self.head
        rest: Iterable[A] = self.tail
        return chain(start(), rest)


@dataclass(frozen=True)
class Finite(Iterable[A]):
    cardinality: int = field(init=True)
    get_checked: Callable[int, A] = field(init=True)

    def get(self, index: int) -> A:
        if 0 <= index < self.cardinality:
            return self.get_checked(index)
        else:
            raise IndexError(index)

    def map(self, f: Callable[[A], B]) -> 'Finite[B]':
        return Finite(self.cardinality, lambda index: f(self.get_checked(index)))

    def __add__(self, right: 'Finite[B]') -> 'Finite[Union[A, B]]':
        return Finite(self.cardinality + right.cardinality,
                      lambda index: (self.get_checked(index) if index < self.cardinality else
                                     right.get_checked(index - self.cardinality)))

    def __mul__(self, right: 'Finite'[B]) -> 'Finite[Tuple[A, B]]':
        return Finite(self.cardinality * right.cardinality,
                      lambda index:
                      (self.get_checked(index // self.cardinality),
                       right.get_checked(index % self.cardinality)))

    def __iter__(self) -> Iterator[A]:
        for i in range(0, self.cardinality):
            yield self.get_checked(i)

    def values(self) -> LazyStream[A]:
        def mk(index: int) -> LazyStream[A]:
            if index <= self.cardinality:
                return Cons(lambda: self.get_checked(index), lambda: mk(index + 1))
            else:
                return Nil()
        return mk(0)

    @staticmethod
    def singleton(x: A) -> 'Finite'[A]:
        return Finite(1, lambda index: x)

    @staticmethod
    def empty() -> 'Finite'[A]:
        def fail(index: int) -> 'Finite'[A]:
            raise IndexError(index)
        return Finite(0, fail)

#class Enumeration



def fibo(n: int, k: int) -> LazyStream[int]:
    return Cons(lambda: n + k, lambda: fibo(k, n+k))


if __name__ == "__main__":
    fibs: LazyStream[int] = Cons(lambda: 1, lambda: Cons(lambda: 1, lambda: fibo(1, 1)))
    fib_iter = iter(fibs)
    for i in range(3):
        print(next(fib_iter))
    fib_iter = iter(fibs)
    for i in range(3):
        print(next(fib_iter))
