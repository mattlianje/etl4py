from typing import TypeVar, Generic, Callable, Any, Tuple, Union, Dict, List
from dataclasses import dataclass

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')

class Node(Generic[A, B]):
    def __init__(self, func: Callable[[A], B]):
        self.func = func

    def __call__(self, x: A) -> B:
        return self.func(x)

    def __rshift__(self, other: Union['Node[B, C]', 'Pipeline[B, C]']) -> 'Pipeline[A, C]':
        if isinstance(other, Pipeline):
            return Pipeline(lambda x: other.run(self(x)))
        return Pipeline(lambda x: other(self(x)))

    def __and__(self, other: 'Node[A, C]') -> 'Node[A, Tuple]':
        def combine(x: A) -> Tuple:
            left = self(x)
            right = other(x)
            if isinstance(left, tuple):
                if isinstance(right, tuple):
                    return left + right
                return left + (right,)
            if isinstance(right, tuple):
                return (left,) + right
            return (left, right)
        return Node(combine)

@dataclass
class Pipeline(Generic[A, B]):
    func: Callable[[A], B]

    def __rshift__(self, other: Union[Node[B, C], 'Pipeline[B, C]']) -> 'Pipeline[A, C]':
        if isinstance(other, Pipeline):
            return Pipeline(lambda x: other.run(self.run(x)))
        return Pipeline(lambda x: other(self.run(x)))

    def __and__(self, other: Union[Node[A, C], 'Pipeline[A, C]']) -> Node[A, Tuple]:
        if isinstance(other, Pipeline):
            return Node(lambda x: (self.run(x), other.run(x)))
        return Node(lambda x: (self.run(x), other(x)))

    def run(self, x: A) -> B:
        return self.func(x)

    def unsafe_run(self, x: A = None) -> B:
        return self.run(x)

def Extract(f: Callable[[A], B]) -> Node[A, B]:
    return Node(f)

def Transform(f: Callable[[A], B]) -> Node[A, B]:
    return Node(f)

def Load(f: Callable[[A], B]) -> Node[A, B]:
    return Node(f)

