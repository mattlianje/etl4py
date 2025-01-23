from __future__ import annotations
import time
from typing import TypeVar, Generic, Callable, Any, Tuple, Union, Dict, List, Optional, overload
from dataclasses import dataclass

A = TypeVar('A', contravariant=True)
B = TypeVar('B', covariant=True)
C = TypeVar('C')
R = TypeVar('R')
D = TypeVar('D')

@dataclass
class RetryConfig:
    max_attempts: int = 3
    delay_ms: int = 100
    
@dataclass
class Reader(Generic[R, D]):
    func: Callable[[R], D]
    
    def run(self, config: R) -> D:
        return self.func(config)
    
    def map(self, f: Callable[[D], B]) -> Reader[R, B]:
        return Reader(lambda r: f(self.func(r)))

    def __and__(self, other: Reader[R, B]) -> Reader[R, Tuple[D, B]]:
        return Reader(lambda r: (self.run(r), other.run(r)))
    
class Node(Generic[A, B]):
    def __init__(self, func: Callable[[A], B]) -> None:
        self.func = func

    def with_retry(self, config: RetryConfig) -> Node[A, B]:
          """Add retry capability to a node"""
          def retry_func(x: A) -> B:
              attempts = 0
              last_error = None
              
              while attempts < config.max_attempts:
                  try:
                      return self.func(x)
                  except Exception as e:
                      attempts += 1
                      last_error = e
                      if attempts < config.max_attempts:
                          time.sleep(config.delay_ms / 1000.0)
                          
              if last_error:
                  raise last_error
              raise RuntimeError("Retry failed")
              
          return Node(retry_func)
        
    def on_failure(self, handler: Callable[[Exception], B]) -> Node[A, B]:
        """Add failure handling to a node"""
        def safe_func(x: A) -> B:
            try:
                return self.func(x)
            except Exception as e:
                return handler(e)
                
        return Node(safe_func)

        
    def __call__(self, x: A) -> B:
        return self.func(x)
    
    def __or__(self, other: Node[B, C]) -> Node[A, C]:
        """Compose two nodes into a new node using |"""
        return Node(lambda x: other(self(x)))
        
    @overload
    def __rshift__(self, other: Node[B, C]) -> Pipeline[A, C]: ...
    
    @overload
    def __rshift__(self, other: Pipeline[B, C]) -> Pipeline[A, C]: ...
        
    def __rshift__(self, other: Union[Node[B, C], Pipeline[B, C]]) -> Pipeline[A, C]:
        if isinstance(other, Pipeline):
            return Pipeline(lambda x: other.run(self(x)))
        return Pipeline(lambda x: other(self(x)))
        
    def __and__(self, other: Union[Node[A, C], Pipeline[A, C]]) -> Node[A, Tuple[Any, ...]]:
        def combine(x: A) -> Tuple[Any, ...]:
            left = self(x)
            right = other.run(x) if isinstance(other, Pipeline) else other(x)
            
            left_tuple = left if isinstance(left, tuple) else (left,)
            right_tuple = right if isinstance(right, tuple) else (right,)
            
            return left_tuple + right_tuple
            
        return Node(combine)


class Extract(Node[A, B], Generic[A, B]):
    pass

class Transform(Node[A, B], Generic[A, B]):
    pass

class Load(Node[A, B], Generic[A, B]):
    pass

@dataclass
class Pipeline(Generic[A, B]):
    func: Callable[[A], B]

    def with_retry(self, config: RetryConfig) -> Pipeline[A, B]:
        """Add retry capability to a pipeline"""
        def retry_func(x: A) -> B:
            attempts = 0
            last_error = None
            
            while attempts < config.max_attempts:
                try:
                    return self.func(x)
                except Exception as e:
                    attempts += 1
                    last_error = e
                    if attempts < config.max_attempts:
                        time.sleep(config.delay_ms / 1000.0)
                        
            if last_error:
                raise last_error
            raise RuntimeError("Retry failed")
            
        return Pipeline(retry_func)
        
    def on_failure(self, handler: Callable[[Exception], B]) -> Pipeline[A, B]:
        """Add failure handling to a pipeline"""
        def safe_func(x: A) -> B:
            try:
                return self.func(x)
            except Exception as e:
                return handler(e)
                
        return Pipeline(safe_func)
    
    @overload
    def __rshift__(self, other: Node[B, C]) -> Pipeline[A, C]: ...
    
    @overload
    def __rshift__(self, other: Pipeline[B, C]) -> Pipeline[A, C]: ...
    
    def __rshift__(self, other: Union[Node[B, C], Pipeline[B, C]]) -> Pipeline[A, C]:
        if isinstance(other, Pipeline):
            return Pipeline(lambda x: other.run(self.run(x)))
        return Pipeline(lambda x: other(self.run(x)))
    
    def __and__(self, other: Union[Node[A, C], Pipeline[A, C]]) -> Node[A, Tuple[Any, ...]]:
        def combine(x: A) -> Tuple[Any, ...]:
            left = self.run(x)
            right = other.run(x) if isinstance(other, Pipeline) else other(x)
            
            left_tuple = left if isinstance(left, tuple) else (left,)
            right_tuple = right if isinstance(right, tuple) else (right,)
            
            return left_tuple + right_tuple
            
        return Node(combine)
        
    def run(self, x: A) -> B:
        return self.func(x)
        
    def unsafe_run(self, x: Optional[A] = None) -> B:
        return self.run(x)

def make_extract(f: Callable[[A], B]) -> Extract[A, B]:
    return Extract(f)

def make_transform(f: Callable[[A], B]) -> Transform[A, B]:
    return Transform(f)

def make_load(f: Callable[[A], B]) -> Load[A, B]:
    return Load(f)

E = Extract
T = Transform
L = Load
