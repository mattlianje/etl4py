import unittest
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple, Callable, TypeVar
from etl4py import Extract, Transform, Load, Pipeline, Node

A = TypeVar('A')
B = TypeVar('B')

@dataclass
class User:
    id: str
    name: str

@dataclass
class UserStats:
    posts: int
    followers: int

class TestPipeline(unittest.TestCase):
    def test_arithmetic(self) -> None:
        """Test simple numeric pipeline with type hints"""
        e: Node[None, int] = Extract(lambda _: 5)
        t1: Node[int, int] = Transform(lambda x: x * 2)
        t2: Node[int, int] = Transform(lambda x: x + 10)

        results: List[int] = []
        l: Node[int, None] = Load(lambda x: results.append(x))

        p = e >> t1 >> t2 >> l
        p.unsafe_run()

        self.assertEqual(results[0], 20)

    def test_parallel_extract(self) -> None:
        """Test combining two extractors with type hints"""
        get_user: Node[str, User] = Extract(
            lambda id: User(id=id, name=f"user_{id}")
        )

        get_stats: Node[str, UserStats] = Extract(
            lambda id: UserStats(posts=100, followers=1000)
        )

        combined = get_user & get_stats
        result = combined("123")

        self.assertEqual(result[0].id, "123")
        self.assertEqual(result[1].posts, 100)

    def test_full_pipeline(self) -> None:
        """Test complete pipeline with parallel operations"""
        get_user: Node[str, User] = Extract(
            lambda id: User(id=id, name=f"user_{id}")
        )
        get_stats: Node[str, UserStats] = Extract(
            lambda id: UserStats(posts=100, followers=1000)
        )

        format_data: Node[Tuple[User, UserStats], Dict[str, Any]] = Transform(
            lambda t: {
                "user": t[0],
                "stats": t[1],
                "score": t[1].posts * 2
            }
        )

        results: List[Any] = []
        save: Node[Dict[str, Any], None] = Load(
            lambda x: results.append(x)
        )
        log: Node[Dict[str, Any], None] = Load(
            lambda x: results.append(f"processed: {x['user'].id}")
        )

        p = (get_user & get_stats) >> format_data >> (save & log)
        result = p.unsafe_run("123")

        self.assertEqual(results[0]["user"].id, "123")
        self.assertEqual(results[0]["stats"].posts, 100)
        self.assertEqual(results[1], "processed: 123")

if __name__ == '__main__':
    unittest.main()
