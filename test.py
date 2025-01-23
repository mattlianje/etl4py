import unittest
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple, Callable, TypeVar
from etl4py import *

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

    def test_multiple_parallel(self) -> None:
      """Test combining more than two extractors with type hints"""
      get_user: Node[str, User] = Extract(
          lambda id: User(id=id, name=f"user_{id}")
      )
      get_stats: Node[str, UserStats] = Extract(
          lambda id: UserStats(posts=100, followers=1000)
      )
      get_extra: Node[str, str] = Extract(
          lambda id: f"extra_info_{id}"
      )
      
      combined = get_user & get_stats & get_extra
      result = combined("123")
      
      self.assertEqual(len(result), 3)
      self.assertEqual(result[0].id, "123")
      self.assertEqual(result[1].posts, 100)
      self.assertEqual(result[2], "extra_info_123")

    def test_nested_parallel_groups(self) -> None:
       """Test pipeline with nested transformations on parallel groups"""
       e1: Node[str, int] = Extract(lambda x: len(x))
       e2: Node[str, str] = Extract(lambda x: x.upper())
       e3: Node[str, str] = Extract(lambda x: x.lower())
       e4: Node[str, bool] = Extract(lambda x: len(x) > 5)
       
       t1: Node[int, int] = Transform(lambda x: x * 2)
       t2: Node[str, int] = Transform(lambda x: len(x))
       t3: Node[str, str] = Transform(lambda x: x + "!")
       t4: Node[bool, str] = Transform(lambda x: "yes" if x else "no")
       
       results: List[Any] = []
       final_load: Node[Tuple[int, int, str, str], None] = Load(
           lambda x: results.append({
               "doubled_length": x[0],
               "upper_length": x[1],
               "lower_with_excl": x[2],
               "is_long_text": x[3]
           })
       )
       
       pipeline = (
           (e1 >> t1) & 
           (e2 >> t2) & 
           (e3 >> t3) & 
           (e4 >> t4)
       ) >> final_load
       
       test_input = "Testing"
       pipeline.unsafe_run(test_input)
       
       self.assertEqual(len(results), 1)
       result = results[0]
       self.assertEqual(result["doubled_length"], 14)
       self.assertEqual(result["upper_length"], 7)
       self.assertEqual(result["lower_with_excl"], "testing!")
       self.assertEqual(result["is_long_text"], "yes")

    def test_reader_pipeline(self) -> None:
        """Test using Reader to create config-driven nodes"""
        @dataclass
        class ApiConfig:
            url: str
            api_key: str
        
        fetch_user: Reader[ApiConfig, Node[str, str]] = Reader(
            lambda config: Transform(
                lambda id: f"Fetched user {id} from {config.url}"
            )
        )
        
        save_user: Reader[ApiConfig, Node[str, str]] = Reader(
            lambda config: Load(
                lambda data: f"Saved {data} with key {config.api_key}"
            )
        )
        
        config = ApiConfig(url="api.example.com", api_key="123")
        pipeline = fetch_user.run(config) >> save_user.run(config)
        result = pipeline.unsafe_run("user_1")
        
        self.assertIn("api.example.com", result)
        self.assertIn("123", result)

    def test_node_composition(self) -> None:
        """Test composing nodes with | operator"""
        to_upper: Node[str, str] = Transform(lambda x: x.upper())
        add_prefix: Node[str, str] = Transform(lambda x: f"User_{x}")
        get_length: Node[str, int] = Transform(lambda x: len(x))
        
        get_user_length: Node[str, int] = add_prefix | to_upper | get_length
        result = get_user_length("alice")
        
        self.assertEqual(result, 10)
        
        parse_json: Node[str, Dict[str, Any]] = Transform(
            lambda x: {"name": x, "timestamp": "2024"}
        )
        get_name: Node[Dict[str, Any], str] = Transform(
            lambda x: x["name"]
        )
        
        get_json_name: Node[str, str] = parse_json | get_name
        
        name = get_json_name("alice")
        self.assertEqual(name, "alice")

    def test_function_composition(self) -> None:
      """Test composing nodes and pipelines using regular functions"""
      
      def get_name(user_id: str) -> str:
          return f"user_{user_id}"
          
      def get_length(name: str) -> int:
          return len(name)
      
      pipeline = Transform(get_name) | Transform(get_length)
      
      result = pipeline("123")
      self.assertEqual(result, 8)
 
    def test_retry_and_failure(self) -> None:
       """Test retry and failure handling capabilities"""
       
       attempts = 0
       def risky_transform(n: int) -> str:
           nonlocal attempts
           attempts += 1
           if attempts < 3:
               raise RuntimeError(f"Attempt {attempts} failed")
           return f"Success after {attempts} attempts"
       
       retry_config = RetryConfig(max_attempts=3, delay_ms=10)
       transform = Transform(risky_transform).with_retry(retry_config)
       
       result = transform(42)
       self.assertEqual(result, "Success after 3 attempts")
       self.assertEqual(attempts, 3)
       
    def test_failure_handling(self) -> None:
        """Test failure handling with onFailure"""
        
        def risky_extract(x: str) -> int:
            raise RuntimeError("Boom!")
        
        safe_extract = Extract(risky_extract).on_failure(
            lambda e: -1
        )
        
        result = safe_extract("test")
        self.assertEqual(result, -1)
        
    def test_retry_pipeline(self) -> None:
        """Test retry in a pipeline"""
        
        attempts = 0
        def risky_transform(n: int) -> str:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise RuntimeError(f"Attempt {attempts} failed")
            return f"Success after {attempts} attempts"
        
        pipeline = (
            Transform(lambda x: x + 1) >> 
            Transform(risky_transform)
        ).with_retry(RetryConfig(max_attempts=3, delay_ms=10))
        
        result = pipeline.unsafe_run(41)
        self.assertEqual(result, "Success after 3 attempts")
        self.assertEqual(attempts, 3)

if __name__ == '__main__':
    unittest.main()
