<p align="center">
  <img src="pix/etl4py-screenshot.png" width="700">
</p>

# etl4py

**Powerful, whiteboard-style ETL**

A lightweight, zero-dependency library for writing beautiful ✨🍰, type-safe data flows in Python 3.7+:

```python
from etl4py import *

# Define your building blocks
five_extract: Extract[None, int]  = Extract(lambda _: 5)
double:       Transform[int, int] = Transform(lambda x: x * 2)
add_10:       Transform[int, int] = Transform(lambda x: x + 10)

attempts = 0
def risky_transform(x: int) -> int:
   global attempts; attempts += 1
   if attempts <= 2: raise RuntimeError(f"Failed {attempts}")
   return x

# Compose nodes with `|`
double_then_add_10: Transform[int, int] = double | add_10

# Add retries/failure handling
risky_node:   Transform[int, int] = Transform(risky_transform)\
                                    .with_retry(RetryConfig(max_attempts=3, delay_ms=100))

console_load: Load[int, None] = Load(lambda x: print(f"Result: {x}"))
db_load:      Load[int, None] = Load(lambda x: print(f"Saved to DB: {x}"))

# Stitch your pipline with >>
pipeline: Pipeline[None, None] = \
     five_extract >> double_then_add_10 >> risky_node >> (console_load & db_load)

# Run your pipeline at the end of the World
pipeline.unsafe_run()
```

This prints:
```
Result: 20
Saved to DB: 20
```

## Features
- Type-safe pipelines with full mypy support
- Zero-dependency: Drop etl4py.py into any Python project
- Effortless task grouping with & operator
- Built-in retry handling and error recovery
- First-class pipeline composition with >>
- Everything is just wrapped pure functions under the hood

## Get started
**etl4py** is on PyPi:
```
pip install etl4py
```
Or try it in your REPL:
```
python -i <(curl -sL https://raw.githubusercontent.com/mattlianje/etl4py/master/etl4py.py)
```

## Core Concepts

**etl4py** has two building blocks:

#### `Pipeline[-In, +Out]`
A complete pipeline composed of nodes chained with `>>`. Takes type `In` and produces `Out` when run:
- Use `unsafe_run()` for "run-or-throw" behavior
- Fully type-safe: won't compile if types don't match (use [mypy](https://github.com/python/mypy))
- Chain pipelines with `>>`

#### `Node[-In, +Out]`
The base abstraction. All nodes, regardless of type, can be:
- Composed with `|` to create new nodes
- Grouped with `&` for parallel operations
- Connected with `>>` to form pipelines

3 semantic type aliases help you express the intent of your dataflows, but all nodes are just function wrappers:
- `Extract[-In, +Out]`
Conventionally used to start pipelines. Create parameter-less extracts that purely produce values like this: `Extract(lambda _: 5)`

- `Transform[-In, +Out]`
Conventionally used for intermediate transformations

- `Load[-In, +Out]`
Conventionally used for pipeline endpoints

### Of note...
- At its core, **etl4py** just wraps pure-*ish* (this is Python after all, not in a bad way) functions ... with a few added niceties like chaining, composition,
keeping infrastructure concerns separate from your dataflows (Reader), and shorthand for grouping parallelizable tasks.
- Chaotic, framework/infra-coupled ETL codebases that grow without an imposed discipline drive dev-teams and data-orgs to their knees.
- **etl4py** is a little DSL to enforce discipline, type-safety and re-use of pure functions - and see [functional ETL](https://maximebeauchemin.medium.com/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a) for what it is... and could be.


### Compose Nodes
Use `|` to do reverse composition of nodes (↦). Think of it as "andThen":
```python
from etl4py import *

def add_prefix(x: int) -> str:
   return f"ID_{x}"

def calculate_hash(s: str) -> int:
   return hash(s)

pipeline = Pipeline(
   lambda x: (Transform(add_prefix) | Transform(calculate_hash))(x)
) >> Load(lambda x: print(f"Hash: {x}"))

pipeline.unsafe_run(42)  # Prints hash of "ID_42"
```

## Chain pipelines
Chain pipelines with `>>`
```python
from etl4py import *

# Pipeline 1: Double then add 5
p1: Pipeline[int, int] = Transform(lambda x: x * 2) >> Transform(lambda x: x + 5)

# Pipeline 2: Triple then subtract 2
p2: Pipeline[int, int] = Transform(lambda x: x * 3) >> Transform(lambda x: x - 2)

# Stiched pipeline
pipeline = p1 >> p2 >> Load(lambda x: print(f"Result: {x}"))
pipeline.unsafe_run(5)  # Result: 43
```

### Parallel Operations
Use `&` to run operations (nodes or pipelines) in parallel:
```python
from etl4py import *

def save_to_db(x: int) -> str:
   return f"Saved {x} to DB"
   
def notify_slack(x: int) -> str:
   return f"Notified Slack about {x}"

pipeline = Pipeline(
   lambda x: (Load(save_to_db) & Load(notify_slack))(x * 2)
)
pipeline.unsafe_run(42)  # Returns: ("Saved 84 to DB", "Notified Slack about 84")
```

### Error/Retry Handling
Handle failures gracefully using `with_retry` or `on_failure` on any Node of Pipeline:
```python
from etl4py import *

def always_fails(_):
   raise RuntimeError("I never work!")

pipeline = (
   Transform(lambda x: {"value": x})
   >> Transform(always_fails)
   >> Load(lambda x: print(f"Result: {x}"))
).with_retry(
   RetryConfig(max_attempts=3, delay_ms=100)
).on_failure(
   lambda err: "{'status': 'failed'}"
)

pipeline.unsafe_run(42)  # Will always hit fallback after 3 retries
```

### Re-usable patterns
Create compositional and re-usable patterns:
```python
from etl4py import *
import logging

# Re-usuable + composable logging pattern
def with_logging(node: Node[T, U], logger: Logger) -> Node[T, U]:
   return Transform(lambda x: logger.info(f"Input: {x}") or x) >> \
          node >> \
          Transform(lambda x: logger.info(f"Output: {x}") or x)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Re-usable validator
def my_validator(x):
   if x <= 0:
       raise ValueError("Must be positive")
   return x

pipeline = (
   with_logging(Transform(my_validator), logger)
   >> Transform(lambda x: x * 2) 
   >> Load(lambda x: print(f"Result: {x}"))
).with_retry(RetryConfig(max_attempts=2))

pipeline.unsafe_run(25)  # Logs validation input/output, prints Result: 50
pipeline.unsafe_run(-1)  # Logs validation attempt, raises ValueError
```

### Config-Driven Pipelines
Use the built in Reader monad to make true config-driven pipelines:
```python
from etl4py import *
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class ApiConfig:
    url: str
    api_key: str

# Define config-driven nodes using Reader
fetch_user = Reader[ApiConfig, Node[str, Dict]](
    lambda config: Transform(
        lambda user_id: {
            "id": user_id,
            "source": f"{config.url}/users/{user_id}",
            "api_key": config.api_key[:4] + "..."
        }
    )
)

fetch_posts = Reader[ApiConfig, Node[str, List[Dict]]](
    lambda config: Transform(
        lambda user_id: [
            {"id": 1, "title": "First Post"},
            {"id": 2, "title": "Second Post"}
        ]
    )
)

merge_data: Transform[Tuple[Dict, List[Dict]], Dict] = Transform(
   lambda data: {"user": data[0], "posts": data[1]}
)

config = ApiConfig(url="https://api.example.com", api_key="secret123")
pipeline = (fetch_user.run(config) & fetch_posts.run(config)) >> merge_data

result = pipeline.unsafe_run("user_123")
```

## More examples

#### Use etl4py to structure your PySpark apps
```python
from etl4py import *
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, array, struct, lit
from typing import List

@dataclass
class SparkConfig:
    master: str
    app_name: str

def create_dummy_data(spark: SparkSession) -> DataFrame:
    data = [
        (1, [
            {"type": "click", "value": 10},
            {"type": "view", "value": 20}
        ]),
        (2, [
            {"type": "click", "value": 15},
            {"type": "view", "value": 25}
        ]),
        (3, [
            {"type": "click", "value": 5},
            {"type": "view", "value": 30}
        ])
    ]
    
    return spark.createDataFrame(
        data,
        "id INTEGER, events ARRAY<STRUCT<type: STRING, value: INTEGER>>"
    )

def create_spark_pipeline(config: SparkConfig) -> Pipeline[None, None]:
    spark_init = Extract(lambda _: SparkSession.builder
        .master(config.master)
        .appName(config.app_name)
        .getOrCreate())
    
    load_data = Transform(lambda spark: create_dummy_data(spark))
    
    process_events = Transform(lambda df: df
        .select(explode(col("events")).alias("event"))
        .groupBy("event.type")
        .agg({"event.value": "sum"})
        .orderBy("type"))
    
    show_results = Load(lambda df: (
        print("\n=== Processing Results ==="),
        df.show(),
        print("========================\n")
    ))
    
    return (
        spark_init >>
        load_data >>
        process_events >>
        show_results
    )

config = SparkConfig(
        master="local[*]",
        app_name="etl4py_example"
)

# Create and run pipeline
spark_pipeline: Pipeline[None, None] = create_spark_pipeline(config)
spark_pipeline.unsafe_run(None)
```

## Inspiration
- This is a port of my [etl4s](https://github.com/mattlianje/etl4s) Scala library.
