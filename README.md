# etl4py

**Powerful, whiteboard-style ETL**

A lightweight, zero-dependency library for writing beautiful, type-safe data flows in Python 3.7+:

```python
from etl4py import *

# Define your building blocks with type hints
five_extract: Extract[None, int]  = Extract(lambda _: 5)
double:       Transform[int, int] = Transform(lambda x: x * 2)
add_10:       Transform[int, int] = Transform(lambda x: x + 10)

console_load: Load[int, None] = Load(lambda x: print(f"Result: {x}"))
db_load:      Load[int, None] = Load(lambda x: print(f"Saved to DB: {x}"))

# Create a pipeline by stitching Nodes
pipeline: Pipeline[None, None] = \
        five_extract >> double >> add_10 >> (console_load & db_load)

# Run at end of World
pipeline.unsafe_run()
```

This prints:
```
Result: 20
Saved to DB: 20
```

## Core Concepts

**etl4py** has two fundamental building blocks:

#### `Pipeline[-In, +Out]`
A complete pipeline composed of nodes chained with `>>`. Takes type `In` and produces `Out` when run:
- Use `unsafe_run()` for "run-or-throw" behavior
- Fully type-safe: won't compile if types don't match
- Composable with other pipelines

#### `Node[-In, +Out]`
The base abstraction. All nodes, regardless of type, can be:
- Composed with `|` to create new nodes
- Grouped with `&` for parallel operations
- Connected with `>>` to form pipelines

Three semantic type aliases that help teams share a common language:
- `Extract[-In, +Out]`
Conventionally used to start pipelines. Create parameter-less extracts: `Extract(lambda _: 5)`

- `Transform[-In, +Out]`
Conventionally used for intermediate transformations

- `Load[-In, +Out]`
Conventionally used for pipeline endpoints

### Of note...

* At its core, **etl4py** just wraps pure(ish) functions (this is Python after all) ... with a few added niceties like chaining, composition,
keeping infrastructure concerns separate from your dataflows (Reader), and shorthand for grouping parallelizable tasks.

* The problem is ETL/OLAP codebases grow into unmaintainable tangles of: framework-specific code that's hard to test, database logic mixed with business rules,
and crazy logic doing the splits between your scheduler and domain logic

The goal isn't to replace your ETL framework - it's to give you a clean, type-safe way to express data flows that your whole team can understand.


### Compose Nodes
Use `|` to create reusable nodes:
```python
def get_name(user_id: str) -> str:
    return f"user_{user_id}"
    
def get_length(name: str) -> int:
    return len(name)

# Compose nodes into a new node
name_length = Transform(get_name) | Transform(get_length)
result = name_length("123")  # Returns 8 (length of "user_123")
```

### Parallel Operations
Use `&` to run operations (nodes or pipelines) in parallel:
```python
stats = get_user_stats & get_user_posts  # Returns tuple of results
pipeline = stats >> process_results
```

### Error Handling
Handle failures gracefully:
```python
safe_extract = risky_extract.on_failure(
    lambda error: f"Failed: {error}"
)
```

### Automatic Retries
Add retry capability to any node or pipeline:
```python
from etl4py import RetryConfig

resilient_node = risky_node.with_retry(
    RetryConfig(max_attempts=3, delay_ms=100)
)
```

### Config-Driven Pipelines
Use the built in Reader monad to make true config-driven pipelines:
```python
fetch_user = Reader[ApiConfig, Node[str, str]](
    lambda config: Transform(
        lambda id: f"Fetching from {config.url}/{id}"
    )
)

# Use with config
node = fetch_user.run(api_config)
```

## Inspiration
- This is a port of my [etl4s](https://github.com/mattlianje/etl4s) Scala library.
