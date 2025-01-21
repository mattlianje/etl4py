# etl4py

**Powerful, whiteboard-style ETL**

A lightweight, zero-dependency library for writing type-safe data flows in Python 3.7+:

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
