# etl4py

**Powerful, whiteboard-style ETL**

A lightweight, zero-dependency library for writing beautiful ✨🍰, type-safe data flows in Python 3.7+:

```python
from etl4py import *

# Define your building blocks with type hints
five_extract: Extract[None, int]  = Extract(lambda _: 5)
double:       Transform[int, int] = Transform(lambda x: x * 2)
add_10:       Transform[int, int] = Transform(lambda x: x + 10)

# Compose nodes with `|`
double_then_add_10:  Transform[int, int] = double | add_10

console_load:        Load[int, None] = Load(lambda x: print(f"Result: {x}"))
db_load:             Load[int, None] = Load(lambda x: print(f"Saved to DB: {x}"))

# Create a pipeline by stitching Nodes with `>>`
pipeline: Pipeline[None, None] = \
        five_extract >> double_then_add_10 >> (console_load & db_load)

# Run at end of World
pipeline.unsafe_run()
```

This prints:
```
Result: 20
Saved to DB: 20
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

Three semantic type aliases that help teams share a common language:
- `Extract[-In, +Out]`
Conventionally used to start pipelines. Create parameter-less extracts that purely produce values like this: `Extract(lambda _: 5)`

- `Transform[-In, +Out]`
Conventionally used for intermediate transformations

- `Load[-In, +Out]`
Conventionally used for pipeline endpoints

### Of note...
- At its core, **etl4py** just wraps pure*ish* (this is Python after all, not in a bad way) functions ... with a few added niceties like chaining, composition,
keeping infrastructure concerns separate from your dataflows (Reader), and shorthand for grouping parallelizable tasks.
- Chaotic, framework/infra-coupled ETL codebases that grow without an imposed discipline drive dev-teams and data-orgs to their knees.
- **etl4py** is a little DSL to enforce discipline, type-safety and re-use of pure functions - and see [functional ETL](https://maximebeauchemin.medium.com/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a) for what it is... and could be.


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

### Error/Retry Handling
Handle failures gracefully using `with_retry` or `on_failure` on any Node of Pipeline:
```python
from etl4py import *

def risky_operation(data: Dict) -> str:
    if random.random() < 0.5:
        raise RuntimeError("Random failure")
    return json.dumps(data)

# Add retry with backoff
resilient_transform = Transform(risky_operation).with_retry(
    RetryConfig(
        max_attempts=3,
        delay_ms=100,
        backoff_factor=2
    )
)

# Add fallback handling
safe_transform = resilient_transform.on_failure(
    lambda error: "{'status': 'failed'}"
)
```

### Re-usable patterns
Create compositional and re-usable patterns:
```python
# Generic validation node
def create_validator(predicate: Callable[[T], bool], error_msg: str) -> Node[T, T]:
    return Transform(lambda x: x if predicate(x) else throw(ValueError(error_msg)))

# Composable logging pattern
def with_logging(node: Node[T, U], logger: Logger) -> Node[T, U]:
    return Transform(lambda x: logger.info(f"Input: {x}") or x) >> \
           node >> \
           Transform(lambda x: logger.info(f"Output: {x}") or x)
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

config = ApiConfig(url="https://api.example.com", api_key="secret123")
pipeline = (fetch_user.run(config) & fetch_posts.run(config)) >> Transform(
    lambda data: {"user": data[0], "posts": data[1]}
)

result = pipeline.unsafe_run("user_123")
```

## More examples
#### Use etl4py to structure ML workflows
```python
from etl4py import *
from dataclasses import dataclass
from typing import Tuple
import torch
from torch import nn, optim
from torch.utils.data import Dataset, DataLoader
import numpy as np

# Simple synthetic dataset
class SyntheticDataset(Dataset):
    def __init__(self, num_samples=1000):
        self.data = torch.randn(num_samples, 10)
        self.labels = torch.randint(0, 2, (num_samples,))
    
    def __len__(self):
        return len(self.data)
    
    def __getitem__(self, idx):
        return self.data[idx], self.labels[idx]

# Simple model
class SimpleModel(nn.Module):
    def __init__(self):
        super().__init__()
        self.layers = nn.Sequential(
            nn.Linear(10, 5),
            nn.ReLU(),
            nn.Linear(5, 2)
        )
    
    def forward(self, x):
        return self.layers(x)

@dataclass
class TrainingConfig:
    batch_size: int
    learning_rate: float
    epochs: int
    device: str = "cuda" if torch.cuda.is_available() else "cpu"

def create_training_pipeline(config: TrainingConfig) -> Pipeline[None, nn.Module]:
    # Data loading
    load_dataset = Extract(lambda _: SyntheticDataset())
    create_loader = Transform(
        lambda dataset: DataLoader(dataset, batch_size=config.batch_size)
    )
    
    def setup_model(_):
        model = SimpleModel().to(config.device)
        optimizer = optim.Adam(model.parameters(), lr=config.learning_rate)
        criterion = nn.CrossEntropyLoss()
        return model, optimizer, criterion
    
    init_training = Transform(setup_model)
    
    def train_model(state: Tuple[nn.Module, optim.Optimizer, nn.Module]) -> nn.Module:
        model, optimizer, criterion = state
        
        for epoch in range(config.epochs):
            model.train()
            running_loss = 0.0
            
            for i, (inputs, labels) in enumerate(train_loader):
                inputs = inputs.to(config.device)
                labels = labels.to(config.device)
                
                optimizer.zero_grad()
                outputs = model(inputs)
                loss = criterion(outputs, labels)
                loss.backward()
                optimizer.step()
                
                running_loss += loss.item()
                if i % 100 == 99:
                    print(f'Epoch {epoch + 1}, Batch {i + 1}, Loss: {running_loss / 100:.3f}')
                    running_loss = 0.0
                    
        return model
    
    train = Transform(train_model)

    save_model = Load(lambda model: torch.save(
        model.state_dict(), 
        'model_checkpoint.pt'
    ))
    
    return (
        load_dataset >>
        create_loader >>
        init_training >>
        train >>
        save_model
    )

config = TrainingConfig(
        batch_size=32,
        learning_rate=0.001,
        epochs=5
)

# Create and run pipeline
pipeline = create_training_pipeline(config)
train_loader = None

model = pipeline.unsafe_run(None)
print("Training complete! Model saved to model_checkpoint.pt")
```

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
spark_pipeline = create_spark_pipeline(config)
spark_pipeline.unsafe_run(None)
```

## Inspiration
- This is a port of my [etl4s](https://github.com/mattlianje/etl4s) Scala library.
