# PipeStream

A high-performance, lock-free pipeline processing library for Rust that enables efficient data processing through configurable component chains. PipeStream provides both batch and streaming processing capabilities with strong typing, error handling, and concurrency support.

## Features

- **Composable Pipelines**: Build data processing pipelines by chaining components together
- **Streaming and Batch Processing**: Process data in batches or as continuous streams
- **Concurrent Processing**: Utilize multiple CPU cores with worker thread pools
- **Type Safety**: Strong typing throughout the pipeline ensures data consistency
- **Error Handling**: Comprehensive error management with error propagation
- **Backpressure Management**: Built-in mechanisms to handle flow control
- **Resource Pooling**: Object pools for efficient memory management
- **Resilience Patterns**: Circuit breakers, retries, and timeouts for robust processing
- **Thread-Safe**: Lock-free design maximizes performance in concurrent environments
- **Producer-Consumer Workflows**: Connect pipelines to create sophisticated data flows

## Getting Started

### Installation

Add PipeStream to your `Cargo.toml`:

```toml
[dependencies]
pipestream = "0.1.0"
```

### Basic Usage

Here's a simple example of creating and using a pipeline:

```rust
use pipestream::{
    common::BoxedError,
    component::base::PipelineComponent,
    pipeline::PipelineBuilder,
    stage::{PipelineStage, StageConfig},
};

// Define a component
#[derive(Debug)]
struct Tokenizer;
impl PipelineComponent<String, Vec<String>> for Tokenizer {
    fn process(&self, input: String) -> Result<Vec<String>, BoxedError> {
        Ok(input.split_whitespace().map(|s| s.to_string()).collect())
    }
}

// Define another component
#[derive(Debug)]
struct WordCounter;
impl PipelineComponent<Vec<String>, usize> for WordCounter {
    fn process(&self, input: Vec<String>) -> Result<usize, BoxedError> {
        Ok(input.len())
    }
}

fn main() {
    // Create pipeline stages
    let tokenizer = PipelineStage::new(Tokenizer, StageConfig::default());
    let counter = PipelineStage::new(WordCounter, StageConfig::default());

    // Build a pipeline
    let pipeline = PipelineBuilder::start_with(tokenizer)
        .then(counter)
        .build();

    // Process data
    let input = "Hello world! This is a test.".to_string();
    let result = pipeline.process(input).unwrap();

    println!("Word count: {}", result); // Outputs: Word count: 6
}
```

### Streaming Processing

PipeStream supports streaming data processing for handling continuous data flows:

```rust
use pipestream::{
    common::BoxedError,
    component::base::PipelineComponent,
    pipeline::PipelineBuilder,
    stage::{PipelineStage, StageConfig},
    streaming::PipelineStreamExt,
};
use crossbeam::channel;

// Define your components...

fn main() {
    // Build pipeline...
    let pipeline = PipelineBuilder::start_with(tokenizer)
        .then(counter)
        .build();

    // Create a streaming processor
    let processor = pipeline.streaming().create_processor::<String>();

    // Create input stream
    let inputs = vec![
        ("id1", "Hello world".to_string()),
        ("id2", "Processing streams".to_string()),
        ("id3", "With PipeStream".to_string()),
    ];

    // Process the stream (with buffer size 10)
    let output_receiver = processor.process_stream(inputs, 10);

    // Collect results
    while let Ok((id, result)) = output_receiver.recv() {
        match result {
            Ok(count) => println!("ID: {}, Word count: {}", id, count),
            Err(e) => println!("Error processing {}: {}", id, e),
        }
    }
}
```

## Architecture

PipeStream is built around these key abstractions:

- **Component**: A processing unit that transforms inputs to outputs
- **Stage**: A component wrapped in execution context (workers, queues, etc.)
- **Pipeline**: A sequence of connected stages that forms a processing chain

## Performance Considerations

PipeStream is designed for high performance:

- **Worker Pools**: Utilize all available CPU cores efficiently
- **Backpressure**: Manage workload to prevent overwhelming system resources
- **Object Pools**: Reduce allocation overhead for frequently used objects
- **Lock-Free Design**: Minimize contention between threads with lock-free data structures
- **Batch Processing**: Reduce per-item overhead through batched processing

## License

[MIT License](LICENSE)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
