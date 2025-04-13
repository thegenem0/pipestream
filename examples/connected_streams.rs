use std::{
    thread,
    time::{Duration, Instant},
};

use crossbeam::channel;
use pipestream::{
    common::BoxedError,
    component::base::PipelineComponent,
    pipeline::PipelineBuilder,
    stage::{PipelineStage, StageConfig},
    stream::PipelineStreamExt,
};

// First pipeline component - generates data
#[derive(Debug, Clone)]
struct NumberGenerator;
impl PipelineComponent<usize, Vec<i32>> for NumberGenerator {
    fn process(&self, count: usize) -> Result<Vec<i32>, BoxedError> {
        // Generate sequence from 1 to count
        let result = (1..=count as i32).collect();

        // Simulate some processing time
        thread::sleep(Duration::from_millis(20));

        println!("Generated {} numbers", count);
        Ok(result)
    }
}

// Second pipeline component - processes data
#[derive(Debug, Clone)]
struct NumberProcessor;
impl PipelineComponent<Vec<i32>, i32> for NumberProcessor {
    fn process(&self, numbers: Vec<i32>) -> Result<i32, BoxedError> {
        // Calculate sum of squares
        let result: i32 = numbers.iter().map(|&n| n * n).sum();

        // Simulate processing time
        thread::sleep(Duration::from_millis(30));

        println!("Processed {} numbers, result: {}", numbers.len(), result);
        Ok(result)
    }
}

fn main() {
    // Create the pipelines
    let generator_pipeline =
        PipelineBuilder::start_with(PipelineStage::new(NumberGenerator, StageConfig::default()))
            .build();

    let processor_pipeline =
        PipelineBuilder::start_with(PipelineStage::new(NumberProcessor, StageConfig::default()))
            .build();

    // Create the streaming processors
    let generator_processor = generator_pipeline.streaming().create_processor::<usize>();
    let processor_processor = processor_pipeline.streaming().create_processor::<usize>();

    // Channel to connect the two pipelines
    let (connector_tx, connector_rx) = channel::bounded(5);

    // Channel for consumer to send results back to main thread
    let (results_tx, results_rx) = channel::unbounded();

    // Start timer
    let start = Instant::now();
    println!("Starting producer-consumer pipeline processing...");

    // Start the producer thread (first pipeline)
    let producer_thread = thread::spawn(move || {
        // Input data for producer pipeline
        let inputs = vec![
            (1, 5),  // Generate 5 numbers
            (2, 10), // Generate 10 numbers
            (3, 15), // Generate 15 numbers
            (4, 20), // Generate 20 numbers
        ];

        println!("Producer thread started with {} jobs", inputs.len());

        // Process each input individually - more reliable than stream
        for (id, count) in inputs {
            match generator_processor.process(count) {
                Ok(numbers) => {
                    println!(
                        "Producer output [{}]: Generated {} numbers",
                        id,
                        numbers.len()
                    );

                    // Send to consumer pipeline through the channel
                    if connector_tx.send((id, numbers)).is_err() {
                        println!("Consumer channel closed, exiting producer");
                        break;
                    }
                }
                Err(e) => println!("Producer error [{}]: {}", id, e),
            }
        }

        println!("Producer thread completed");
        // Sender is dropped here, which will close the channel for the consumer
    });

    // Start the consumer thread (second pipeline)
    let consumer_thread = thread::spawn(move || {
        println!("Consumer thread started");

        // Process inputs coming from the producer channel
        let mut results = Vec::new();

        // Keep receiving from the channel until it's closed
        while let Ok((id, numbers)) = connector_rx.recv() {
            println!("Consumer received [{}]: {} numbers", id, numbers.len());

            // Process through the pipeline
            match processor_processor.process(numbers) {
                Ok(sum_of_squares) => {
                    println!(
                        "Consumer output [{}]: Sum of squares = {}",
                        id, sum_of_squares
                    );
                    results.push((id, sum_of_squares));
                }
                Err(e) => println!("Consumer error [{}]: {}", id, e),
            }
        }

        println!("Consumer thread completed with {} results", results.len());

        // Send results back to main thread
        let _ = results_tx.send(results);
    });

    // Wait for producer thread to complete
    producer_thread.join().unwrap();
    println!("Producer thread joined");

    // Wait for consumer thread to complete and get results
    consumer_thread.join().unwrap();
    println!("Consumer thread joined");

    // Receive results from the consumer thread
    let results = results_rx.recv().unwrap_or_else(|_| Vec::new());

    // Report results
    println!("\n=== Final Results ===");
    println!("Total processing time: {:?}", start.elapsed());
    println!("Total items processed: {}", results.len());

    // Calculate sum of all results
    let grand_total: i32 = results.iter().map(|(_, sum)| sum).sum();
    println!("Grand total of all sums of squares: {}", grand_total);
}
