use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use crossbeam::channel;
use pipestream::{
    common::LibResult,
    pipeline::PipelineBuilder,
    stage::{Stage, StageConfig, StageImpl},
    streaming::PipelineStreamExt,
};
use rand::{Rng, SeedableRng, rngs::StdRng};

fn generate_random_text(word_count: usize, rng: &mut StdRng) -> String {
    const WORDS: &[&str] = &[
        "the",
        "quick",
        "brown",
        "fox",
        "jumps",
        "over",
        "lazy",
        "dog",
        "hello",
        "world",
        "rust",
        "programming",
        "language",
        "pipeline",
        "processing",
        "system",
        "text",
        "analysis",
        "tokenizer",
        "stop",
        "words",
        "remover",
        "statistics",
        "performance",
        "benchmark",
        "test",
        "parallel",
        "concurrent",
        "sequential",
        "batch",
        "stream",
        "data",
        "input",
        "output",
        "result",
        "error",
        "success",
        "failure",
        "try",
        "catch",
        "handle",
        "exception",
        "function",
        "method",
        "class",
        "struct",
        "enum",
        "trait",
        "implementation",
        "module",
        "crate",
        "package",
    ];

    const PUNCTUATION: &[&str] = &[".", ",", "!", "?", ";", ":", "-", "(", ")", "\""];

    let mut text = String::new();

    for i in 0..word_count {
        // Add a word
        let word = WORDS[rng.random_range(0..WORDS.len())];
        text.push_str(word);

        // Potentially add punctuation (20% chance)
        if rng.random_bool(0.2) {
            let punct = PUNCTUATION[rng.random_range(0..PUNCTUATION.len())];
            text.push_str(punct);
        }

        // Add space if not the last word
        if i < word_count - 1 {
            text.push(' ');
        }
    }

    text
}

fn main() {
    #[derive(Debug, Clone)]
    struct Tokenizer;
    impl StageImpl<String, Vec<String>> for Tokenizer {
        fn process(&self, input: String) -> LibResult<Vec<String>> {
            let normalized = input
                .chars()
                .map(|c| {
                    if c.is_alphanumeric() || c.is_whitespace() {
                        c
                    } else {
                        ' '
                    }
                })
                .collect::<String>();

            Ok(normalized
                .split_whitespace()
                .map(|s| s.to_lowercase())
                .collect())
        }
    }

    #[derive(Debug, Clone)]
    struct StopWordRemover;
    impl StageImpl<Vec<String>, Vec<String>> for StopWordRemover {
        fn process(&self, input: Vec<String>) -> LibResult<Vec<String>> {
            // Common English stop words
            let stop_words: HashSet<&str> = [
                "a", "an", "the", "and", "but", "or", "for", "nor", "on", "at", "to", "from", "by",
                "in", "out", "is", "are", "am", "was", "were", "be", "been", "being", "have",
                "has", "had", "do", "does", "did", "of", "with", "this", "that", "these", "those",
                "it", "its", "they", "them", "their", "we", "us", "our", "i", "me", "my",
            ]
            .iter()
            .copied()
            .collect();

            Ok(input
                .into_iter()
                .filter(|word| !stop_words.contains(word.as_str()))
                .collect())
        }
    }

    #[derive(Debug, Clone)]
    struct TextStats {
        word_count: usize,
        unique_word_count: usize,
        avg_word_length: f64,
    }

    #[derive(Debug, Clone)]
    struct TextAnalyzer;
    impl StageImpl<Vec<String>, TextStats> for TextAnalyzer {
        fn process(&self, input: Vec<String>) -> LibResult<TextStats> {
            let word_count = input.len();
            let total_chars: usize = input.iter().map(|s| s.len()).sum();
            let avg_word_length = if word_count > 0 {
                total_chars as f64 / word_count as f64
            } else {
                0.0
            };

            let unique_words = input.iter().collect::<HashSet<_>>().len();

            Ok(TextStats {
                word_count,
                unique_word_count: unique_words,
                avg_word_length,
            })
        }
    }

    const NUM_JOBS: usize = 10_000; // Number of jobs to process
    const MIN_WORDS: usize = 800; // Minimum words per job
    const MAX_WORDS: usize = 1000; // Maximum words per job
    const MONITOR_INTERVAL_MS: u64 = 500; // How often to print status updates
    const INPUT_BUFFER_SIZE: usize = 100; // Size of input buffer
    const OUTPUT_BUFFER_SIZE: usize = 100; // Size of output buffer

    // Create a deterministic random number generator for reproducible tests
    let seed = 42;
    let mut rng = StdRng::seed_from_u64(seed);

    let tokenizer = Stage::new(Tokenizer, StageConfig::default());
    let stop_word_remover = Stage::new(StopWordRemover, StageConfig::default());
    let text_stats = Stage::new(TextAnalyzer, StageConfig::default());

    let pipeline = PipelineBuilder::start_with(tokenizer)
        .then(stop_word_remover)
        .then(text_stats)
        .build_blocking();

    // Create a streaming processor from our pipeline
    let processor = pipeline.streaming().create_processor::<usize>();

    // Create channels for input and output data
    let (input_sender, input_receiver) = channel::bounded(INPUT_BUFFER_SIZE);

    // Create shared state for monitoring
    let processed_count = Arc::new(Mutex::new(0));
    let processing_complete = Arc::new(Mutex::new(false));

    // Clone references for the monitoring thread
    let monitor_processed_count = Arc::clone(&processed_count);
    let monitor_complete = Arc::clone(&processing_complete);

    // Spawn monitoring thread
    let monitoring_thread = thread::spawn(move || {
        let start_time = Instant::now();
        let mut last_count = 0;

        println!("Starting monitoring thread...");

        loop {
            thread::sleep(Duration::from_millis(MONITOR_INTERVAL_MS));

            let current_count = *monitor_processed_count.lock().unwrap();
            let elapsed = start_time.elapsed().as_secs_f64();
            let is_complete = *monitor_complete.lock().unwrap();

            // Calculate processing rate
            let jobs_per_second = current_count as f64 / elapsed;
            let recent_rate =
                (current_count - last_count) as f64 / (MONITOR_INTERVAL_MS as f64 / 1000.0);
            last_count = current_count;

            // Print status update
            println!("\n--- Pipeline Status Update ---");
            println!("Time elapsed: {:.2}s", elapsed);
            println!(
                "Jobs processed: {}/{} ({:.2}%)",
                current_count,
                NUM_JOBS,
                (current_count as f64 / NUM_JOBS as f64) * 100.0
            );
            println!(
                "Processing rate: {:.2} jobs/sec (overall), {:.2} jobs/sec (recent)",
                jobs_per_second, recent_rate
            );

            if is_complete {
                println!("\nProcessing complete!");
                break;
            }
        }
    });

    // Spawn producer thread to generate inputs
    let producer_thread = thread::spawn(move || {
        println!("Starting input generator thread...");
        let start_gen = Instant::now();

        for id in 0..NUM_JOBS {
            let word_count = rng.random_range(MIN_WORDS..=MAX_WORDS);
            let text = generate_random_text(word_count, &mut rng);

            // Send the job to the processing pipeline
            if input_sender.send((id, text)).is_err() {
                // Receiver dropped, exit thread
                println!("Input channel closed, exiting producer");
                break;
            }

            // Add a small delay to simulate real-world data production
            if id % 100 == 0 {
                thread::sleep(Duration::from_millis(1));
            }
        }

        let gen_time = start_gen.elapsed();
        println!("Generated {} jobs in {:.2?}", NUM_JOBS, gen_time);

        // Close the sender, indicating no more data will be sent
        drop(input_sender);
    });

    // Process the stream on the main thread
    println!("Starting stream processing...");
    let start_time = Instant::now();

    // Process stream
    let output_receiver = processor.process_stream(input_receiver, OUTPUT_BUFFER_SIZE);

    // Collect results and update statistics
    let mut all_results = Vec::with_capacity(NUM_JOBS);

    while let Ok((id, result)) = output_receiver.recv() {
        all_results.push((id, result));

        // Update processed count for monitoring
        let mut count = processed_count.lock().unwrap();
        *count += 1;
    }

    // Signal processing is complete
    *processing_complete.lock().unwrap() = true;

    // Wait for monitoring thread to finish
    monitoring_thread.join().unwrap();

    // Wait for producer thread to finish
    producer_thread.join().unwrap();

    // Final report
    let total_time = start_time.elapsed();
    println!("\n=== Final Processing Report ===");
    println!("Total processing time: {:.2?}", total_time);
    println!(
        "Average processing rate: {:.2} jobs/second",
        NUM_JOBS as f64 / total_time.as_secs_f64()
    );

    // Calculate aggregate statistics from successful results
    let success_count = all_results.iter().filter(|r| r.1.is_ok()).count();

    if success_count > 0 {
        let mut total_words = 0;
        let mut total_unique_words = 0;
        let mut total_avg_length = 0.0;

        for (_, result) in all_results.iter() {
            if let Ok(stats) = result {
                total_words += stats.word_count;
                total_unique_words += stats.unique_word_count;
                total_avg_length += stats.avg_word_length;
            }
        }

        println!("\nAggregate Statistics:");
        println!("Successful jobs: {}/{}", success_count, NUM_JOBS);
        println!("Failed jobs: {}", NUM_JOBS - success_count);
        println!("Total words processed: {}", total_words);
        println!(
            "Average words per job: {:.2}",
            total_words as f64 / success_count as f64
        );
        println!(
            "Average unique words per job: {:.2}",
            total_unique_words as f64 / success_count as f64
        );
        println!(
            "Average word length: {:.2}",
            total_avg_length / success_count as f64
        );
    }
}
