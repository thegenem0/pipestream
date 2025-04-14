use std::{collections::HashSet, time::Instant};

use pipestream::{
    common::LibResult,
    pipeline::PipelineBuilder,
    stage::{Stage, StageConfig, StageImpl},
};

#[derive(Debug, Clone)]
struct TextStats {
    word_count: usize,
    unique_word_count: usize,
    avg_word_length: f64,
}

fn main() {
    #[derive(Debug)]
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

    #[derive(Debug)]
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

    #[derive(Debug)]
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

    let tokenizer = Stage::new(Tokenizer, StageConfig::default());
    let stop_word_remover = Stage::new(StopWordRemover, StageConfig::default());
    let text_stats = Stage::new(TextAnalyzer, StageConfig::default());

    let pipeline = PipelineBuilder::start_with(tokenizer)
        .then(stop_word_remover)
        .then(text_stats)
        .build_blocking();

    let start = Instant::now();

    let results = pipeline.process_batch(get_test_batch());

    for (id, result) in &results {
        match result {
            Ok(_stats) => {}
            Err(e) => println!("Job {}: Error: {}", id, e),
        }
    }

    println!("\n=== Stats ===\n");
    println!("Total time elapsed: {:?}\n", start.elapsed());
    println!("Total jobs processed: {}\n", results.len());
    println!(
        "Average time per job: {:?}\n",
        start.elapsed() / results.len().try_into().unwrap()
    );
    println!(
        "Throughput: {:.2} jobs/second\n",
        results.len() as f64 / start.elapsed().as_secs_f64()
    );
}

fn get_test_batch() -> Vec<(usize, String)> {
    let mut batch = Vec::with_capacity(100);

    let text = "
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla venenatis suscipit massa, vestibulum congue nulla ullamcorper quis. Quisque et condimentum arcu, nec maximus turpis. Sed rhoncus in justo et ultrices. Suspendisse eleifend porta ex, ut pulvinar turpis congue eu. Nulla in sem vel sapien consequat mollis non et lorem. Pellentesque tempor rhoncus mi, ultrices dapibus lacus porta ut. Interdum et malesuada fames ac ante ipsum primis in faucibus. Nunc venenatis quis metus et feugiat. Ut consequat eget velit vitae lacinia. Vivamus euismod lobortis lorem non luctus. Mauris maximus elit enim, et sodales est lobortis at. Curabitur dapibus, orci id fringilla varius, ante turpis dictum sem, in auctor quam augue at enim. Maecenas eget purus purus. Nunc facilisis justo nec convallis commodo.
".to_string();

    for id in 0..10_000 {
        batch.push((id, text.clone()));
    }

    batch
}
