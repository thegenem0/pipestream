use crate::common::{BoxedError, IOParam};
use std::fmt::Debug;

pub trait PipelineComponent<I: IOParam, O: IOParam>: Send + Sync + Debug {
    fn process(&self, input: I) -> Result<O, BoxedError>;

    fn name(&self) -> String {
        std::any::type_name::<Self>()
            .split("::")
            .last()
            .unwrap_or("Unknown")
            .to_string()
    }
}

impl<I, O> PipelineComponent<I, O> for Box<dyn PipelineComponent<I, O> + Send + Sync>
where
    I: IOParam + Clone,
    O: IOParam,
{
    fn process(&self, input: I) -> Result<O, BoxedError> {
        (**self).process(input)
    }

    fn name(&self) -> String {
        (**self).name()
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::sync::{Arc, Mutex};
//
//     // Test input and output types
//     #[derive(Debug, Clone, PartialEq)]
//     struct TestInput(usize);
//
//     #[derive(Debug, Clone, PartialEq)]
//     struct TestMiddle(String);
//
//     #[derive(Debug, Clone, PartialEq)]
//     struct TestOutput(f64);
//
//     // Basic component implementation for testing
//     #[derive(Debug)]
//     struct TestComponent {
//         name: String,
//         multiplier: usize,
//     }
//
//     impl TestComponent {
//         fn new(name: &str, multiplier: usize) -> Self {
//             Self {
//                 name: name.to_string(),
//                 multiplier,
//             }
//         }
//     }
//
//     impl PipelineComponent<TestInput, TestOutput> for TestComponent {
//         fn process(&self, input: TestInput) -> Result<TestOutput, BoxedError> {
//             Ok(TestOutput((input.0 * self.multiplier) as f64))
//         }
//
//         fn name(&self) -> String {
//             self.name.clone()
//         }
//     }
//
//     // Test component that can fail
//     #[derive(Debug)]
//     struct FailingComponent {
//         name: String,
//         fail_on: usize,
//     }
//
//     impl FailingComponent {
//         fn new(name: &str, fail_on: usize) -> Self {
//             Self {
//                 name: name.to_string(),
//                 fail_on,
//             }
//         }
//     }
//
//     impl PipelineComponent<TestInput, TestOutput> for FailingComponent {
//         fn process(&self, input: TestInput) -> Result<TestOutput, BoxedError> {
//             if input.0 == self.fail_on {
//                 Err(format!("Failed on input {}", input.0).into())
//             } else {
//                 Ok(TestOutput(input.0 as f64))
//             }
//         }
//
//         fn name(&self) -> String {
//             self.name.clone()
//         }
//     }
//
//     // Test the default name implementation
//     #[test]
//     fn test_default_name() {
//         struct SimpleComponent;
//
//         impl PipelineComponent<TestInput, TestOutput> for SimpleComponent {
//             fn process(&self, input: TestInput) -> Result<TestOutput, BoxedError> {
//                 Ok(TestOutput(input.0 as f64))
//             }
//             // Use default name implementation
//         }
//
//         impl Debug for SimpleComponent {
//             fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//                 f.debug_struct("SimpleComponent").finish()
//             }
//         }
//
//         let component = SimpleComponent;
//         assert_eq!(component.name(), "SimpleComponent");
//     }
//
//     // Test ComponentFn
//     #[test]
//     fn test_component_fn_basic() {
//         let component = ComponentFn::new(|input: TestInput| -> Result<TestOutput, BoxedError> {
//             Ok(TestOutput(input.0 as f64 * 2.0))
//         });
//
//         let result = component.process(TestInput(5));
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), TestOutput(10.0));
//     }
//
//     #[test]
//     fn test_component_fn_with_id() {
//         let component = ComponentFn::new(|input: TestInput| -> Result<TestOutput, BoxedError> {
//             Ok(TestOutput(input.0 as f64))
//         })
//         .with_id(42);
//
//         let result = component.process(TestInput(5));
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), TestOutput(5.0));
//
//         // ID doesn't affect processing, just serves as metadata
//         let debug_str = format!("{:?}", component);
//         assert!(debug_str.contains("ComponentFn"));
//     }
//
//     #[test]
//     fn test_component_fn_error() {
//         let component = ComponentFn::new(|input: TestInput| -> Result<TestOutput, BoxedError> {
//             if input.0 > 10 {
//                 Err("Input too large".into())
//             } else {
//                 Ok(TestOutput(input.0 as f64))
//             }
//         });
//
//         // Test success case
//         let result = component.process(TestInput(5));
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), TestOutput(5.0));
//
//         // Test error case
//         let result = component.process(TestInput(15));
//         assert!(result.is_err());
//         let err = result.unwrap_err();
//         assert_eq!(err.to_string(), "Input too large");
//     }
//
//     #[test]
//     fn test_component_fn_name() {
//         let component =
//             ComponentFn::<TestInput, TestOutput, _>::new(|input| Ok(TestOutput(input.0 as f64)));
//
//         let name = component.name();
//         assert!(name.contains("ComponentFn"));
//         assert!(name.contains("TestInput"));
//         assert!(name.contains("TestOutput"));
//     }
//
//     #[test]
//     fn test_component_fn_debug() {
//         let component =
//             ComponentFn::<TestInput, TestOutput, _>::new(|input| Ok(TestOutput(input.0 as f64)));
//
//         let debug_str = format!("{:?}", component);
//         assert!(debug_str.contains("ComponentFn"));
//         assert!(debug_str.contains("name"));
//     }
//
//     #[test]
//     fn test_component_fn_clone() {
//         let component = ComponentFn::new(|input: TestInput| -> Result<TestOutput, BoxedError> {
//             Ok(TestOutput(input.0 as f64 * 2.0))
//         });
//
//         let cloned = component; // Move semantics due to Copy trait
//
//         let result = cloned.process(TestInput(5));
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), TestOutput(10.0));
//     }
//
//     // Test mutable state with ComponentFn
//     #[test]
//     fn test_component_fn_with_state() {
//         let counter = Arc::new(Mutex::new(0));
//         let counter_clone = Arc::clone(&counter);
//
//         let component =
//             ComponentFn::new(move |input: TestInput| -> Result<TestOutput, BoxedError> {
//                 let mut count = counter_clone.lock().unwrap();
//                 *count += 1;
//                 Ok(TestOutput(input.0 as f64 * (*count as f64)))
//             });
//
//         // First call: counter = 1
//         let result = component.process(TestInput(5));
//         assert_eq!(result.unwrap(), TestOutput(5.0 * 1.0));
//
//         // Second call: counter = 2
//         let result = component.process(TestInput(5));
//         assert_eq!(result.unwrap(), TestOutput(5.0 * 2.0));
//
//         // Third call: counter = 3
//         let result = component.process(TestInput(5));
//         assert_eq!(result.unwrap(), TestOutput(5.0 * 3.0));
//
//         assert_eq!(*counter.lock().unwrap(), 3);
//     }
//
//     // Test Chain component
//     #[test]
//     fn test_chain_basic() {
//         // First component: multiply by 2
//         let first = ComponentFn::new(|input: TestInput| -> Result<TestMiddle, BoxedError> {
//             Ok(TestMiddle((input.0 * 2).to_string()))
//         });
//
//         // Second component: convert string to float and multiply by 3
//         let second = ComponentFn::new(|input: TestMiddle| -> Result<TestOutput, BoxedError> {
//             let val = input
//                 .0
//                 .parse::<f64>()
//                 .map_err(|e| Box::new(e) as BoxedError)?;
//             Ok(TestOutput(val * 3.0))
//         });
//
//         let chain = Chain::new(first, second);
//
//         // Input 5 -> (5*2) -> "10" -> (10*3) -> 30.0
//         let result = chain.process(TestInput(5));
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), TestOutput(30.0));
//     }
//
//     #[test]
//     fn test_chain_error_in_first() {
//         // First component: fail on input 5
//         let first = ComponentFn::new(|input: TestInput| -> Result<TestMiddle, BoxedError> {
//             if input.0 == 5 {
//                 Err("Failed in first component".into())
//             } else {
//                 Ok(TestMiddle(input.0.to_string()))
//             }
//         });
//
//         // Second component: just convert
//         let second = ComponentFn::new(|input: TestMiddle| -> Result<TestOutput, BoxedError> {
//             let val = input
//                 .0
//                 .parse::<f64>()
//                 .map_err(|e| Box::new(e) as BoxedError)?;
//             Ok(TestOutput(val))
//         });
//
//         let chain = Chain::new(first, second);
//
//         // Should succeed with input 3
//         let result = chain.process(TestInput(3));
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), TestOutput(3.0));
//
//         // Should fail with input 5
//         let result = chain.process(TestInput(5));
//         assert!(result.is_err());
//         assert_eq!(result.unwrap_err().to_string(), "Failed in first component");
//     }
//
//     #[test]
//     fn test_chain_error_in_second() {
//         // First component: just convert to string
//         let first = ComponentFn::new(|input: TestInput| -> Result<TestMiddle, BoxedError> {
//             Ok(TestMiddle(input.0.to_string()))
//         });
//
//         // Second component: fail on input "5"
//         let second = ComponentFn::new(|input: TestMiddle| -> Result<TestOutput, BoxedError> {
//             if input.0 == "5" {
//                 Err("Failed in second component".into())
//             } else {
//                 let val = input
//                     .0
//                     .parse::<f64>()
//                     .map_err(|e| Box::new(e) as BoxedError)?;
//                 Ok(TestOutput(val))
//             }
//         });
//
//         let chain = Chain::new(first, second);
//
//         // Should succeed with input 3
//         let result = chain.process(TestInput(3));
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), TestOutput(3.0));
//
//         // Should fail with input 5
//         let result = chain.process(TestInput(5));
//         assert!(result.is_err());
//         assert_eq!(
//             result.unwrap_err().to_string(),
//             "Failed in second component"
//         );
//     }
//
//     #[test]
//     fn test_chain_name() {
//         let first = TestComponent::new("FirstComponent", 2);
//         let second = TestComponent::new("SecondComponent", 3);
//
//         // Have to create a chain with compatible types
//         struct Adapter;
//
//         impl PipelineComponent<TestOutput, TestInput> for Adapter {
//             fn process(&self, input: TestOutput) -> Result<TestInput, BoxedError> {
//                 Ok(TestInput(input.0 as usize))
//             }
//
//             fn name(&self) -> String {
//                 "Adapter".to_string()
//             }
//         }
//
//         impl Debug for Adapter {
//             fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//                 f.debug_struct("Adapter").finish()
//             }
//         }
//
//         let chain = Chain::new(first, Adapter);
//
//         // Check that the name includes both component names
//         let name = chain.name();
//         assert!(name.contains("FirstComponent"));
//         assert!(name.contains("Adapter"));
//         assert!(name.contains("Chain"));
//     }
//
//     #[test]
//     fn test_chain_debug() {
//         let first = TestComponent::new("FirstComponent", 2);
//         let second = TestComponent::new("SecondComponent", 3);
//
//         // Have to create a chain with compatible types
//         struct Adapter;
//
//         impl PipelineComponent<TestOutput, TestInput> for Adapter {
//             fn process(&self, input: TestOutput) -> Result<TestInput, BoxedError> {
//                 Ok(TestInput(input.0 as usize))
//             }
//
//             fn name(&self) -> String {
//                 "Adapter".to_string()
//             }
//         }
//
//         impl Debug for Adapter {
//             fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//                 f.debug_struct("Adapter").finish()
//             }
//         }
//
//         let chain = Chain::new(first, Adapter);
//
//         // Should be able to debug-format the chain
//         let debug_str = format!("{:?}", chain);
//         assert!(debug_str.contains("Chain"));
//     }
//
//     // Test complex component composition
//     #[test]
//     fn test_complex_composition() {
//         // Component 1: multiply by 2
//         let multiply = ComponentFn::new(|input: TestInput| -> Result<TestMiddle, BoxedError> {
//             Ok(TestMiddle((input.0 * 2).to_string()))
//         });
//
//         // Component 2: add "_processed" suffix
//         let suffix = ComponentFn::new(|input: TestMiddle| -> Result<TestMiddle, BoxedError> {
//             Ok(TestMiddle(format!("{}_processed", input.0)))
//         });
//
//         // Component 3: convert to float and square
//         let square = ComponentFn::new(|input: TestMiddle| -> Result<TestOutput, BoxedError> {
//             // Extract the numeric part
//             let numeric_part = input.0.split('_').next().unwrap_or("0");
//             let val = numeric_part
//                 .parse::<f64>()
//                 .map_err(|e| Box::new(e) as BoxedError)?;
//             Ok(TestOutput(val * val))
//         });
//
//         // Chain: multiply -> suffix
//         let chain1 = Chain::new(multiply, suffix);
//
//         // Chain: (multiply -> suffix) -> square
//         let chain2 = Chain::new(chain1, square);
//
//         // Input 5 -> (5*2) -> "10" -> "10_processed" -> square(10) -> 100.0
//         let result = chain2.process(TestInput(5));
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), TestOutput(100.0));
//     }
//
//     // Test trait object usage
//     #[test]
//     fn test_trait_object() {
//         // Create components
//         let multiply = ComponentFn::new(|input: TestInput| -> Result<TestOutput, BoxedError> {
//             Ok(TestOutput(input.0 as f64 * 2.0))
//         });
//
//         let add = ComponentFn::new(|input: TestInput| -> Result<TestOutput, BoxedError> {
//             Ok(TestOutput(input.0 as f64 + 3.0))
//         });
//
//         // Store in a vector of trait objects
//         let mut components: Vec<Box<dyn PipelineComponent<TestInput, TestOutput> + Send + Sync>> =
//             Vec::new();
//         components.push(Box::new(multiply));
//         components.push(Box::new(add));
//
//         // Use the components
//         let result1 = components[0].process(TestInput(5));
//         assert_eq!(result1.unwrap(), TestOutput(10.0));
//
//         let result2 = components[1].process(TestInput(5));
//         assert_eq!(result2.unwrap(), TestOutput(8.0));
//     }
//
//     // Test Send + Sync requirements
//     #[test]
//     fn test_send_sync() {
//         fn assert_send_sync<T: Send + Sync>() {}
//
//         // Verify that our test types implement Send + Sync
//         assert_send_sync::<TestComponent>();
//         assert_send_sync::<
//             ComponentFn<TestInput, TestOutput, fn(TestInput) -> Result<TestOutput, BoxedError>>,
//         >();
//
//         // Verify that Chain implements Send + Sync
//         type TestChain = Chain<
//             TestComponent,
//             ComponentFn<TestOutput, TestInput, fn(TestOutput) -> Result<TestInput, BoxedError>>,
//             TestInput,
//             TestOutput,
//             TestInput,
//         >;
//         assert_send_sync::<TestChain>();
//     }
// }
