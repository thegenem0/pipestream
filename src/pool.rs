use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicUsize};

use crossbeam::queue::ArrayQueue;

/// A generic object pool for reusing allocated objects
pub struct ObjectPool<T> {
    /// Storage for pooled objects
    objects: Arc<ArrayQueue<T>>,
    /// Function to create new objects when the pool is empty
    create_fn: Box<dyn Fn() -> T + Send + Sync>,
    /// Maximum number of objects the pool will store
    max_size: usize,
    /// Current number of objects in the pool
    size: Arc<AtomicUsize>,
}

impl<T> ObjectPool<T> {
    /// Create a new object pool with the given initial capacity and creation function
    ///
    /// # Parameters
    ///
    /// * `initial_capacity` - The initial number of objects to create and store in the pool
    /// * `max_size` - The maximum number of objects the pool will store
    /// * `create_fn` - A function that creates new objects of type `T` when needed
    pub fn new<F>(initial_capacity: usize, max_size: usize, create_fn: F) -> Self
    where
        F: Fn() -> T + Send + Sync + 'static,
    {
        assert!(
            initial_capacity <= max_size,
            "initial_capacity must be <= max_size"
        );

        let queue = ArrayQueue::new(max_size);

        for _ in 0..initial_capacity {
            let _ = queue.push(create_fn());
        }

        Self {
            objects: Arc::new(queue),
            create_fn: Box::new(create_fn),
            max_size,
            size: Arc::new(AtomicUsize::new(initial_capacity)),
        }
    }

    /// Get an object from the pool, or create a new one if the pool is empty
    ///
    /// This method returns a `PooledObject<T>` which acts as a smart pointer to the
    /// underlying object. When the `PooledObject` is dropped, the underlying object
    /// is automatically returned to the pool (unless the pool is already at max_size).
    ///
    /// # Returns
    ///
    /// A `PooledObject<T>` wrapping an object from the pool, or a newly created object
    /// if the pool was empty.
    pub fn get(&self) -> PooledObject<T> {
        let backoff = crossbeam::utils::Backoff::new();

        let object = loop {
            match self.objects.pop() {
                Some(obj) => {
                    self.size.fetch_sub(1, Ordering::Relaxed);
                    break obj;
                }
                None => {
                    if backoff.is_completed() {
                        break (self.create_fn)();
                    }
                    backoff.snooze();
                }
            }
        };

        PooledObject {
            object: Some(object),
            pool: Arc::clone(&self.objects),
            max_size: self.max_size,
            size: Arc::clone(&self.size),
        }
    }

    /// Try to get an object from the pool without creating a new one if empty
    ///
    /// # Returns
    ///
    /// Some(PooledObject<T>) if an object was available in the pool, None otherwise
    pub fn try_get(&self) -> Option<PooledObject<T>> {
        self.objects.pop().map(|object| {
            self.size.fetch_sub(1, Ordering::Relaxed);

            PooledObject {
                object: Some(object),
                pool: Arc::clone(&self.objects),
                max_size: self.max_size,
                size: Arc::clone(&self.size),
            }
        })
    }

    /// Get the current number of available objects in the pool
    ///
    /// # Returns
    ///
    /// The number of objects currently available in the pool
    pub fn available(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Get the maximum size of the pool
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Clear all objects from the pool
    pub fn clear(&self) {
        while self.objects.pop().is_some() {
            self.size.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Return an approximate count of how many objects have been created by this pool
    ///
    /// This is calculated as: current_pool_size + (max_size - available)
    /// which gives a lower bound on the number of live objects created by this pool
    pub fn live_objects_count(&self) -> usize {
        let available = self.available();
        self.max_size - available
    }

    /// Pre-populate the pool with objects up to the specified target_size
    ///
    /// This is useful when you want to ensure a minimum number of objects are ready in the pool
    /// Returns the number of new objects created
    pub fn ensure_min_capacity(&self, target_size: usize) -> usize {
        let target_size = target_size.min(self.max_size);
        let mut added = 0;

        while self.available() < target_size {
            let obj = (self.create_fn)();
            match self.objects.push(obj) {
                Ok(_) => {
                    self.size.fetch_add(1, Ordering::Relaxed);
                    added += 1;
                }
                Err(_) => {
                    // Pool is full (race condition with other threads)
                    break;
                }
            }
        }

        added
    }
}

/// A smart pointer to an object from the pool that returns the object to the pool when dropped
pub struct PooledObject<T> {
    object: Option<T>,
    pool: Arc<ArrayQueue<T>>,
    max_size: usize,
    size: Arc<AtomicUsize>,
}

impl<T> Drop for PooledObject<T> {
    fn drop(&mut self) {
        if let Some(object) = self.object.take() {
            let current_size = self.size.load(Ordering::Relaxed);

            if current_size < self.max_size && self.pool.push(object).is_ok() {
                self.size.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl<T> Deref for PooledObject<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.object.as_ref().expect("Object has been taken")
    }
}

impl<T> DerefMut for PooledObject<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.object.as_mut().expect("Object has been taken")
    }
}

/// Trait for objects that need to be reset before returning to the pool
pub trait Resetable {
    /// Reset the object to its initial state before returning to the pool
    fn reset(&mut self);
}

/// A smart pointer that resets the object before returning it to the pool
pub struct ResetablePooledObject<T: Resetable> {
    inner: PooledObject<T>,
}

impl<T: Resetable> Drop for ResetablePooledObject<T> {
    fn drop(&mut self) {
        if let Some(mut obj) = self.inner.object.take() {
            obj.reset();

            let current_size = self.inner.size.load(Ordering::Relaxed);
            if current_size < self.inner.max_size && self.inner.pool.push(obj).is_ok() {
                self.inner.size.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl<T: Resetable> Deref for ResetablePooledObject<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T: Resetable> DerefMut for ResetablePooledObject<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// Extension trait for ObjectPool to support resetable objects
pub trait ObjectPoolExt<T: Resetable> {
    /// Get a resetable object from the pool
    fn get_resetable(&self) -> ResetablePooledObject<T>;
}

impl<T: Resetable> ObjectPoolExt<T> for ObjectPool<T> {
    fn get_resetable(&self) -> ResetablePooledObject<T> {
        ResetablePooledObject { inner: self.get() }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for ObjectPool<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectPool")
            .field("available", &self.available())
            .field("max_size", &self.max_size)
            .finish()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for PooledObject<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledObject")
            .field("object", &self.object)
            .finish()
    }
}

impl<T: Resetable + std::fmt::Debug> std::fmt::Debug for ResetablePooledObject<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResetablePooledObject")
            .field("inner", &self.inner)
            .finish()
    }
}

impl Resetable for Vec<u8> {
    fn reset(&mut self) {
        self.clear();
    }
}

/// A memory pool specifically for reusing buffers
#[derive(Debug)]
pub struct BufferPool {
    pool: ObjectPool<Vec<u8>>,
}

impl BufferPool {
    /// Create a new buffer pool with the given initial capacity, buffer size, and maximum pool size
    ///
    /// # Parameters
    ///
    /// * `initial_capacity` - The initial number of buffers to create and store in the pool
    /// * `buffer_size` - The capacity in bytes of each buffer
    /// * `max_size` - The maximum number of buffers the pool will store
    pub fn new(initial_capacity: usize, buffer_size: usize, max_size: usize) -> Self {
        let pool = ObjectPool::new(initial_capacity, max_size, move || {
            Vec::with_capacity(buffer_size)
        });

        Self { pool }
    }

    /// Get a buffer from the pool, will be automatically cleared before return
    ///
    /// # Returns
    ///
    /// A buffer from the pool with its contents cleared
    pub fn get(&self) -> ResetablePooledObject<Vec<u8>> {
        self.pool.get_resetable()
    }

    /// Try to get a buffer from the pool without creating a new one if empty
    ///
    /// # Returns
    ///
    /// Some(buffer) if a buffer was available, None otherwise
    pub fn try_get(&self) -> Option<ResetablePooledObject<Vec<u8>>> {
        self.pool
            .try_get()
            .map(|inner| ResetablePooledObject { inner })
    }

    /// Get the current number of available buffers in the pool
    pub fn available(&self) -> usize {
        self.pool.available()
    }

    /// Get the maximum size of the buffer pool
    pub fn max_size(&self) -> usize {
        self.pool.max_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_object_pool_initial_capacity() {
        let pool = ObjectPool::new(5, 10, || String::from("test"));
        assert_eq!(pool.available(), 5);
    }

    #[test]
    fn test_object_pool_get_and_return() {
        let pool = ObjectPool::new(2, 5, || String::from("test"));
        assert_eq!(pool.available(), 2);

        // Get an object, reducing available count
        let obj = pool.get();
        assert_eq!(pool.available(), 1);
        assert_eq!(*obj, "test");

        // After drop, object should return to pool
        drop(obj);
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_object_pool_create_when_empty() {
        let created = Arc::new(AtomicUsize::new(0));
        let created_clone = created.clone();

        let pool = ObjectPool::new(0, 5, move || {
            created_clone.fetch_add(1, Ordering::SeqCst);
            String::from("test")
        });

        assert_eq!(pool.available(), 0);

        // Should create a new object
        let obj = pool.get();
        assert_eq!(*obj, "test");
        assert_eq!(created.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_object_pool_max_size() {
        let pool = ObjectPool::new(2, 3, || String::from("test"));

        // Get all objects and one more
        let obj1 = pool.get();
        let obj2 = pool.get();
        let obj3 = pool.get();

        assert_eq!(pool.available(), 0);

        // Return all objects
        drop(obj1);
        drop(obj2);
        drop(obj3);

        // Pool should not exceed max_size
        assert_eq!(pool.available(), 3);

        // Get and return one more
        let obj4 = pool.get();
        drop(obj4);

        // Should still be at max_size
        assert_eq!(pool.available(), 3);
    }

    #[test]
    fn test_buffer_pool() {
        let pool = BufferPool::new(3, 1024, 5);
        assert_eq!(pool.available(), 3);

        // Get a buffer and use it
        let mut buffer = pool.get();
        buffer.extend_from_slice(&[1, 2, 3, 4]);
        assert_eq!(&*buffer, &[1, 2, 3, 4]);

        // Check capacity is preserved
        assert!(buffer.capacity() >= 1024);

        // Return buffer to pool
        drop(buffer);
        assert_eq!(pool.available(), 3);

        // Get the same buffer again
        let buffer2 = pool.get();
        assert_eq!(buffer2.len(), 0); // Should be empty due to reset
        assert!(buffer2.capacity() >= 1024); // But capacity preserved
    }

    #[test]
    fn test_high_concurrency() {
        let pool = Arc::new(ObjectPool::new(10, 50, || Vec::<u8>::with_capacity(1024)));
        let mut handles = vec![];

        for _ in 0..20 {
            let pool_clone = pool.clone();
            let handle = thread::spawn(move || {
                for _ in 0..1000 {
                    let mut buf = pool_clone.get();
                    buf.extend_from_slice(&[1, 2, 3, 4]);
                    // Drop buf and return to pool
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All objects should be back in the pool up to max_size
        assert!(pool.available() <= 50);
    }

    #[test]
    fn test_clear() {
        let pool = ObjectPool::new(5, 10, || String::from("test"));
        assert_eq!(pool.available(), 5);

        pool.clear();
        assert_eq!(pool.available(), 0);

        // Getting after clear should create new objects
        let _obj = pool.get();
        drop(_obj);
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn test_ensure_min_capacity() {
        let pool = ObjectPool::new(2, 10, || String::from("test"));
        assert_eq!(pool.available(), 2);

        // Ensure we have at least 5 objects
        let added = pool.ensure_min_capacity(5);
        assert_eq!(added, 3);
        assert_eq!(pool.available(), 5);

        // Trying to ensure beyond max_size should cap at max_size
        let added = pool.ensure_min_capacity(15);
        assert_eq!(added, 5); // Only 5 more to reach max_size of 10
        assert_eq!(pool.available(), 10);
    }

    #[test]
    fn test_backoff_behavior() {
        // This test is more to verify the code compiles and runs
        // than to test actual backoff behavior which is hard to test deterministically
        let pool = ObjectPool::new(1, 5, || String::from("test"));

        // Get all available objects
        let obj1 = pool.get();
        assert_eq!(pool.available(), 0);

        // This should use backoff but eventually create a new object
        let obj2 = pool.get();
        assert_eq!(*obj2, "test");

        // Return objects
        drop(obj1);
        drop(obj2);
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_try_get() {
        let pool = ObjectPool::new(1, 5, || String::from("test"));

        // Get the only object
        let obj1 = pool.get();
        assert_eq!(pool.available(), 0);

        // try_get should return None since pool is empty
        let obj2 = pool.try_get();
        assert!(obj2.is_none());

        // Return obj1 to pool
        drop(obj1);

        // Now try_get should succeed
        let obj3 = pool.try_get();
        assert!(obj3.is_some());
        assert_eq!(*obj3.unwrap(), "test");
    }

    #[test]
    fn test_resetable_objects() {
        struct TestObj {
            value: i32,
            was_reset: bool,
        }

        impl Resetable for TestObj {
            fn reset(&mut self) {
                self.value = 0;
                self.was_reset = true;
            }
        }

        impl TestObj {
            fn new() -> Self {
                Self {
                    value: 42,
                    was_reset: false,
                }
            }
        }

        let pool = ObjectPool::new(1, 5, TestObj::new);

        // Get object and modify it
        {
            let mut obj = pool.get_resetable();
            obj.value = 100;
            assert_eq!(obj.value, 100);
            assert!(!obj.was_reset);
            // obj will be dropped here, which triggers reset
        }

        // Get object again from the pool
        let obj2 = pool.get_resetable();

        // Should be in a reset state
        assert_eq!(obj2.value, 0);
        assert!(obj2.was_reset);
    }
}
