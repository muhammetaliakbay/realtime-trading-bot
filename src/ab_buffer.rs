use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex, MutexGuard,
};

pub struct ABBuffer<T> {
    a: Mutex<Vec<T>>,
    b: Mutex<Vec<T>>,
    latch: AtomicBool,
}

impl<T> ABBuffer<T> {
    pub fn new() -> Self {
        ABBuffer {
            a: Mutex::new(Vec::new()),
            b: Mutex::new(Vec::new()),
            latch: AtomicBool::new(false),
        }
    }

    pub fn mutate(&self) -> MutexGuard<Vec<T>> {
        if self.latch.load(Ordering::Relaxed) {
            self.a.lock().unwrap()
        } else {
            self.b.lock().unwrap()
        }
    }

    pub fn swap(&self) -> MutexGuard<Vec<T>> {
        if self.latch.fetch_xor(true, Ordering::Relaxed) {
            self.a.lock().unwrap()
        } else {
            self.b.lock().unwrap()
        }
    }
}
