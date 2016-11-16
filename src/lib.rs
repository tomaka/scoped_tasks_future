extern crate crossbeam;
extern crate futures;
extern crate num_cpus;
#[macro_use]
extern crate lazy_static;

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;

use crossbeam::sync::MsQueue;

use futures::Future;
use futures::Oneshot;
use futures::Poll;

thread_local!(static STARTER: RefCell<bool> = RefCell::new(false));

pub struct Task<'a, R> {
    inner: TaskInner<'a, R>,
}

enum TaskInner<'a, R> {
    Invalid,
    NotStarted(Box<CallTrait<R> + 'a>),
    StartedOrFinished(Oneshot<R>),
}

trait CallTrait<R>: Send { fn call(self: Box<Self>) -> R; }
impl<T, R> CallTrait<R> for T where T: FnOnce() -> R + Send, R: Send {
    #[inline] fn call(self: Box<Self>) -> R { self() }
}

impl<'a, R> Task<'a, R> where R: Send {
    pub fn new<F>(f: F) -> Task<'a, R> where F: FnOnce() -> R + Send + 'a {
        Task {
            inner: TaskInner::NotStarted(Box::new(f))
        }
    }
}

impl<'a, R> Future for Task<'a, R> where R: Send + 'static {
    type Item = R;
    type Error = ();

    fn poll(&mut self) -> Poll<R, ()> {
        match mem::replace(&mut self.inner, TaskInner::Invalid) {
            TaskInner::Invalid => panic!(),

            TaskInner::NotStarted(task) => {
                STARTER.with(|s| { assert!(*s.borrow()); });

                let (tx, mut rx) = futures::oneshot();
                let task: Box<CallTrait<R> + 'static> = unsafe { mem::transmute(task) }; 
                thread::spawn(move || {
                    tx.complete(task.call());
                });

                let result = rx.poll().map_err(|_| ());
                self.inner = TaskInner::StartedOrFinished(rx);
                result
            },

            TaskInner::StartedOrFinished(mut oneshot) => {
                let result = oneshot.poll().map_err(|_| ());
                self.inner = TaskInner::StartedOrFinished(oneshot);
                result
            },
        }
    }
}

pub fn execute<F>(future: F) -> Result<F::Item, F::Error> where F: Future {
    STARTER.with(|s| { *s.borrow_mut() = true; });
    let ret = future.wait();
    STARTER.with(|s| { *s.borrow_mut() = false; });
    ret
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::thread;
    use futures::Future;
    use Task;
    use execute;

    #[test]
    fn basic() {
        let mut a = 5;
        execute(Task::new(|| a = 12)).unwrap();
        assert_eq!(a, 12);
    }

    #[test]
    fn then() {
        let a = AtomicUsize::new(5);
        let a = &a;

        let f1 = Task::new(move || {
            thread::sleep_ms(100);
            let v = a.swap(12, Ordering::SeqCst);
            assert!(v == 5 || v == 13);
        });

        let f2 = Task::new(move || {
            thread::sleep_ms(100);
            let v = a.swap(13, Ordering::SeqCst);
            assert!(v == 5 || v == 12);
        });

        let f3 = f1.join(f2).and_then(move |_| {
            Task::new(move || {
                thread::sleep_ms(100);
                let v = a.swap(14, Ordering::SeqCst);
                assert!(v == 12 || v == 13);
            })
        });

        execute(f3).unwrap();
        assert_eq!(a.load(Ordering::SeqCst), 14);
    }

    #[test]
    #[should_panic]
    fn forbidden_manual_task() {
        let f = Task::new(move || {});
        let _ = f.wait();
    }
}
