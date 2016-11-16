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

lazy_static! {
    static ref POOL: Arc<MsQueue<Box<CallTrait>>> = {
        let queue = Arc::new(MsQueue::<Box<CallTrait>>::new());

        for _ in 0 .. num_cpus::get() {
            let queue = queue.clone();
            thread::spawn(move || {
                loop {
                    let f = queue.pop();
                    f.call();
                }
            });
        }

        queue
    };
}

pub struct Task<'a, R> {
    inner: TaskInner<R>, 
    marker: PhantomData<&'a ()>
}

enum TaskInner<R> {
    Invalid,
    NotStarted(Box<CallTrait>, Oneshot<R>),
    StartedOrFinished(Oneshot<R>),
}

trait CallTrait: Send { fn call(self: Box<Self>); }
impl<T> CallTrait for T where T: FnOnce() + Send {
    #[inline] fn call(self: Box<Self>) { self() }
}

impl<'a, R> Task<'a, R> where R: Send + 'a {
    pub fn new<F>(f: F) -> Task<'a, R> where F: FnOnce() -> R + Send + 'a {
        let (tx, rx) = futures::oneshot();

        let f = Box::new(move || tx.complete(f())) as Box<CallTrait + 'a>;
        let f: Box<CallTrait + 'static> = unsafe { mem::transmute(f) };
    
        Task {
            inner: TaskInner::NotStarted(f, rx),
            marker: PhantomData,
        }
    }
}

impl<'a, R> Future for Task<'a, R> where R: Send + 'a {
    type Item = R;
    type Error = ();

    fn poll(&mut self) -> Poll<R, ()> {
        match mem::replace(&mut self.inner, TaskInner::Invalid) {
            TaskInner::Invalid => panic!(),

            TaskInner::NotStarted(task, mut rx) => {
                STARTER.with(|s| { assert!(*s.borrow()); });

                POOL.push(task);

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
