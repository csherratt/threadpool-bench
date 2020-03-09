extern crate threadpool;
extern crate threadpool_crossbeam_channel;
extern crate rayon;
extern crate cpu_time;
extern crate num_cpus;
extern crate tokio_threadpool;
extern crate executors;

#[cfg(target_os = "macos")]
extern crate dispatch;

use std::thread;
use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicUsize};
use std::sync::mpsc::{channel, Sender};
use std::time::{Duration, Instant};
use futures::future::lazy;

use threadpool::ThreadPool as PlainThreadPool;
use threadpool_crossbeam_channel::ThreadPool as PlainThreadPoolWithCrossbeam;
use rayon::{ThreadPool as RayonThreadPool};
use executors::Executor;

trait ThreadPool: Send + Sized {
    fn start() -> Self;
    fn run<F>(&self, cb: F) where F: FnOnce(&Self) + Send + 'static;
    fn shutdown(self) {

    }
}

struct PlainOldThread;

impl ThreadPool for PlainOldThread {
    fn start() -> PlainOldThread { PlainOldThread }
    fn run<F>(&self, cb: F) where F: FnOnce(&Self) + Send + 'static {
         thread::spawn(move || {
             cb(&PlainOldThread);
         });
    }
}

#[derive(Clone)]
struct PlainThreadPoolSingle(PlainThreadPool);

impl ThreadPool for PlainThreadPoolSingle {
    fn start() -> PlainThreadPoolSingle {
        PlainThreadPoolSingle(PlainThreadPool::new(1))
    }
    fn run<F>(&self, cb: F) where F: FnOnce(&Self) + Send + 'static {
        let clone = self.clone();
        self.0.execute(move || {
            cb(&clone);
        })
    }
}


impl ThreadPool for PlainThreadPool {
    fn start() -> PlainThreadPool {
        PlainThreadPool::new(num_cpus::get())
    }
    fn run<F>(&self, cb: F) where F: FnOnce(&Self) + Send + 'static {
        let clone = self.clone();
        self.execute(move || {
            cb(&clone);
        })
    }
}

impl ThreadPool for PlainThreadPoolWithCrossbeam {
    fn start() -> PlainThreadPoolWithCrossbeam {
        PlainThreadPoolWithCrossbeam::new(num_cpus::get())
    }
    fn run<F>(&self, cb: F) where F: FnOnce(&Self) + Send + 'static {
        let clone = self.clone();
        self.execute(move || {
            cb(&clone);
        })
    }
}

impl ThreadPool for Arc<RayonThreadPool> {
    fn start() -> Arc<RayonThreadPool> {
        let pool = Arc::new(rayon::ThreadPoolBuilder::new().build().unwrap());
        pool
    }
    fn run<F>(&self, cb: F) where F: FnOnce(&Self) + Send + 'static {
        let clone = self.clone();
        self.spawn(move || {
            cb(&clone);
        });
    }
}

#[cfg(target_os = "macos")]
struct DispatchSerial(dispatch::Queue);

#[cfg(target_os = "macos")]
impl ThreadPool for DispatchSerial {
    fn start() -> DispatchSerial {
        DispatchSerial(dispatch::Queue::create("com.example.rust", dispatch::QueueAttribute::Serial))
    }

    fn run<F>(&self, cb: F) where F: FnOnce(&Self) + Send + 'static {
        let clone = DispatchSerial(self.0.clone());
        self.0.exec_async(move || {
            cb(&clone);
        });
    }
}

#[cfg(target_os = "macos")]
struct DispatchConcurrent(dispatch::Queue);

#[cfg(target_os = "macos")]
impl ThreadPool for DispatchConcurrent {
    fn start() -> DispatchConcurrent {
        DispatchConcurrent(dispatch::Queue::create("com.example.rust", dispatch::QueueAttribute::Concurrent))
    }

    fn run<F>(&self, cb: F) where F: FnOnce(&Self) + Send + 'static {
        let clone = DispatchConcurrent(self.0.clone());
        self.0.exec_async(move || {
            cb(&clone);
        });
    }
}

impl ThreadPool for Arc<tokio_threadpool::ThreadPool> {
    fn start() -> Arc<tokio_threadpool::ThreadPool> {
        Arc::new(tokio_threadpool::ThreadPool::new())
    }

    fn run<F>(&self, cb: F) where F: FnOnce(&Self) + Send + 'static {
        let clone = self.clone();
        self.spawn(lazy(move || {
            cb(&clone);
            Ok(())
        }));
    }
}

#[derive(Clone)]
struct ExecutorsWorkStealingSmallPool(executors::crossbeam_workstealing_pool::ThreadPool<executors::parker::StaticParker<executors::parker::SmallThreadData>>);

impl ThreadPool for ExecutorsWorkStealingSmallPool {
    fn start() -> ExecutorsWorkStealingSmallPool {
        ExecutorsWorkStealingSmallPool(
            executors::crossbeam_workstealing_pool::small_pool(std::cmp::min(num_cpus::get(), 32))
        )
    }

    fn run<F>(&self, cb: F) where F: FnOnce(&Self) + Send + 'static {
        let clone = self.clone();
        self.0.execute(move || {
            cb(&clone);
        });
    }

    fn shutdown(self) {
        self.0.shutdown().unwrap()
    }
}

#[derive(Clone)]
struct ExecutorsWorkStealingLargePool(executors::crossbeam_workstealing_pool::ThreadPool<executors::parker::StaticParker<executors::parker::LargeThreadData>>);

impl ThreadPool for ExecutorsWorkStealingLargePool {
    fn start() -> ExecutorsWorkStealingLargePool {
        ExecutorsWorkStealingLargePool(
            executors::crossbeam_workstealing_pool::large_pool(std::cmp::min(num_cpus::get(), 64))
        )
    }

    fn run<F>(&self, cb: F) where F: FnOnce(&Self) + Send + 'static {
        let clone = self.clone();
        self.0.execute(move || {
            cb(&clone);
        });
    }

    fn shutdown(self) {
        self.0.shutdown().unwrap()
    }
}

fn chain<P>() -> Timing where P: ThreadPool {
    let atomic = Arc::new(AtomicUsize::new(100_000));
    let pool = P::start();
    let (send, recv) = channel();
    fn child_run<P>(pool: &P, atomic: Arc<AtomicUsize>, channel: Sender<()>) where P: ThreadPool {
        let last = atomic.fetch_sub(1, Ordering::Relaxed);
        if last != 0 {
            pool.run(move |pool| {
               child_run(pool, atomic, channel);
            });
        } else {
            channel.send(()).unwrap();
        }
    }

    timeit(|| {
        child_run(&pool, atomic, send);
        recv.recv().unwrap();
        pool.shutdown();
    })
}

fn fanout<P>() -> Timing where P: ThreadPool {
    let pool = P::start();
    let (send, recv) = channel::<()>();

    timeit(|| {
        for _ in 0..100_000 {
            let send = send.clone();
            pool.run(|_| { drop(send) });
        }
        drop(send);
        recv.recv().err().unwrap();
        pool.shutdown();
    })
}

fn fibb<P>() -> Timing where P: ThreadPool {
    let pool = P::start();
    let (send, recv) = channel::<()>();

    fn child_run<P>(pool: &P, count: usize, channel: Sender<()>) where P: ThreadPool {
        if count == 0 {
            return;
        }

        let extra = channel.clone();
        pool.run(move |pool| child_run(pool, count - 1, extra));
        pool.run(move |pool| child_run(pool, count - 1, channel));
    }

    timeit(|| {
        child_run(&pool, 22, send);
        recv.recv().err().unwrap();
        pool.shutdown();
    })
}

fn burst<P>() -> Timing where P: ThreadPool {
    let pool = P::start();


    fn child_run<P>(pool: &P, count: usize, channel: Sender<()>) where P: ThreadPool {
        if count == 0 {
            return;
        }

        let extra = channel.clone();
        pool.run(move |pool| child_run(pool, count - 1, extra));
        pool.run(move |pool| child_run(pool, count - 1, channel));
    }

    timeit(|| {
        for _ in 0..10 {
            let (send, recv) = channel::<()>();
            child_run(&pool, 16, send);
            recv.recv().err().unwrap();
        }
        pool.shutdown();
    })
}

struct Timing {
    wall_clock : Duration,
    total_time: Duration
}

impl Timing {
    fn print(&self, name: &str) {
        println!("\t{}: {:?} {:?}", name, self.wall_clock, self.total_time);
    }
}

fn timeit<F>(cb: F) -> Timing where F: FnOnce() {
    let start_cpu = cpu_time::ProcessTime::now();
    let start = Instant::now();
    cb();
    let end = Instant::now();
    let end_cpu = cpu_time::ProcessTime::now();

    Timing {
        wall_clock: end.duration_since(start),
        total_time: end_cpu.duration_since(start_cpu)
    }
}

fn main() {
    println!("chain:");
    chain::<PlainOldThread>().print("threads");
    chain::<PlainThreadPool>().print("thread pool");
    chain::<PlainThreadPoolSingle>().print("thread pool single");
    chain::<PlainThreadPoolWithCrossbeam>().print("thread pool w/ crossbeam");
    chain::<Arc<RayonThreadPool>>().print("rayon threadpool");
    chain::<Arc<tokio_threadpool::ThreadPool>>().print("tokio threadpool");
    chain::<ExecutorsWorkStealingSmallPool>().print("Executors WorkStealing SmallPool");
    chain::<ExecutorsWorkStealingLargePool>().print("Executors WorkStealing LargePool");
    #[cfg(target_os = "macos")] chain::<DispatchSerial>().print("dispatch serial");
    #[cfg(target_os = "macos")] chain::<DispatchConcurrent>().print("dispatch concurrent");

    println!("fanout:");
    fanout::<PlainOldThread>().print("threads");
    fanout::<PlainThreadPool>().print("thread pool");
    fanout::<PlainThreadPoolSingle>().print("thread pool single");
    fanout::<PlainThreadPoolWithCrossbeam>().print("thread pool w/ crossbeam");
    fanout::<Arc<RayonThreadPool>>().print("rayon threadpool");
    fanout::<Arc<tokio_threadpool::ThreadPool>>().print("tokio threadpool");
    fanout::<ExecutorsWorkStealingSmallPool>().print("Executors WorkStealing SmallPool");
    fanout::<ExecutorsWorkStealingLargePool>().print("Executors WorkStealing LargePool");
    #[cfg(target_os = "macos")] fanout::<DispatchSerial>().print("dispatch serial");
    #[cfg(target_os = "macos")] fanout::<DispatchConcurrent>().print("dispatch concurrent");

    println!("fibb:");
    fibb::<PlainThreadPool>().print("thread pool");
    fibb::<PlainThreadPoolSingle>().print("thread pool single");
    fibb::<PlainThreadPoolWithCrossbeam>().print("thread pool w/ crossbeam");
    fibb::<Arc<RayonThreadPool>>().print("rayon threadpool");
    fibb::<Arc<tokio_threadpool::ThreadPool>>().print("tokio threadpool");
    fibb::<ExecutorsWorkStealingSmallPool>().print("Executors WorkStealing SmallPool");
    fibb::<ExecutorsWorkStealingLargePool>().print("Executors WorkStealing LargePool");
    #[cfg(target_os = "macos")] fibb::<DispatchSerial>().print("dispatch serial");
    #[cfg(target_os = "macos")] fibb::<DispatchConcurrent>().print("dispatch concurrent");

    println!("burst:");
    burst::<PlainThreadPool>().print("thread pool");
    burst::<PlainThreadPoolSingle>().print("thread pool single");
    burst::<PlainThreadPoolWithCrossbeam>().print("thread pool w/ crossbeam");
    burst::<Arc<RayonThreadPool>>().print("rayon threadpool");
    burst::<Arc<tokio_threadpool::ThreadPool>>().print("tokio threadpool");
    burst::<ExecutorsWorkStealingSmallPool>().print("Executors WorkStealing SmallPool");
    burst::<ExecutorsWorkStealingLargePool>().print("Executors WorkStealing LargePool");
    #[cfg(target_os = "macos")] burst::<DispatchSerial>().print("dispatch serial");
    #[cfg(target_os = "macos")] burst::<DispatchConcurrent>().print("dispatch concurrent");
}
