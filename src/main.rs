extern crate threadpool;
extern crate threadpool_crossbeam_channel;
extern crate rayon;
extern crate cpu_time;
extern crate num_cpus;

use std::thread::{self, Thread};
use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicUsize};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::time::{Duration, Instant};

use threadpool::ThreadPool as PlainThreadPool;
use threadpool_crossbeam_channel::ThreadPool as PlainThreadPoolWithCrossbeam;
use rayon::{ThreadPool as RayonThreadPool};

trait ThreadPool: Send {
    fn start() -> Self;
    fn run<F>(&self, cb: F) where F: FnOnce(&Self) + Send + 'static;
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
        let mut pool = Arc::new(rayon::ThreadPoolBuilder::new().build().unwrap());
        pool
    }
    fn run<F>(&self, cb: F) where F: FnOnce(&Self) + Send + 'static {
        let clone = self.clone();
        self.spawn(move || {
            cb(&clone);
        });
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
            channel.send(());
        }
    }

    timeit(|| {
        child_run(&pool, atomic, send);
        recv.recv().unwrap();
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
    chain::<PlainThreadPoolWithCrossbeam>().print("thread pool w/ crossbeam");
    chain::<Arc<RayonThreadPool>>().print("rayon threadpool");

    println!("fanout:");
    fanout::<PlainOldThread>().print("threads");
    fanout::<PlainThreadPool>().print("thread pool");
    fanout::<PlainThreadPoolWithCrossbeam>().print("thread pool w/ crossbeam");
    fanout::<Arc<RayonThreadPool>>().print("rayon threadpool");

    println!("fibb:");
    fibb::<PlainThreadPool>().print("thread pool");
    fibb::<PlainThreadPoolWithCrossbeam>().print("thread pool w/ crossbeam");
    fibb::<Arc<RayonThreadPool>>().print("rayon threadpool");

    println!("burst:");
    burst::<PlainThreadPool>().print("thread pool");
    burst::<PlainThreadPoolWithCrossbeam>().print("thread pool w/ crossbeam");
    burst::<Arc<RayonThreadPool>>().print("rayon threadpool");
}
