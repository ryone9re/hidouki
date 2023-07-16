use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::{
    sync::{Arc, Mutex},
    task::Context,
};

use futures::{
    future::BoxFuture,
    task::{waker_ref, ArcWake},
    Future, FutureExt,
};

struct Hello {
    state: StateHello,
}

enum StateHello {
    HELLO,
    WORLD,
    END,
}

impl Hello {
    fn new() -> Self {
        Hello {
            state: StateHello::HELLO,
        }
    }
}

impl Future for Hello {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.state {
            StateHello::HELLO => {
                print!("Hello, ");
                // WORLD状態に遷移
                self.state = StateHello::WORLD;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
            StateHello::WORLD => {
                println!("World!");
                // END状態に遷移
                self.state = StateHello::END;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
            StateHello::END => std::task::Poll::Ready(()), // 終了
        }
    }
}

struct Task {
    // 実行するコルーチン
    future: Mutex<BoxFuture<'static, ()>>, // 1
    // Executorへスケジューリングするためのチャネル
    sender: SyncSender<Arc<Task>>, // 2
}

impl ArcWake for Task {
    // 3
    fn wake_by_ref(arc_self: &Arc<Self>) {
        // 自身をスケジューリング
        let self0 = arc_self.clone();
        arc_self.sender.send(self0).unwrap();
    }
}

// 1
struct Executor {
    // 実行キュー
    sender: SyncSender<Arc<Task>>,
    receiver: Receiver<Arc<Task>>,
}

impl Executor {
    fn new() -> Self {
        // チャネルを生成｡キューのサイズは最大1024個
        let (sender, receiver) = sync_channel(1024);
        Executor { sender, receiver }
    }

    // 2
    fn get_spawner(&self) -> Spawner {
        Spawner {
            sender: self.sender.clone(),
        }
    }

    // 3
    fn run(&self) {
        while let Ok(task) = self.receiver.recv() {
            // コンテキストを生成
            let mut future = task.future.lock().unwrap();
            let waker = waker_ref(&task);
            let mut ctx = Context::from_waker(&waker);
            // pollを呼び出し実行
            let _ = future.as_mut().poll(&mut ctx);
        }
    }
}

struct Spawner {
    sender: SyncSender<Arc<Task>>,
}

impl Spawner {
    // 2
    fn spawn(&self, future: impl Future<Output = ()> + 'static + Send) {
        let future = future.boxed(); // FutureをBox化
        let task = Arc::new(Task {
            future: Mutex::new(future),
            sender: self.sender.clone(),
        });

        // 実行キューにエンキュー
        let _ = self.sender.send(task);
    }
}

fn main() {
    // 初期化
    let executor = Executor::new();
    executor.get_spawner().spawn(Hello::new());
    executor.run();
}
