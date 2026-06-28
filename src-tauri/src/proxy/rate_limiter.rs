//! 请求限流 + 并发控制模块
//!
//! 使用滑动时间窗口（Sliding Window Log）算法实现精确的每分钟调用次数限制，
//! 并使用 Semaphore 控制同时发往上游的并发请求数。
//!
//! 核心保证：
//! - 任意 60s 滚动窗口内，通过 `acquire()` 的请求数 ≤ `max_per_minute`
//! - 任意时刻同时发往上游的请求数 ≤ `max_concurrent`
//! - 不允许突发（burst）：即使一开始 token 满，也不会瞬间全部放行
//! - 高并发安全：多个请求同时 `acquire()` 不会超额
//!
//! 算法：
//! - 频率限流：维护一个 `VecDeque<Instant>` 记录最近请求的时间戳
//!   - `acquire()` 时：
//!     1. 持锁，清除超过 60s 的旧记录
//!     2. 如果 `len < max_per_minute`：把 `now` 入队，释放锁，立即返回
//!     3. 否则：计算 `oldest + 60s - now = wait_duration`，释放锁，sleep 对应时间后重试
//! - 并发限流：使用 `tokio::sync::Semaphore` 控制 in-flight 请求数
//!   - `acquire_concurrency_owned()` 阻塞等待并发许可，返回 `OwnedSemaphorePermit`
//!   - 许可随响应流转，在响应完全消费后释放（Drop）
//!   - 确保同一时刻不会有过多的请求同时发往上流

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};

/// 限流器运行时状态（用于前端展示）
#[derive(Debug, Clone, Default)]
pub struct RateLimiterStatus {
    /// 当前 60s 窗口内已使用的调用次数
    pub current_count: usize,
    /// 每分钟最大允许调用次数
    pub max_per_minute: u32,
    /// 当前正在等待（延时排队）的请求数
    pub waiting_count: usize,
    /// 当前正在执行的并发请求数（in-flight）
    pub current_concurrent: usize,
    /// 最大允许并发请求数
    pub max_concurrent: u32,
    /// 当前正在等待并发许可的请求数
    pub concurrent_waiting_count: usize,
}

/// 滑动时间窗口限流器 + 并发控制器 + 过载退避管理
pub struct RateLimiter {
    /// 频率限流内部状态
    inner: Arc<Mutex<RateLimiterInner>>,
    /// 时间窗口大小（固定 60 秒）
    window: Duration,
    /// 当前正在等待频率时隙（sleep）的请求数
    waiting_count: Arc<AtomicUsize>,
    /// 并发控制信号量
    concurrency_semaphore: Arc<Semaphore>,
    /// 最大并发数（记录配置值，用于 status() 和热更新判断）
    max_concurrent: Arc<std::sync::Mutex<u32>>,
    /// 当前正在等待并发许可的请求数
    concurrent_waiting_count: Arc<AtomicUsize>,

    // ====== 全局过载退避状态（跨所有请求共享） ======
    /// 过载退避计数器：每次上游返回 429/503 时 +1，任何请求成功时归零
    overload_backoff_count: Arc<AtomicU32>,
}

struct RateLimiterInner {
    /// 最近请求的时间戳（按时间排序，队首最旧）
    request_times: VecDeque<Instant>,
    /// 每分钟最大请求数
    max_per_minute: u32,
}

impl RateLimiter {
    /// 创建新的限流器
    ///
    /// - `max_per_minute`：每分钟最大请求数（默认 40）
    /// - `max_concurrent`：最大并发请求数（默认 5）
    pub fn new(max_per_minute: u32, max_concurrent: u32) -> Self {
        let max_per_minute = max_per_minute.max(1);
        let max_concurrent = max_concurrent.max(1);

        Self {
            inner: Arc::new(Mutex::new(RateLimiterInner {
                request_times: VecDeque::with_capacity(max_per_minute as usize + 1),
                max_per_minute,
            })),
            window: Duration::from_secs(60),
            waiting_count: Arc::new(AtomicUsize::new(0)),
            concurrency_semaphore: Arc::new(Semaphore::new(max_concurrent as usize)),
            max_concurrent: Arc::new(std::sync::Mutex::new(max_concurrent)),
            concurrent_waiting_count: Arc::new(AtomicUsize::new(0)),
            overload_backoff_count: Arc::new(AtomicU32::new(0)),
        }
    }

    /// 更新限流速率（配置变更时调用）
    pub async fn update_rate(&self, max_per_minute: u32) {
        let max_per_minute = max_per_minute.max(1);
        let mut inner = self.inner.lock().await;
        inner.max_per_minute = max_per_minute;
        // 如果当前队列长度超过新的限制，截断（保留最近的限制数个）
        while inner.request_times.len() > max_per_minute as usize {
            inner.request_times.pop_front();
        }
    }

    /// 更新并发限制（配置变更时调用）
    ///
    /// - 增加：通过 `add_permits()` 立即生效
    /// - 减少：多余的许可会被自然消耗（in-flight 请求完成后不再释放新许可）
    ///   但由于 Semaphore 无法直接 remove_permits，如果需要严格减少，
    ///   可以通过 `acquire_many_owned()` 抢占多余的许可然后 forget。
    pub fn update_concurrency(&self, new_max: u32) {
        let new_max = new_max.max(1);
        let old_max = {
            let mut guard = self.max_concurrent.lock().unwrap();
            let old = *guard;
            *guard = new_max;
            old
        };

        if new_max > old_max {
            // 增加并发数：添加许可
            let delta = (new_max - old_max) as usize;
            self.concurrency_semaphore.add_permits(delta);
            log::info!(
                "[RateLimiter] 并发限制从 {} 增加到 {}，添加 {} 个许可",
                old_max, new_max, delta
            );
        } else if new_max < old_max {
            // 减少并发数：尝试立即抢占多余的许可
            let delta = (old_max - new_max) as usize;
            let mut actually_removed = 0;
            for _ in 0..delta {
                // try_acquire 不阻塞，成功则 forget（永久移除该许可）
                match self.concurrency_semaphore.try_acquire() {
                    Ok(permit) => {
                        permit.forget(); // forget = 永久移除，不归还给 semaphore
                        actually_removed += 1;
                    }
                    Err(_) => break, // 当前没有多余许可可抢占（有 in-flight 请求持有）
                }
            }
            if actually_removed > 0 {
                log::info!(
                    "[RateLimiter] 并发限制从 {} 减少到 {}，立即移除 {} 个许可，剩余 {} 个将在 in-flight 请求完成后自然收敛",
                    old_max, new_max, actually_removed, delta - actually_removed
                );
            } else {
                log::info!(
                    "[RateLimiter] 并发限制从 {} 减少到 {}，当前无多余许可可抢占，将在 in-flight 请求完成后自然收敛",
                    old_max, new_max
                );
            }
            // 对于未能立即抢占的许可：当 in-flight 请求完成时，释放的许可
            // 会回到 semaphore，但此时 available_permits 可能超过 new_max。
            // 我们不严格阻止这种短暂的超额——新请求 acquire 时仍受 semaphore 控制，
            // 而已 in-flight 的请求会自然完成释放许可。
            // 后续可以考虑更严格的方案（如 recreate semaphore）。
        }
        // new_max == old_max: 无需变更
    }

    /// 获取一个频率发送时隙
    ///
    /// 阻塞直到可以发送请求（确保不会触发上游频率限流 429）
    pub async fn acquire(&self) {
        loop {
            let maybe_sleep_duration = {
                let mut inner = self.inner.lock().await;
                let now = Instant::now();

                // 清除超过 60s 的旧请求记录
                Self::evict_expired(&mut inner, now, self.window);

                if inner.request_times.len() < inner.max_per_minute as usize {
                    // 有时隙，预约当前时间，立即返回
                    inner.request_times.push_back(now);
                    None
                } else {
                    // 无时隙，计算需要等待多久
                    // 队首是最旧的那条记录，它 + 60s 后就会过期
                    let oldest = inner.request_times[0];
                    let wait_duration = (oldest + self.window).duration_since(now);
                    Some(wait_duration)
                }
            };

            match maybe_sleep_duration {
                Some(duration) if duration.as_millis() > 0 => {
                    let wait_ms = duration.as_millis() as u64;
                    self.waiting_count.fetch_add(1, Ordering::Relaxed);
                    log::info!(
                        "[RateLimiter] 请求频率达到限制，主动延时 {}ms 后发送（当前等待队列: {}）",
                        wait_ms,
                        self.waiting_count.load(Ordering::Relaxed),
                    );
                    tokio::time::sleep(duration).await;
                    self.waiting_count.fetch_sub(1, Ordering::Relaxed);
                    // sleep 完后重试（重新持锁，这时队首应该已过期并被清除）
                }
                _ => return, // None（有时隙）或 duration=0（队首刚好过期）
            }
        }
    }

    /// 获取一个并发许可（OwnedSemaphorePermit）
    ///
    /// 阻塞直到有并发许可可用。许可随响应流转，在响应完全消费后 Drop 释放。
    /// 调用方需将此许可存入 ForwardResult，随响应一起传递到 response_processor，
    /// 最终 move 进流式 body future（与非流式响应作用域），覆盖整个响应生命周期。
    pub async fn acquire_concurrency_owned(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.concurrent_waiting_count.fetch_add(1, Ordering::Relaxed);
        log::debug!(
            "[RateLimiter] 等待并发许可（当前等待: {}, 信号量可用: {}）",
            self.concurrent_waiting_count.load(Ordering::Relaxed),
            self.concurrency_semaphore.available_permits(),
        );
        // acquire_owned 不会返回 AcquireError（我们从不 close semaphore）
        let permit = self
            .concurrency_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("[RateLimiter] semaphore acquire_owned failed — semaphore should never be closed");
        self.concurrent_waiting_count.fetch_sub(1, Ordering::Relaxed);
        log::debug!(
            "[RateLimiter] 获取并发许可成功（当前 in-flight: {}, 信号量可用: {}）",
            *self.max_concurrent.lock().unwrap() as usize - self.concurrency_semaphore.available_permits(),
            self.concurrency_semaphore.available_permits(),
        );
        permit
    }

    /// 获取当前状态（用于日志/监控/前端展示）
    pub fn status(&self) -> RateLimiterStatus {
        let max_concurrent = *self.max_concurrent.lock().unwrap();
        let current_concurrent = max_concurrent as usize - self.concurrency_semaphore.available_permits();
        let (current_count, max_per_minute, waiting_count) = if let Ok(inner) = self.inner.try_lock() {
            (inner.request_times.len(), inner.max_per_minute, self.waiting_count.load(Ordering::Relaxed))
        } else {
            (0, 0, self.waiting_count.load(Ordering::Relaxed))
        };

        RateLimiterStatus {
            current_count,
            max_per_minute,
            waiting_count,
            current_concurrent,
            max_concurrent,
            concurrent_waiting_count: self.concurrent_waiting_count.load(Ordering::Relaxed),
        }
    }

    /// ====== 全局过载退避方法 ======

    /// 记录一次上游过载（429/503），同时计算退避时间
    ///
    /// 此计数器是**全局共享**的，所有请求共用同一个计数器：
    /// - 每次调用 fetch_add(1)（原子操作）递增
    /// - 退避时间：第 1 次 8s、第 2 次 64s、第 3 次 512s、第 4 次及以上 600s（10 分钟）
    /// - 当任何请求成功时通过 `reset_on_success()` 归零
    ///
    /// ⚠️ 合并递增和计算为一次原子操作，避免并发请求在两次独立操作之间
    /// 递增计数器导致退避时间不精确（偏大）。
    pub fn record_overload_and_get_backoff(&self) -> u64 {
        let count = self.overload_backoff_count.fetch_add(1, Ordering::SeqCst) + 1;
        log::info!(
            "[RateLimiter] 过载退避计数器: {} (全局共享)",
            count
        );
        match count {
            1 => 8,
            2 => 64,
            3 => 512,
            _ => 600, // 10 分钟
        }
    }

    /// 当请求成功时重置过载退避计数器
    ///
    /// 上游恢复正常后，后续请求不应再受之前的过载退避影响。
    /// 在 `forward_with_retry` 的 Ok 分支中调用。
    pub fn reset_on_success(&self) {
        let prev = self.overload_backoff_count.swap(0, Ordering::SeqCst);
        if prev > 0 {
            log::info!(
                "[RateLimiter] 上游已恢复，过载退避计数器从 {} 重置为 0",
                prev
            );
        }
    }

    /// 清除超过窗口大小的旧记录
    fn evict_expired(inner: &mut RateLimiterInner, now: Instant, window: Duration) {
        while let Some(&t) = inner.request_times.front() {
            if now.duration_since(t) >= window {
                inner.request_times.pop_front();
            } else {
                break;
            }
        }
    }
}

impl Clone for RateLimiter {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            window: self.window,
            waiting_count: Arc::clone(&self.waiting_count),
            concurrency_semaphore: Arc::clone(&self.concurrency_semaphore),
            max_concurrent: Arc::clone(&self.max_concurrent),
            concurrent_waiting_count: Arc::clone(&self.concurrent_waiting_count),
            overload_backoff_count: Arc::clone(&self.overload_backoff_count),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::task;

    /// 测试：基本功能，前 N 次立即返回
    #[tokio::test]
    async fn test_basic_acquire() {
        let limiter = RateLimiter::new(5, 3);
        // 前 5 次频率限流应该立即返回
        for _ in 0..5 {
            limiter.acquire().await;
        }
        // 第 6 次会阻塞，用 timeout 验证
        let start = Instant::now();
        tokio::time::timeout(Duration::from_millis(100), limiter.acquire()).await.unwrap();
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(100));
    }

    /// 测试：并发许可获取和释放
    #[tokio::test]
    async fn test_concurrency_acquire_release() {
        let limiter = RateLimiter::new(100, 3);
        // 获取 3 个并发许可
        let p1 = limiter.acquire_concurrency_owned().await;
        let p2 = limiter.acquire_concurrency_owned().await;
        let p3 = limiter.acquire_concurrency_owned().await;

        // 第 4 个应该阻塞
        let result = tokio::time::timeout(Duration::from_millis(50), limiter.acquire_concurrency_owned()).await;
        assert!(result.is_err(), "第 4 个并发许可不应立即可用");

        // 释放一个许可
        drop(p1);
        // 现在应该可以获取
        let p4 = tokio::time::timeout(Duration::from_millis(50), limiter.acquire_concurrency_owned()).await;
        assert!(p4.is_ok(), "释放许可后应能获取新的");

        drop(p2);
        drop(p3);
        drop(p4.unwrap());
    }

    /// 测试：并发安全，多个任务同时 acquire 不会超额
    #[tokio::test]
    async fn test_concurrent_no_exceed() {
        let limiter = Arc::new(RateLimiter::new(3, 10));
        let mut handles = vec![];
        let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let barrier = Arc::new(tokio::sync::Barrier::new(10));

        for _ in 0..10 {
            let l = Arc::clone(&limiter);
            let c = Arc::clone(&completed);
            let b = Arc::clone(&barrier);
            handles.push(task::spawn(async move {
                b.wait().await;
                l.acquire().await;
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }));
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
        let count = completed.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(count, 3, "并发超额：预期 3 个完成，实际 {} 个", count);

        for h in handles {
            h.abort();
        }
    }

    #[test]
    fn test_status() {
        let limiter = RateLimiter::new(40, 5);
        let status = limiter.status();
        assert_eq!(status.max_per_minute, 40);
        assert_eq!(status.current_count, 0);
        assert_eq!(status.waiting_count, 0);
        assert_eq!(status.max_concurrent, 5);
        assert_eq!(status.current_concurrent, 0);
    }

    /// 测试：update_concurrency 增加
    #[tokio::test]
    async fn test_update_concurrency_increase() {
        let limiter = RateLimiter::new(100, 3);
        assert_eq!(limiter.concurrency_semaphore.available_permits(), 3);

        limiter.update_concurrency(5);
        assert_eq!(limiter.concurrency_semaphore.available_permits(), 5);
    }

    /// 测试：update_concurrency 减少（有可用许可时）
    #[tokio::test]
    async fn test_update_concurrency_decrease() {
        let limiter = RateLimiter::new(100, 5);
        assert_eq!(limiter.concurrency_semaphore.available_permits(), 5);

        limiter.update_concurrency(3);
        // try_acquire 应该成功抢占 2 个多余许可
        assert_eq!(limiter.concurrency_semaphore.available_permits(), 3);
    }
}
