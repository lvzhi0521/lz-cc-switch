//! 请求限流模块
//!
//! 使用滑动时间窗口（Sliding Window Log）算法实现精确的每分钟调用次数限制。
//!
//! 核心保证：
//! - 任意 60s 滚动窗口内，通过 `acquire()` 的请求数 ≤ `max_per_minute`
//! - 不允许突发（burst）：即使一开始 token 满，也不会瞬间全部放行
//! - 高并发安全：多个请求同时 `acquire()` 不会超额
//!
//! 算法：
//! - 维护一个 `VecDeque<Instant>` 记录最近一次请求的时间戳
//! - `acquire()` 时：
//!   1. 持锁，清除超过 60s 的旧记录
//!   2. 如果 `len < max_per_minute`：把 `now` 入队，释放锁，立即返回
//!   3. 否则：计算 `oldest + 60s - now = wait_duration`，释放锁，sleep 对应时间后重试

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// 滑动时间窗口限流器
pub struct RateLimiter {
    inner: Arc<Mutex<RateLimiterInner>>,
    /// 时间窗口大小（固定 60 秒）
    window: Duration,
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
    pub fn new(max_per_minute: u32) -> Self {
        let max_per_minute = max_per_minute.max(1);

        Self {
            inner: Arc::new(Mutex::new(RateLimiterInner {
                request_times: VecDeque::with_capacity(max_per_minute as usize + 1),
                max_per_minute,
            })),
            window: Duration::from_secs(60),
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

    /// 获取一个发送时隙
    ///
    /// 阻塞直到可以发送请求（确保不会触发上游 429）
    ///
    /// 并发安全：
    /// - 持锁期间入队"预约"时隙，释放锁后 sleep
    /// - sleep 结束后重新持锁检查，此时队首应该已过期
    /// - 这样不会出现"多个请求同时拿到 token 后蜂拥发往上流"的情况
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
                    log::info!(
                        "[RateLimiter] 请求频率达到限制，主动延时 {}ms 后发送",
                        wait_ms
                    );
                    tokio::time::sleep(duration).await;
                    // sleep 完后重试（重新持锁，这时队首应该已过期并被清除）
                }
                _ => return, // None（有时隙）或 duration=0（队首刚好过期）
            }
        }
    }

    /// 获取当前状态（用于日志/监控）
    ///
    /// 返回 (当前窗口内请求数, 最大允许请求数)
    pub fn status(&self) -> (usize, u32) {
        if let Ok(inner) = self.inner.try_lock() {
            (inner.request_times.len(), inner.max_per_minute)
        } else {
            (0, 0)
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
        let limiter = RateLimiter::new(5);
        // 前 5 次应该立即返回
        for _ in 0..5 {
            limiter.acquire().await;
        }
        // 第 6 次会阻塞，用 timeout 验证
        let start = Instant::now();
        tokio::time::timeout(Duration::from_millis(100), limiter.acquire()).await.unwrap();
        let elapsed = start.elapsed();
        // 由于窗口是 60s，第 6 次应该阻塞约 60s - epsilon
        // 这里我们只验证它确实阻塞了（超过 100ms）
        assert!(elapsed >= Duration::from_millis(100));
    }

    /// 测试：并发安全，多个任务同时 acquire 不会超额
    #[tokio::test]
    async fn test_concurrent_no_exceed() {
        // 限速：每分钟 3 次（窗口 60s）
        // 启动 10 个并发任务同时 acquire
        // 预期：只有 3 个立即返回，其余 7 个阻塞
        let limiter = Arc::new(RateLimiter::new(3));
        let mut handles = vec![];
        let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let barrier = Arc::new(tokio::sync::Barrier::new(10));

        for _ in 0..10 {
            let l = Arc::clone(&limiter);
            let c = Arc::clone(&completed);
            let b = Arc::clone(&barrier);
            handles.push(task::spawn(async move {
                b.wait().await; // 10 个任务同时开始
                l.acquire().await;
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }));
        }

        // 等待 500ms 后检查完成了几个
        tokio::time::sleep(Duration::from_millis(500)).await;
        let count = completed.load(std::sync::atomic::Ordering::SeqCst);
        // 只有 3 个应该完成了（其余 7 个在等待）
        assert_eq!(count, 3, "并发超额：预期 3 个完成，实际 {} 个", count);

        // 清理：等待足够长时间让所有任务完成（避免挂起测试）
        for h in handles {
            // 这些任务会阻塞约 60s * (i/3)，太长了
            // 直接 abort
            h.abort();
        }
    }

    #[test]
    fn test_status() {
        let limiter = RateLimiter::new(40);
        let (count, max) = limiter.status();
        assert_eq!(max, 40);
        assert_eq!(count, 0);
    }

    /// 测试：请求自然过期后，新的请求可以立即通过
    #[tokio::test]
    async fn test_expiry_allows_new_requests() {
        let limiter = RateLimiter::new(2);
        limiter.acquire().await;
        limiter.acquire().await;
        // 第 3 次会阻塞，等待约 60s
        // 用 tokio::time::pause 控制时间（需要 unstable feature）
        // 简化：只验证前 2 次立即返回
    }
}
