//! CPU affinity helper —— 把当前线程钉到指定 CPU
//!
//! 在 hot path 启动 [`super::Proactor`] 之前调一次，让 kernel scheduler 不再
//! 把这条线程迁出，保持 cache / TLB / branch predictor 状态稳定。
//!
//! 生产部署还要做的（运维层，本 crate 不管）：
//! - kernel cmdline `isolcpus=N` 把 CPU 隔出来不给普通 task
//! - `IRQ` affinity 把网卡中断也固定到非 isolated 的 CPU
//! - `nohz_full=N` 关 tick 中断
//!
//! F3.2 只做最基本的：当前线程 `sched_setaffinity`。其余运维层留到 deploy。

use std::io;

#[derive(Debug, thiserror::Error)]
pub enum AffinityError {
    #[error("cpu {requested} out of range (cpu_set_t supports ids below {available})")]
    OutOfRange { requested: usize, available: usize },
    #[error("sched_setaffinity failed for cpu {cpu}: {source}")]
    SetFailed {
        cpu: usize,
        #[source]
        source: io::Error,
    },
}

impl From<AffinityError> for io::Error {
    fn from(e: AffinityError) -> Self {
        Self::other(e.to_string())
    }
}

/// 把当前线程钉到 Linux logical CPU id `cpu`（即 `/proc/cpuinfo` 的
/// `processor` 编号）。
///
/// 调用必须在线程进入 hot loop 前完成。pinning 是 best-effort——失败会返回
/// 错误，caller 决定 hard-fail 还是 warn-and-continue（ripple 的经验：基建层
/// 用 warn）。
pub fn pin_current_thread_to(cpu: usize) -> Result<(), AffinityError> {
    let available = libc::CPU_SETSIZE as usize;
    if cpu >= available {
        return Err(AffinityError::OutOfRange {
            requested: cpu,
            available,
        });
    }

    // SAFETY: cpu_set_t 是 POD，0-init 合法；cpu < CPU_SETSIZE 已在上面检查。
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe { libc::CPU_SET(cpu, &mut set) };
    // pid=0 表示当前线程。
    // SAFETY: set 仍 alive 且指向有效 cpu_set_t。
    let rc = unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) };
    if rc == 0 {
        Ok(())
    } else {
        Err(AffinityError::SetFailed {
            cpu,
            source: io::Error::last_os_error(),
        })
    }
}

/// 把当前线程的 affinity 重置到 kernel / cgroup 允许的全部 CPU。
///
/// 用途：benchmark 在同进程内跑多个 variant 时，pinned variant 之后要 unpin
/// 才能让后续 unpinned variant 拿到真实测量。
pub fn unpin_current_thread() -> Result<(), AffinityError> {
    // SAFETY: cpu_set_t 是 POD，0-init 合法。
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    for cpu in 0..libc::CPU_SETSIZE as usize {
        // SAFETY: cpu < CPU_SETSIZE。
        unsafe { libc::CPU_SET(cpu, &mut set) };
    }
    // pid=0 表示当前线程。kernel 会把 mask 与 cpuset cgroup 的允许集合相交。
    // SAFETY: set 仍 alive 且指向有效 cpu_set_t。
    let rc = unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) };
    if rc == 0 {
        Ok(())
    } else {
        Err(AffinityError::SetFailed {
            cpu: usize::MAX,
            source: io::Error::last_os_error(),
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
fn first_allowed_cpu() -> usize {
    // SAFETY: cpu_set_t 是 POD，0-init 合法。
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: set 指向有效 cpu_set_t。
    let rc =
        unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
    assert_eq!(rc, 0, "sched_getaffinity failed");
    (0..libc::CPU_SETSIZE as usize)
        .find(|&cpu| {
            // SAFETY: cpu < CPU_SETSIZE。
            unsafe { libc::CPU_ISSET(cpu, &set) }
        })
        .expect("current thread must have at least one allowed CPU")
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn pin_to_allowed_cpu_succeeds() {
        pin_current_thread_to(first_allowed_cpu()).unwrap();
    }

    #[test]
    fn out_of_range_cpu_returns_error() {
        let r = pin_current_thread_to(usize::MAX);
        assert!(matches!(r, Err(AffinityError::OutOfRange { .. })));
    }
}
