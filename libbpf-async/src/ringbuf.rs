// Copyright (C) 2021 and 2022 The libbpf-async Authors.
//
// Licensed under LGPL-2.1 or BSD-2-Clause.

use core::task::{Context, Poll};
use libbpf_rs::{Map, MapCore};
use std::io::Result;
use std::num::NonZeroUsize;
use std::os::fd::{AsFd, AsRawFd, RawFd};
use std::ptr::NonNull;
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, ReadBuf};

const BPF_RINGBUF_BUSY_BIT: u32 = 1 << 31;
const BPF_RINGBUF_DISCARD_BIT: u32 = 1 << 30;
const BPF_RINGBUF_HDR_SZ: u32 = 8;

/// A synchronous version of RingBuffer that can be created outside of tokio runtime
pub struct SyncRingBuffer {
    mask: u64,
    fd: RawFd,
    consumer: NonNull<core::ffi::c_void>,
    producer: NonNull<core::ffi::c_void>,
    data: NonNull<core::ffi::c_void>,
}

unsafe impl Send for SyncRingBuffer {}
unsafe impl Sync for SyncRingBuffer {}

impl SyncRingBuffer {
    /// Create a new SyncRingBuffer from a Map
    pub fn new(map: &Map) -> Self {
        let max_entries = map.max_entries();
        let psize = page_size::get();
        let fd = map.as_fd().as_raw_fd();
        let consumer = unsafe {
            nix::sys::mman::mmap(
                None,
                NonZeroUsize::new(psize).expect("page size must not be zero"),
                nix::sys::mman::ProtFlags::PROT_WRITE | nix::sys::mman::ProtFlags::PROT_READ,
                nix::sys::mman::MapFlags::MAP_SHARED,
                fd,
                0,
            )
            .unwrap()
        };
        let producer = unsafe {
            nix::sys::mman::mmap(
                None,
                NonZeroUsize::new(psize + 2 * max_entries as usize)
                    .expect("page size + 2 * max_entries must not be zero"),
                nix::sys::mman::ProtFlags::PROT_READ,
                nix::sys::mman::MapFlags::MAP_SHARED,
                fd,
                psize as i64,
            )
            .unwrap()
        };

        // SAFETY: These pointers come from mmap and are guaranteed to be non-null
        let consumer = unsafe { NonNull::new_unchecked(consumer) };
        let producer = unsafe { NonNull::new_unchecked(producer) };
        let data = unsafe { NonNull::new_unchecked(producer.as_ptr().add(psize)) };

        SyncRingBuffer {
            mask: (max_entries - 1) as u64,
            fd,
            consumer,
            producer,
            data,
        }
    }

    /// Convert this SyncRingBuffer into an async RingBuffer
    /// This must be called within a tokio runtime
    pub fn into_async(self) -> Result<RingBuffer> {
        let async_fd = AsyncFd::with_interest(self.fd, tokio::io::Interest::READABLE)?;
        
        Ok(RingBuffer {
            mask: self.mask,
            async_fd,
            consumer: self.consumer,
            producer: self.producer,
            data: self.data,
            // Don't drop the original SyncRingBuffer fields
            _sync_rb: Some(self)
        })
    }
}

impl Drop for SyncRingBuffer {
    fn drop(&mut self) {
        let psize = page_size::get();
        unsafe {
            let _ = nix::sys::mman::munmap(self.consumer.as_ptr(), psize);
            let _ = nix::sys::mman::munmap(self.producer.as_ptr(), psize + 2 * (self.mask as usize + 1));
        }
    }
}

/// The async version of RingBuffer that can be used with tokio
pub struct RingBuffer {
    mask: u64,
    async_fd: AsyncFd<RawFd>,
    consumer: NonNull<core::ffi::c_void>,
    producer: NonNull<core::ffi::c_void>,
    data: NonNull<core::ffi::c_void>,
    // This field is used to prevent double-free when RingBuffer is created from SyncRingBuffer
    _sync_rb: Option<SyncRingBuffer>,
}

unsafe impl Send for RingBuffer {}
unsafe impl Sync for RingBuffer {}

/// Helper function for rounding up the length
fn roundup_len(mut len: u32) -> u32 {
    len <<= 2;
    len >>= 2;
    len += BPF_RINGBUF_HDR_SZ;
    (len + 7) / 8 * 8
}

impl RingBuffer {
    /// Create a new RingBuffer from a Map
    /// This must be called within a tokio runtime
    pub fn new(map: &Map) -> Self {
        let sync_rb = SyncRingBuffer::new(map);
        sync_rb.into_async().unwrap()
    }
}

impl Drop for RingBuffer {
    fn drop(&mut self) {
        // Only free memory if this RingBuffer wasn't created from a SyncRingBuffer
        if self._sync_rb.is_none() {
            let psize = page_size::get();
            unsafe {
                let _ = nix::sys::mman::munmap(self.consumer.as_ptr(), psize);
                let _ = nix::sys::mman::munmap(self.producer.as_ptr(), psize + 2 * (self.mask as usize + 1));
            }
        }
    }
}

impl AsyncRead for RingBuffer {
    fn poll_read(
        self: core::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let mut cons_pos =
            unsafe { std::ptr::read_volatile(self.consumer.as_ptr() as *const std::os::raw::c_ulong) };
        std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
        loop {
            let prod_pos =
                unsafe { std::ptr::read_volatile(self.producer.as_ptr() as *const std::os::raw::c_ulong) };
            std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
            if cons_pos < prod_pos {
                let len_ptr = unsafe { self.data.as_ptr().offset((cons_pos & self.mask) as isize) };
                let mut len = unsafe { std::ptr::read_volatile(len_ptr as *const u32) };
                std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);

                if (len & BPF_RINGBUF_BUSY_BIT) == 0 {
                    cons_pos += roundup_len(len) as u64;
                    if (len & BPF_RINGBUF_DISCARD_BIT) == 0 {
                        let sample = unsafe {
                            std::slice::from_raw_parts_mut(
                                len_ptr.offset(BPF_RINGBUF_HDR_SZ as isize) as *mut u8,
                                len as usize,
                            )
                        };
                        len = std::cmp::min(len, buf.capacity() as u32);
                        buf.put_slice(&sample[..len as usize]);
                    }
                    std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
                    unsafe {
                        std::ptr::write_volatile(
                            self.consumer.as_ptr() as *mut std::os::raw::c_ulong,
                            cons_pos,
                        )
                    };
                    if (len & BPF_RINGBUF_DISCARD_BIT) == 0 {
                        return Poll::Ready(Ok(()));
                    } else {
                        continue;
                    }
                }
            }
            let mut ev = futures::ready!(self.async_fd.poll_read_ready(cx))?;
            ev.clear_ready();
        }
    }
}