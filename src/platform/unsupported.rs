//! Concurrency utilities for memory mapping operations.
//!
//! This module provides concurrency primitives optimized for memory mapping
//! operations.

use std::sync::{RwLock as StdRwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::atomic::{AtomicPtr as StdAtomicPtr, Ordering};
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};

/// A read-write lock optimized for memory mapping operations.
///
/// This is a thin wrapper around the standard library's `RwLock` with
/// additional optimizations for memory mapping operations.
pub struct RwLock<T> {
    inner: StdRwLock<T>,
}

impl<T> RwLock<T> {
    /// Create a new read-write lock.
    #[inline]
    pub fn new(value: T) -> RwLock<T> {
        RwLock {
            inner: StdRwLock::new(value),
        }
    }

    /// Acquire a read lock.
    #[inline]
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.inner.read().unwrap()
    }

    /// Acquire a write lock.
    #[inline]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.inner.write().unwrap()
    }

    /// Try to acquire a read lock.
    #[inline]
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        self.inner.try_read().ok()
    }

    /// Try to acquire a write lock.
    #[inline]
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        self.inner.try_write().ok()
    }

    /// Consume the lock and return the inner value.
    #[inline]
    pub fn into_inner(self) -> T {
        self.inner.into_inner().unwrap()
    }
}

impl<T: Default> Default for RwLock<T> {
    #[inline]
    fn default() -> RwLock<T> {
        RwLock::new(T::default())
    }
}

/// An atomic pointer optimized for memory mapping operations.
///
/// This is a thin wrapper around the standard library's `AtomicPtr` with
/// additional optimizations for memory mapping operations.
pub struct AtomicPtr<T> {
    inner: StdAtomicPtr<T>,
}

impl<T> AtomicPtr<T> {
    /// Create a new atomic pointer.
    #[inline]
    pub fn new(ptr: *mut T) -> AtomicPtr<T> {
        AtomicPtr {
            inner: StdAtomicPtr::new(ptr),
        }
    }

    /// Load the pointer.
    #[inline]
    pub fn load(&self, order: Ordering) -> *mut T {
        self.inner.load(order)
    }

    /// Store a new pointer.
    #[inline]
    pub fn store(&self, ptr: *mut T, order: Ordering) {
        self.inner.store(ptr, order);
    }

    /// Swap the pointer with a new one.
    #[inline]
    pub fn swap(&self, ptr: *mut T, order: Ordering) -> *mut T {
        self.inner.swap(ptr, order)
    }

    /// Compare and swap the pointer.
    #[inline]
    pub fn compare_and_swap(&self, current: *mut T, new: *mut T, order: Ordering) -> *mut T {
        self.inner.compare_and_swap(current, new, order)
    }

    /// Compare and exchange the pointer.
    #[inline]
    pub fn compare_exchange(
        &self,
        current: *mut T,
        new: *mut T,
        success: Ordering,
        failure: Ordering,
    ) -> Result<*mut T, *mut T> {
        self.inner.compare_exchange(current, new, success, failure)
    }

    /// Compare and exchange the pointer with weak ordering.
    #[inline]
    pub fn compare_exchange_weak(
        &self,
        current: *mut T,
        new: *mut T,
        success: Ordering,
        failure: Ordering,
    ) -> Result<*mut T, *mut T> {
        self.inner.compare_exchange_weak(current, new, success, failure)
    }
}

impl<T> Default for AtomicPtr<T> {
    #[inline]
    fn default() -> AtomicPtr<T> {
        AtomicPtr::new(std::ptr::null_mut())
    }
}

/// A memory fence.
///
/// This function is used to ensure that memory operations are ordered
/// correctly.
#[inline]
pub fn fence(order: Ordering) {
    std::sync::atomic::fence(order);
}