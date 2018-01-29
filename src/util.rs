use std::ops::DerefMut;

use futures::{Future, Poll};

pub trait Partition2Ext: Iterator {
    fn partition2<B, C, F>(self, f: F) -> (B, C)
        where Self: Sized,
              B: Default + Extend<Self::Item>,
              C: Default + Extend<Self::Item>,
              F: FnMut(&Self::Item) -> bool;
}

impl<I: Iterator> Partition2Ext for I {
    fn partition2<B, C, F>(self, mut f: F) -> (B, C)
        where Self: Sized,
              B: Default + Extend<Self::Item>,
              C: Default + Extend<Self::Item>,
              F: FnMut(&Self::Item) -> bool
    {
        let mut left: B = Default::default();
        let mut right: C = Default::default();

        for x in self {
            if f(&x) {
                left.extend(Some(x));
            } else {
                right.extend(Some(x));
            }
        }

        (left, right)
    }
}

pub struct FutureDerefMutWrapper<T>(T);

pub fn wrap_deref_mut<T, U>(t: T) -> FutureDerefMutWrapper<T>
    where T: DerefMut<Target = U>,
          U: Future + ?Sized
{
    FutureDerefMutWrapper(t)
}

impl<T, U> Future for FutureDerefMutWrapper<T>
    where T: DerefMut<Target = U>,
          U: Future + ?Sized
{
    type Item = U::Item;
    type Error = U::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}
