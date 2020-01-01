pub trait Partition2Ext: Iterator {
    fn partition2<B, C, F>(self, f: F) -> (B, C)
    where
        Self: Sized,
        B: Default + Extend<Self::Item>,
        C: Default + Extend<Self::Item>,
        F: FnMut(&Self::Item) -> bool;
}

impl<I: Iterator> Partition2Ext for I {
    fn partition2<B, C, F>(self, mut f: F) -> (B, C)
    where
        Self: Sized,
        B: Default + Extend<Self::Item>,
        C: Default + Extend<Self::Item>,
        F: FnMut(&Self::Item) -> bool,
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
