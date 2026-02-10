//! Track which fields were explicitly provided ("set") for a model instance.
//!
//! This is required to implement Pydantic-compatible `exclude_unset` semantics:
//! a field with a default value should still be included if it was explicitly
//! provided at construction/validation time.

/// A compact bitset representing "field is set" for indices `0..len`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldsSet {
    len: usize,
    bits: Box<[u64]>,
}

impl FieldsSet {
    /// Create an empty (all-unset) set for `len` fields.
    #[must_use]
    pub fn empty(len: usize) -> Self {
        let words = len.div_ceil(64);
        Self {
            len,
            bits: vec![0u64; words].into_boxed_slice(),
        }
    }

    /// Create a full (all-set) set for `len` fields.
    #[must_use]
    pub fn all(len: usize) -> Self {
        let mut s = Self::empty(len);
        for idx in 0..len {
            s.set(idx);
        }
        s
    }

    /// Number of fields represented by this set.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// True if `len == 0`.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Mark a field index as set.
    ///
    /// Indices outside `0..len` are ignored (defensive for forward-compat).
    pub fn set(&mut self, idx: usize) {
        if idx >= self.len {
            return;
        }
        let word = idx / 64;
        let bit = idx % 64;
        if let Some(w) = self.bits.get_mut(word) {
            *w |= 1u64 << bit;
        }
    }

    /// Check whether a field index is set.
    #[must_use]
    pub fn is_set(&self, idx: usize) -> bool {
        if idx >= self.len {
            return false;
        }
        let word = idx / 64;
        let bit = idx % 64;
        self.bits
            .get(word)
            .is_some_and(|w| (w & (1u64 << bit)) != 0)
    }
}
