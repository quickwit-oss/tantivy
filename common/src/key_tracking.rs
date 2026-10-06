/// Initial capacity of the `Vec<u8>` key buffer, large enough for most keys.
const DEFAULT_KEY_CAPACITY: usize = 100;

/// Buffer in which a term dictionary streamer keeps track of the current key.
///
/// - `Vec<u8>` keeps track of the current key.
/// - [`WithoutKeys`] skips this work, for callers that only need term ordinals or values.
pub trait KeyTracking {
    /// Returns the buffer a streamer starts with.
    fn make_default() -> Self;

    /// Sets the current key to `key`.
    fn set_key(&mut self, key: &[u8]);

    /// Sets the current key to its first `common_prefix_len` bytes, followed by `suffix`.
    ///
    /// The current key is not always a key of the dictionary. For instance, when positioning
    /// itself on its lower bound, the sstable streamer sets the current key to a prefix of
    /// the lower bound, and then applies its first entry with this method.
    fn update_with_prefix(&mut self, common_prefix_len: usize, suffix: &[u8]);
}

impl KeyTracking for Vec<u8> {
    #[inline]
    fn make_default() -> Self {
        Vec::with_capacity(DEFAULT_KEY_CAPACITY)
    }

    #[inline(always)]
    fn set_key(&mut self, key: &[u8]) {
        self.clear();
        self.extend_from_slice(key);
    }

    #[inline(always)]
    fn update_with_prefix(&mut self, common_prefix_len: usize, suffix: &[u8]) {
        self.truncate(common_prefix_len);
        self.extend_from_slice(suffix);
    }
}

/// [`KeyTracking`] that does not keep track of keys.
///
/// A streamer using it only gives access to term ordinals and values.
#[derive(Default)]
pub struct WithoutKeys;

impl KeyTracking for WithoutKeys {
    #[inline]
    fn make_default() -> Self {
        WithoutKeys
    }

    #[inline(always)]
    fn set_key(&mut self, _key: &[u8]) {}

    #[inline(always)]
    fn update_with_prefix(&mut self, _common_prefix_len: usize, _suffix: &[u8]) {}
}

#[cfg(test)]
mod tests {
    use super::KeyTracking;

    #[test]
    fn test_vec_key_tracking() {
        let mut key: Vec<u8> = Vec::make_default();
        key.update_with_prefix(0, b"abc");
        assert_eq!(key, b"abc");
        key.update_with_prefix(2, b"xy");
        assert_eq!(key, b"abxy");
        key.update_with_prefix(0, b"z");
        assert_eq!(key, b"z");
    }

    #[test]
    fn test_vec_set_key() {
        let mut key: Vec<u8> = Vec::make_default();
        key.set_key(b"abc");
        assert_eq!(key, b"abc");
        key.set_key(b"de");
        assert_eq!(key, b"de");
        key.set_key(b"");
        assert!(key.is_empty());
    }
}
