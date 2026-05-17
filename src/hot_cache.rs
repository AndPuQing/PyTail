use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

#[derive(Debug)]
pub struct BoundedLruCache<K, V> {
    entries: HashMap<K, CacheEntry<V>>,
    order: VecDeque<(K, u64)>,
    capacity: usize,
    next_sequence: u64,
}

#[derive(Debug)]
struct CacheEntry<V> {
    value: V,
    sequence: u64,
}

impl<K, V> BoundedLruCache<K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            capacity,
            next_sequence: 0,
        }
    }

    pub fn get_cloned(&mut self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let value = self.entries.get(key)?.value.clone();
        self.touch(key.clone());
        Some(value)
    }

    pub fn insert(&mut self, key: K, value: V) {
        if self.capacity == 0 {
            return;
        }

        if let Some(entry) = self.entries.get_mut(&key) {
            entry.value = value;
            self.touch(key);
            return;
        }

        while self.entries.len() >= self.capacity {
            if !self.evict_lru() {
                break;
            }
        }

        let sequence = self.advance_sequence();
        self.entries
            .insert(key.clone(), CacheEntry { value, sequence });
        self.order.push_back((key, sequence));
    }

    #[cfg(test)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    #[cfg(test)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.entries.remove(key).map(|entry| entry.value)
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn touch(&mut self, key: K) {
        let sequence = self.advance_sequence();
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.sequence = sequence;
            self.order.push_back((key, sequence));
        }
    }

    fn evict_lru(&mut self) -> bool {
        while let Some((key, sequence)) = self.order.pop_front() {
            if self
                .entries
                .get(&key)
                .is_some_and(|entry| entry.sequence == sequence)
            {
                self.entries.remove(&key);
                return true;
            }
        }
        false
    }

    fn advance_sequence(&mut self) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        sequence
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_least_recently_used_entry() {
        let mut cache = BoundedLruCache::new(2);
        cache.insert("a", 1);
        cache.insert("b", 2);

        assert_eq!(cache.get_cloned(&"a"), Some(1));
        cache.insert("c", 3);

        assert!(cache.contains_key(&"a"));
        assert!(!cache.contains_key(&"b"));
        assert!(cache.contains_key(&"c"));
    }

    #[test]
    fn caps_entries_after_repeated_updates() {
        let mut cache = BoundedLruCache::new(2);
        cache.insert("a", 1);
        cache.insert("a", 2);
        cache.insert("b", 3);
        cache.insert("c", 4);

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get_cloned(&"a"), None);
        assert_eq!(cache.get_cloned(&"b"), Some(3));
        assert_eq!(cache.get_cloned(&"c"), Some(4));
    }
}
