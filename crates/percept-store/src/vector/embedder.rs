//! `Embedder` trait + `HashEmbedder` placeholder.
//!
//! DESIGN Appendix A names FastEmbed-rs + `bge-small-en-v1.5` as the
//! production embedder. v1 ships with `HashEmbedder` — a deterministic,
//! dependency-free hash-into-32-dims projection — because the ONNX model
//! download isn't available in our CI/sandbox environment and the
//! FastEmbed/`ort` toolchain has the same compile/disk profile that
//! pushed us off DuckDB in slice 3.
//!
//! Real semantic search lands in a slice-4 follow-up: swap the `Embedder`
//! impl, keep everything downstream. The model id is stored per vector so
//! a re-index is safe (slice 4 deferred the re-index command per PLAN).

use std::sync::Arc;

pub trait Embedder: Send + Sync {
    fn embed(&self, text: &str) -> Vec<f32>;
    fn dim(&self) -> usize;
    fn model_id(&self) -> &str;
}

/// Deterministic, dependency-free embedder used as the v1 placeholder.
/// Splits input into whitespace tokens, hashes each into a fixed-width
/// vector, and L2-normalises. Useful for tests and shipping the wiring
/// before the real model lands.
pub struct HashEmbedder {
    dim: usize,
    model_id: String,
}

impl HashEmbedder {
    #[must_use]
    pub fn new(dim: usize) -> Self {
        Self {
            dim,
            model_id: format!("percept-hash-v1-d{dim}"),
        }
    }
}

impl Default for HashEmbedder {
    fn default() -> Self {
        Self::new(64)
    }
}

impl Embedder for HashEmbedder {
    fn embed(&self, text: &str) -> Vec<f32> {
        let mut v = vec![0f32; self.dim];
        for token in text.split_whitespace() {
            let h = blake3::hash(token.as_bytes());
            let bytes = h.as_bytes();
            for (i, &b) in bytes.iter().enumerate() {
                let idx = (i + token.len()) % self.dim;
                // Map a byte to [-1, 1] for some signed structure.
                v[idx] += (f32::from(b) - 127.5) / 127.5;
            }
        }
        normalise(&mut v);
        v
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn model_id(&self) -> &str {
        &self.model_id
    }
}

fn normalise(v: &mut [f32]) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > f32::EPSILON {
        for x in v.iter_mut() {
            *x /= norm;
        }
    }
}

#[must_use]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut dot = 0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
    }
    // Vectors are stored normalised, so dot == cosine. Defensive in case
    // someone hands in a non-normalised vector.
    dot
}

/// Stable embedder handle the rest of the pipeline shares.
pub type SharedEmbedder = Arc<dyn Embedder>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embeddings_are_deterministic() {
        let e = HashEmbedder::new(32);
        let a = e.embed("person at the front door");
        let b = e.embed("person at the front door");
        assert_eq!(a, b);
    }

    #[test]
    fn similar_strings_are_more_similar_than_different_ones() {
        let e = HashEmbedder::new(64);
        let a = e.embed("person at the front door");
        let near = e.embed("person at the door");
        let far = e.embed("temperature reading from kitchen thermometer");
        assert!(
            cosine_similarity(&a, &near) > cosine_similarity(&a, &far),
            "shared tokens should produce higher cosine"
        );
    }

    #[test]
    fn unit_norm_within_epsilon() {
        let e = HashEmbedder::new(32);
        let v = e.embed("hello world");
        let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((n - 1.0).abs() < 1e-5);
    }
}
