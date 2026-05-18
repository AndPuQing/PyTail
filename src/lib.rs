#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod cache;
pub mod config;
pub mod hot_cache;
pub mod range;
pub mod server;
pub mod simple;
pub mod upstream;
