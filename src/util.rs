use std::future::Future;

/// See `maybe_add_send`
#[cfg(not(target_family = "wasm"))]
#[macro_export]
macro_rules! maybe_add_send_sync {
    ($($tt:tt)*) => {
        $($tt)* + Send + Sync
    };
}

/// See `maybe_add_send`
#[cfg(target_family = "wasm")]
#[macro_export]
macro_rules! maybe_add_send_sync {
    ($($tt:tt)*) => {
        $($tt)*
    };
}

#[macro_export]
macro_rules! async_trait_maybe_send {
    ($($tt:tt)*) => {
        #[cfg_attr(not(target_family = "wasm"), ::async_trait::async_trait)]
        #[cfg_attr(target_family = "wasm", ::async_trait::async_trait(?Send))]
        $($tt)*
    };
}

#[cfg(not(target_family = "wasm"))]
pub fn spawn<F>(_name: &str, future: F)
    where
        F: Future<Output=()> + 'static + Send,
{
    tokio::spawn(future);
}

#[cfg(target_family = "wasm")]
pub fn spawn<F>(_name: &str, future: F)
    where
        F: Future<Output=()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}
