# kameo-child-process

We can run Handler logic for a kameo actor in a subprocess, 1:1 with the actor instance in the parent process.

---

## Why kameo-child-process?

This crate exists to let you run the important parts of a kameo actor's handler logic in a separate subprocess, fully isolated from the parent process. This isolation provides:
- **Safety:** Bugs, panics, or resource leaks in the handler can't take down the parent.
- **Language/runtime agnosticism:** The handler can be implemented in any language or runtime that speaks the message protocol.


## Macros for Actor Registration and Main Setup

### `register_subprocess_actors!`

Registers one or more actor types for subprocess use. When the process is launched as a child, this macro dispatches to the correct actor handler loop based on the environment.

**Example:**
```rust
register_subprocess_actors! {
    actors = {
        (MyActor, MyMessage, MyCallback),
        (OtherActor, OtherMessage, OtherCallback),
    }
}

fn main() {
    if let Some(result) = maybe_run_subprocess_registry() {
        // This is a child process: run the actor loop
        result.unwrap();
        return;
    }
    // Parent process logic continues here
}
```

### `setup_subprocess_system!`

Sets up the complete subprocess actor system, including custom runtime initialization for both parent and child. This macro ensures the correct main function structure and dispatch for robust, cross-platform actor isolation.

**Example:**
```rust
setup_subprocess_system! {
    actors = {
        (MyActor, MyMessage, MyCallback),
    },
    child_init = {
        tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .init();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to build runtime")
    },
    parent_init = {
        tracing_subscriber::fmt()
            .with_env_filter("info")
            .init();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        runtime.block_on(async {
            // Spawn and interact with your actor(s) here
            Ok::<(), Box<dyn std::error::Error>>(())
        })
    }
}
```

## Example Usage

```rust
use kameo_child_process::prelude::*;
use tracing::Level;

setup_subprocess_system! {
    actors = {
        (MyActor, MyMessage, MyCallback),
    },
    child_init = {
        tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .init();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to build runtime")
    },
    parent_init = {
        tracing_subscriber::fmt()
            .with_env_filter("info")
            .init();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        runtime.block_on(async {
            // Spawn and interact with your actor(s) here
            Ok::<(), Box<dyn std::error::Error>>(())
        })
    }
}
```

// Define your message, reply, and callback types, and implement KameoChildProcessMessage for your message.

