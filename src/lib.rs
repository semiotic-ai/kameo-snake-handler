use serde::{Serialize, Deserialize};
use ipc_channel::ipc::{IpcSender, IpcReceiver, IpcOneShotServer};
use std::env;
use std::process::Command;
use tracing::{trace, debug};
use kameo::{Actor, message::{Context, Message as KameoMessage}};

/// Trait for messages that can be sent to a Kameo child process actor.
pub trait KameoChildProcessMessage: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static {}

impl<T> KameoChildProcessMessage for T where T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static {}

use serde::{Serializer, Deserializer};
use serde::de::{self, Visitor, EnumAccess, VariantAccess};
use std::fmt;

#[derive(Debug, Serialize, Deserialize)]
pub enum Control<T> {
    Handshake,
    Real(T),
}

impl<T> Control<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    pub fn is_handshake(&self) -> bool {
        matches!(self, Control::Handshake)
    }
    pub fn into_real(self) -> Option<T> {
        match self {
            Control::Real(t) => Some(t),
            _ => None,
        }
    }
}

/// Encapsulated handshake protocol for parent/child IPC.
pub mod handshake {
    use super::{Control, KameoChildProcessMessage};
    use serde::{Serialize, de::DeserializeOwned, Deserialize};
    use bincode;
    use ipc_channel::ipc::{IpcSender, IpcReceiver, IpcOneShotServer};
    use std::env;
    use std::process::Command;
    use tracing::{trace, debug};

    /// Parent side handshake. Spawns the child and returns a typed (tx, rx) pair.
    pub fn parent<M, R>(cmd: &mut std::process::Command) -> ((IpcSender<Control<M>>, IpcReceiver<Control<R>>), std::process::Child)
    where
        M: Serialize + DeserializeOwned + Send + Sync + 'static,
        R: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        // 1. Parent creates a one-shot server for initial handle exchange
        let (server, server_name) = IpcOneShotServer::<IpcSender<Vec<u8>>>::new().unwrap();
        cmd.env("KAMEO_ONESHOT_SERVER", &server_name);
        let child = cmd.spawn().expect("Failed to spawn child");
        trace!("Parent: waiting for child to connect to one-shot server");
        let (rx, child_sender): (IpcReceiver<IpcSender<Vec<u8>>>, IpcSender<Vec<u8>>) = server.accept().unwrap();
        trace!("Parent: child connected, received channel from child");
        let child_sender = child_sender;

        // 2. Parent creates reply server and sends reply_name to child
        let (reply_server, reply_name) = IpcOneShotServer::<Vec<u8>>::new().unwrap();
        child_sender.send(encode_real(reply_name.clone())).unwrap();
        trace!("Parent: sent reply_name to child: {}", reply_name);

        // 3. Parent returns (tx, rx) for typed comms
        let tx = unsafe { std::mem::transmute::<IpcSender<Vec<u8>>, IpcSender<Control<M>>>(child_sender) };
        let (rx_reply, _): (IpcReceiver<Vec<u8>>, Vec<u8>) = reply_server.accept().unwrap();
        let rx = unsafe { std::mem::transmute::<IpcReceiver<Vec<u8>>, IpcReceiver<Control<R>>>(rx_reply) };
        ((tx, rx), child)
    }

    /// Child side handshake. Returns a typed (tx, rx) pair.
    pub fn child<M, R>() -> (IpcSender<Control<R>>, IpcReceiver<Control<M>>)
    where
        M: Serialize + DeserializeOwned + Send + Sync + 'static,
        R: Serialize + Send + Sync + 'static,
    {
        let server_name = std::env::var("KAMEO_ONESHOT_SERVER").expect("KAMEO_ONESHOT_SERVER not set");
        trace!("Child: connecting to parent's one-shot server");
        let tx: IpcSender<IpcSender<Vec<u8>>> = IpcSender::connect(server_name).unwrap();
        // Child creates a channel for ongoing comms
        let (child_sender, child_receiver): (IpcSender<Vec<u8>>, IpcReceiver<Vec<u8>>) = ipc_channel::ipc::channel().unwrap();
        tx.send(child_sender).unwrap();
        trace!("Child: sent channel to parent");

        // Wait for reply_name from parent
        let reply_name_bytes = child_receiver.recv().unwrap();
        let reply_name: String = decode_control(&reply_name_bytes).into_real().unwrap();
        trace!("Child: received reply_name from parent: {}", reply_name);

        // Connect to reply channel
        let reply_sender: IpcSender<Vec<u8>> = IpcSender::connect(reply_name).unwrap();
        let tx = unsafe { std::mem::transmute::<IpcSender<Vec<u8>>, IpcSender<Control<R>>>(reply_sender) };
        let rx = unsafe { std::mem::transmute::<IpcReceiver<Vec<u8>>, IpcReceiver<Control<M>>>(child_receiver) };
        (tx, rx)
    }

    /// Typed sender adapter
    pub struct TypedSender<T> {
        inner: IpcSender<Vec<u8>>,
        _phantom: std::marker::PhantomData<T>,
    }
    impl<T: Serialize> TypedSender<T> {
        pub fn new(inner: IpcSender<Vec<u8>>) -> IpcSender<T> {
            // Safety: T must be Serialize + DeserializeOwned
            unsafe { std::mem::transmute(inner) }
        }
    }
    /// Typed receiver adapter
    pub struct TypedReceiver<T> {
        inner: IpcReceiver<Vec<u8>>,
        _phantom: std::marker::PhantomData<T>,
    }
    impl<T: for<'de> Deserialize<'de>> TypedReceiver<T> {
        pub fn new(inner: IpcReceiver<Vec<u8>>) -> IpcReceiver<T> {
            // Safety: T must be Serialize + DeserializeOwned
            unsafe { std::mem::transmute(inner) }
        }
    }

    pub fn encode_handshake<T>() -> Vec<u8>
    where
        T: Serialize + for<'de> DeserializeOwned + Send + Sync + 'static,
    {
        trace!("Encoding handshake");
        bincode::serde::encode_to_vec(&Control::<T>::Handshake, bincode::config::standard()).unwrap()
    }

    pub fn decode_control<T>(bytes: &[u8]) -> Control<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let (ctrl, _): (Control<T>, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard()).unwrap();
        match &ctrl {
            Control::Handshake => trace!("Decoded handshake"),
            Control::Real(_) => debug!("Decoded real message"),
        }
        ctrl
    }

    pub fn encode_real<T: Serialize>(msg: T) -> Vec<u8> {
        debug!("Encoding real message");
        bincode::serde::encode_to_vec(&Control::Real(msg), bincode::config::standard()).unwrap()
    }

    pub fn run_subprocess<A, M>()
    where
        A: Default + Send + Sync + 'static,
        M: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        tracing::info!("Running subprocess actor: {}", std::any::type_name::<A>());
        let (_tx, _rx) = child::<M, M>();
        // TODO: Implement actor loop here.
        // For now, just exit.
    }
}

/// Trait for subprocess message handlers.
pub trait Handler<M, R> {
    fn handle(&mut self, msg: M) -> R;
}

/// Default actor loop for a subprocess actor. The actor type must implement:
///   fn handle(&mut self, msg: M) -> R
/// where R: Serialize.
pub fn default_actor_loop<A, M, R>()
where
    A: Default + Send + Sync + 'static + Handler<M, R>,
    M: Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    R: Serialize + Send + Sync + 'static,
{
    tracing::info!("Running subprocess actor: {}", std::any::type_name::<A>());
    let (tx, rx) = handshake::child::<M, R>();
    let mut actor = A::default();
    loop {
        match rx.recv() {
            Ok(Control::Real(msg)) => {
                let reply = actor.handle(msg);
                tx.send(Control::Real(reply)).unwrap();
            }
            Ok(Control::Handshake) => {
                tracing::trace!("Subprocess received handshake");
            }
            Err(e) => {
                tracing::info!("Subprocess channel closed: {:?}", e);
                break;
            }
        }
    }
}

/// Macro to register actors for subprocess spawning, with custom code per actor.
#[macro_export]
macro_rules! register_subprocess_actors {
    ($(($actor:ty, $msg:ty) => $body:block),* $(,)?) => {
        #[allow(dead_code)]
        /// Checks if this process is a subprocess, runs the handler if so, and exits. Returns true if handled, false otherwise.
        pub fn maybe_run_subprocess_registry_and_exit() -> bool {
            if let Ok(actor_name) = std::env::var("KAMEO_CHILD_ACTOR") {
                let span = ::tracing::span!(::tracing::Level::INFO, "process", process_role = "child");
                let _enter = span.enter();
                ::tracing::info!("[subprocess] Subprocess handler starting for actor: {}", actor_name);
                match actor_name.as_str() {
                    $(
                        stringify!($actor) => {
                            $body
                            ::tracing::info!("[subprocess] Handler for {} complete, exiting", actor_name);
                            std::process::exit(0);
                        }
                    )*
                    _ => {
                        eprintln!("[subprocess] Unknown actor: {}", actor_name);
                        std::process::exit(1);
                    }
                }
            }
            false
        }
    };
}

/// Spawns an actor in a subprocess using the registry pattern.
pub fn spawn_subprocess<A, M, R>() -> (ipc_channel::ipc::IpcSender<Control<M>>, ipc_channel::ipc::IpcReceiver<Control<R>>)
where
    A: Default + Send + Sync + 'static,
    M: Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    R: Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    use std::process::Command;
    use std::env;
    let exe = env::current_exe().unwrap();
    let actor_name = std::any::type_name::<A>().rsplit("::").next().unwrap();
    let mut cmd = Command::new(&exe);
    cmd.env("KAMEO_CHILD_ACTOR", actor_name);
    if let Ok(rust_log) = env::var("RUST_LOG") {
        cmd.env("RUST_LOG", rust_log);
    }
    let ((tx, rx), _child) = handshake::parent::<M, R>(&mut cmd);
    (tx, rx)
}
