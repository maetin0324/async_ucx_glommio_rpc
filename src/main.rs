use async_ucx_glommio_rpc::server::run_server;
use async_ucx_glommio_rpc::client::run_client;
use glommio::{LocalExecutorBuilder, Placement};
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn main() {
  tracing_subscriber::registry()
    .with(
      tracing_subscriber::EnvFilter::try_from_default_env()
      .unwrap_or_else(|_| "Debug".into())
    )
    .with(tracing_subscriber::fmt::Layer::default().with_ansi(true))
    .init();

    let ex = LocalExecutorBuilder::new(Placement::Fixed(0)).make().unwrap();

  if let Some(addr) = std::env::args().nth(1) {
    info!("client mode");
    if let Err(e) = ex.run(run_client(&addr)){
      eprintln!("client error: {:?}", e);
    }
  } else {
    info!("server mode");
    if let Err(e) = ex.run(run_server()){
      eprintln!("server error: {:?}", e);
    }
  }
}
