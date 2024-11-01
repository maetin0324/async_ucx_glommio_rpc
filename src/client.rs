use std::vec;

use anyhow::Ok;
use async_ucx::ucp::*;
use tracing::debug;
static BUFFER_SIZE: usize = 4 * 1024;
static OFFSET: usize = 8;
static TOTAL_SIZE: usize = BUFFER_SIZE + OFFSET;
static SEND_SIZE: usize = 1 * 1024 * 1024 * 1024;

pub async fn run_client(addr: &str) -> anyhow::Result<()> {
  let ctx = Context::new()?;
  let worker = ctx.create_worker()?;
  let polling_worker = worker.clone();

  glommio::spawn_local(async move {
    polling_worker.polling().await;
  }).detach();

  let ep = worker.connect_socket(addr.parse().unwrap()).await?;
  let buf: Vec<u8> = vec![0; TOTAL_SIZE];
  let mut count: u64 = 0;

  debug!("start send");
  loop {
    ep.tag_send(100, &buf).await?;
    count += 1;
  }
  // ep.tag_send(100, &[0]).await?;
  debug!("send finished, count: {}", count);

  Ok(())
}