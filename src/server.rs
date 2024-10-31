#![allow(unused_imports)]
use std::{mem::MaybeUninit, rc::Rc, sync::atomic::AtomicU64};

use anyhow::Ok;
use async_ucx::ucp::*;
use tracing::{debug, info, warn};
use glommio::io::{BufferedFile, DmaBuffer, DmaFile};

static BUFFER_SIZE: usize = 4 * 1024;
// static TRAGET_PATH: &str = "test.txt";
// static COMM_COUNT: AtomicU64 = AtomicU64::new(0);

pub async fn run_server() -> anyhow::Result<()>{
  let ctx = Context::new()?;
  let worker = ctx.create_worker()?;


  let polling_worker = worker.clone();
  glommio::spawn_local(async move {
    polling_worker.polling().await;
    warn!("polling abnormally finished");
  }).detach();

  let mut listener = worker.create_listener("0.0.0.0:10000".parse().unwrap())?;
  debug!(addr=listener.socket_addr().unwrap().to_string(), "Listening");

  let mut count = 0;
  loop {
    let conn = listener.next().await;
    conn.remote_addr()?;
    count += 1;
    debug!(count, "Connection accepted");
    let ep = worker.accept(conn).await?;
    glommio::spawn_local(handler(ep, count)).detach();
  }
}

async fn handler(ep: Endpoint, _count: u64) -> anyhow::Result<u64> {
  // let file = DmaFile::open(TRAGET_PATH).await
  //   .map_err(|e| anyhow::anyhow!(e.to_string()))?;
  // let file = Rc::new(file);
  // let mut buf = file.alloc_dma_buffer(BUFFER_SIZE);
  // // let mut recv_buf: Vec<MaybeUninit<u8>> = Vec::with_capacity(BUFFER_SIZE);
  // let mut uninit_buf_ref = unsafe {
  //   std::mem::transmute::<&mut [u8], &mut [MaybeUninit<u8>]>(buf.as_mut())
  // };
  // ep.worker().tag_recv(100, &mut uninit_buf_ref).await?;
  let mut buf: Vec<MaybeUninit<u8>> = Vec::with_capacity(BUFFER_SIZE);
  let mut count = 0;

  let now = std::time::Instant::now();
  loop {
    let recv_size = ep.worker().tag_recv(100, &mut buf).await?;
    if recv_size == 0 {
      break;
    }
    count += 1;
  }

  info!("Time elapsed: {:?}", now.elapsed());
  Ok(count)
}

// async fn write_handler(ep: Rc<Endpoint>, file: Rc<DmaFile>) -> anyhow::Result<()> {
//   let mut buf = file.alloc_dma_buffer(BUFFER_SIZE);
//   let mut uninit_buf_ref = unsafe {
//     std::mem::transmute::<&mut [u8], &mut [MaybeUninit<u8>]>(buf.as_mut())
//   };
//   Ok(())
// }