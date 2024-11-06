#![allow(unused_imports)]
use std::{cell::RefCell, mem::MaybeUninit, rc::Rc, sync::{atomic::AtomicU64, Arc}};

use anyhow::Ok;
use async_ucx::ucp::*;
use futures::{stream::FuturesUnordered, StreamExt};
use tracing::{debug, info, warn};
use glommio::{io::{BufferedFile, DmaBuffer, DmaFile, OpenOptions}, task::JoinHandle};
use tracing_subscriber::field::debug;

static BUFFER_SIZE: usize = 1024 * 1024;
static OFFSET: usize = 8;
static TOTAL_SIZE: usize = BUFFER_SIZE + OFFSET;
static SEND_SIZE: usize = 128 * 1024 * 1024 * 1024;
static TRAGET_PATH: &str = "/scr/rmaeda/tmp/test.txt";
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
    debug!("Connection established");
    // glommio::spawn_local(async move { 
    //   if let Err(e) = handler(ep, count).await {
    //     warn!("handler error: {:?}", e);
    //   }
    // }).detach();
    glommio::spawn_local(async move { 
      if let Err(e) = write_handler(ep, count).await {
        warn!("handler error: {:?}", e);
      }
    }).detach();
  }
}

async fn handler(ep: Endpoint, _count: u64) -> anyhow::Result<usize> {
  // let file = DmaFile::open(TRAGET_PATH).await
  //   .map_err(|e| anyhow::anyhow!(e.to_string()))?;
  // let file = Rc::new(file);
  // let mut buf = file.alloc_dma_buffer(BUFFER_SIZE);
  // // let mut recv_buf: Vec<MaybeUninit<u8>> = Vec::with_capacity(BUFFER_SIZE);
  // let mut uninit_buf_ref = unsafe {
  //   std::mem::transmute::<&mut [u8], &mut [MaybeUninit<u8>]>(buf.as_mut())
  // };
  // ep.worker().tag_recv(100, &mut uninit_buf_ref).await?;
  let mut buf: Vec<MaybeUninit<u8>> = Vec::with_capacity(TOTAL_SIZE);
  buf.resize(TOTAL_SIZE, MaybeUninit::uninit());
  let mut count = 0;

  debug!("start recv");
  // let recv_size = ep.worker().tag_recv(100, &mut buf).await?;
  // debug!("recv_size: {:?}", recv_size);
  debug!("ep state: {:?}", ep.print_to_stderr());

  let mut now = std::time::Instant::now();
  loop {
    let now2 = std::time::Instant::now();
    let recv_size = ep.worker().tag_recv(100, &mut buf).await?;
    if count == 1 {
      debug!("now2.elapsed(): {:?}", now2.elapsed());
    }
    count += 1;
    if count % (SEND_SIZE / BUFFER_SIZE) == 0 {
      debug!("count: {:?}, time elapsed: {:?}", count, now.elapsed());
      now = std::time::Instant::now();
      break;
    }
  }
  info!("Time elapsed: {:?}, count: {:?}", now.elapsed(), count);
  Ok(count)
}

async fn write_handler(ep: Endpoint, _: u64) -> anyhow::Result<()> {
  let file = DmaFile::create(TRAGET_PATH)
    .await
    .map_err(|e| anyhow::anyhow!(e.to_string()))?;

  let file = Arc::new(file);
  debug!("File opend in write handler");

  let mut recv_buf: Vec<MaybeUninit<u8>> = Vec::with_capacity(TOTAL_SIZE);
  recv_buf.resize(TOTAL_SIZE, MaybeUninit::uninit());
  let mut count = 0;

  // let mut uninit_buf_ref = unsafe {
  //   std::mem::transmute::<&mut [u8], &mut [MaybeUninit<u8>]>(buf.as_mut())
  // };

  // let tasks: RefCell<Vec<JoinHandle<()>>> = RefCell::new(Vec::new());
  let mut tasks = FuturesUnordered::new();

  let mut now = std::time::Instant::now();
  loop {
    ep.worker().tag_recv(100, &mut recv_buf).await?;
    let file_buf = file.alloc_dma_buffer(BUFFER_SIZE);
    let (offset_buf, data)  = recv_buf.split_at(OFFSET);

    let offset_buf = unsafe {
      std::mem::transmute::<&[MaybeUninit<u8>], &[u8]>(&offset_buf)
    };
    let offset = u64::from_le_bytes(offset_buf.try_into().unwrap());

    let mut data = unsafe {
      std::mem::transmute::<&[MaybeUninit<u8>], &[u8]>(&data)
    };

    std::mem::swap(&mut data, &mut file_buf.as_ref());

    let file_clone = file.clone();

    let task = glommio::spawn_local(async move {
      if let Err(e) = write_task(file_clone, file_buf, offset).await {
        warn!("write_task error: {:?}", e);
      };
    }).detach();

    tasks.push(task);
    count += 1;

    if count % 8192 == 0 {
      // futures::future::join_all(tasks.borrow_mut().drain(..)).await;
      while let Some(_) = tasks.next().await {}
    }

    if count % (SEND_SIZE / BUFFER_SIZE) == 0 {
      debug!("count: {:?}, time elapsed: {:?}", count, now.elapsed());
      now = std::time::Instant::now();
    }
  }
  // Ok(())
}

async fn write_task(file: Arc<DmaFile>, buf: DmaBuffer, pos: u64) -> anyhow::Result<()> {
  file.write_at(buf, pos).await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
  // file.write_at(buf, pos).await.unwrap();
  Ok(())
}
