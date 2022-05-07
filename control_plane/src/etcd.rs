use std::{
    fs,
    path::PathBuf,
    process::{Command, Stdio},
};

use anyhow::Context;
use nix::{
    sys::signal::{kill, Signal},
    unistd::Pid,
};

use crate::{local_env, read_pidfile};

pub fn start_etcd_process(env: &local_env::LocalEnv) -> anyhow::Result<()> {
    let etcd_broker = &env.etcd_broker;
    println!(
        "Starting etcd broker using {}",
        etcd_broker.etcd_binary_path.display()
    );

    let etcd_data_dir = env.base_data_dir.join("etcd");
    fs::create_dir_all(&etcd_data_dir).with_context(|| {
        format!(
            "Failed to create etcd data dir: {}",
            etcd_data_dir.display()
        )
    })?;

    let etcd_stdout_file =
        fs::File::create(etcd_data_dir.join("etcd.stdout.log")).with_context(|| {
            format!(
                "Failed to create ectd stout file in directory {}",
                etcd_data_dir.display()
            )
        })?;
    let etcd_stderr_file =
        fs::File::create(etcd_data_dir.join("etcd.stderr.log")).with_context(|| {
            format!(
                "Failed to create ectd stderr file in directory {}",
                etcd_data_dir.display()
            )
        })?;
    let client_urls = etcd_broker.comma_separated_endpoints();

    let etcd_process = Command::new(&etcd_broker.etcd_binary_path)
        .args(&[
            format!("--data-dir={}", etcd_data_dir.display()),
            format!("--listen-client-urls={client_urls}"),
            format!("--advertise-client-urls={client_urls}"),
        ])
        .stdout(Stdio::from(etcd_stdout_file))
        .stderr(Stdio::from(etcd_stderr_file))
        .spawn()
        .context("Failed to spawn etcd subprocess")?;
    let pid = etcd_process.id();

    let etcd_pid_file_path = etcd_pid_file_path(env);
    fs::write(&etcd_pid_file_path, pid.to_string()).with_context(|| {
        format!(
            "Failed to create etcd pid file at {}",
            etcd_pid_file_path.display()
        )
    })?;

    Ok(())
}

pub fn stop_etcd_process(env: &local_env::LocalEnv) -> anyhow::Result<()> {
    let etcd_path = &env.etcd_broker.etcd_binary_path;
    println!("Stopping etcd broker at {}", etcd_path.display());

    let etcd_pid_file_path = etcd_pid_file_path(env);
    let pid = Pid::from_raw(read_pidfile(&etcd_pid_file_path).with_context(|| {
        format!(
            "Failed to read etcd pid filea at {}",
            etcd_pid_file_path.display()
        )
    })?);

    kill(pid, Signal::SIGTERM).with_context(|| {
        format!(
            "Failed to stop etcd with pid {pid} at {}",
            etcd_pid_file_path.display()
        )
    })?;

    Ok(())
}

fn etcd_pid_file_path(env: &local_env::LocalEnv) -> PathBuf {
    env.base_data_dir.join("etcd.pid")
}
