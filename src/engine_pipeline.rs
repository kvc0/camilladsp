// CamillaDSP - A flexible tool for processing audio
// Copyright (C) 2026 Henrik Enquist
//
// This file is part of CamillaDSP.
//
// CamillaDSP is free software; you can redistribute it and/or modify it
// under the terms of either:
//
// a) the GNU General Public License version 3,
//    or
// b) the Mozilla Public License Version 2.0.
//
// You should have received copies of the GNU General Public License and the
// Mozilla Public License along with this program. If not, see
// <https://www.gnu.org/licenses/> and <https://www.mozilla.org/MPL/2.0/>.

use std::{
    sync::{Arc, Barrier},
    thread,
};

use crate::{
    CommandMessage, ProcessingState, StatusMessage, StatusStructs, audiodevice, config, processing,
};

/// Supervisory handles for the running capture/processing/playback threads of
/// one device group.
pub struct EnginePipeline {
    /// The device group this pipeline drives.
    device_group: usize,
    /// commands (set speed, exit) to the capture thread.
    tx_command_cap: crossbeam_channel::Sender<CommandMessage>,
    /// config updates to the processing thread.
    tx_pipeconf: crossbeam_channel::Sender<(config::ConfigChange, config::Configuration)>,
    /// status messages from this group's capture and playback threads.
    rx_status: crossbeam_channel::Receiver<StatusMessage>,
    /// 4-way startup barrier (capture, playback, processing, supervisor).
    barrier: Arc<Barrier>,
    pb_handle: Box<thread::JoinHandle<()>>,
    cap_handle: Box<thread::JoinHandle<()>>,
    /// Status structs for this group. Group 0 shares the process-lifetime structs
    /// held by the WebSocket server; others share only the global run status.
    status: StatusStructs,
    pb_ready: bool,
    cap_ready: bool,
    /// Whether the supervisor has already met this group's startup barrier.
    barrier_released: bool,
}

impl EnginePipeline {
    /// The device group this pipeline drives.
    pub fn device_group(&self) -> usize {
        self.device_group
    }

    /// Status structs for this group (its own capture/playback/processing state).
    pub fn status(&self) -> &StatusStructs {
        &self.status
    }

    /// Status channel from this group's capture and playback threads. Used by the
    /// supervisor to fan-in over all groups.
    pub fn status_channel(&self) -> &crossbeam_channel::Receiver<StatusMessage> {
        &self.rx_status
    }

    /// Whether this group has already met its startup barrier.
    pub fn barrier_released(&self) -> bool {
        self.barrier_released
    }

    /// When both capture and playback are ready, meet this group's startup
    /// barrier so its threads begin processing. Returns `true` once this group is
    /// past the barrier (whether it was released just now or already).
    pub fn release_barrier_if_ready(&mut self) -> bool {
        if self.barrier_released {
            return true;
        }
        if self.pb_ready && self.cap_ready {
            debug!(
                "Device group {} ready, releasing startup barrier",
                self.device_group
            );
            self.barrier.wait();
            self.barrier_released = true;
            return true;
        }
        false
    }

    /// Tell the capture thread to exit, release the startup barrier if it has not
    /// been met yet (so the device/processing threads unblock), then join the
    /// capture and playback threads.
    pub fn stop(self) {
        if self.tx_command_cap.send(CommandMessage::Exit).is_err() {
            debug!("Capture thread has already exited");
        }
        if !self.barrier_released {
            debug!("Stopping before startup barrier was met, releasing it");
            self.barrier.wait();
        }
        trace!("Wait for playback thread to exit..");
        self.pb_handle.join().unwrap();
        trace!("Wait for capture thread to exit..");
        self.cap_handle.join().unwrap();
    }

    /// Send a config update to the processing thread
    pub fn update_processing_config(
        &self,
        change: config::ConfigChange,
        configuration: config::Configuration,
    ) {
        self.tx_pipeconf.send((change, configuration)).unwrap();
    }

    /// Set playback readiness state
    pub fn set_playback_ready(&mut self) {
        self.pb_ready = true;
    }

    /// Set capture readiness state
    pub fn set_capture_ready(&mut self) {
        self.cap_ready = true;
    }

    pub fn send_capture_command(&self, command: CommandMessage) {
        if self.tx_command_cap.send(command).is_err() {
            debug!("Capture thread has already exited");
        }
    }
}

/// Open the devices and spawn the capture, processing, and playback threads for
/// one `device_group` of `active_config`. The returned [`EnginePipeline`] takes
/// ownership of `status_structs` and carries the channel on which the device
/// threads report their status.
pub fn start_pipeline(
    active_config: &config::Configuration,
    device_group: usize,
    status_structs: StatusStructs,
) -> EnginePipeline {
    let devices = active_config.devices.group(device_group).clone();

    let (tx_pb, rx_pb) = crossbeam_channel::bounded(devices.queuelimit());
    let (tx_cap, rx_cap) = crossbeam_channel::bounded(devices.queuelimit());
    let (tx_status, rx_status) = crossbeam_channel::unbounded();
    let (tx_command_cap, rx_command_cap) = crossbeam_channel::unbounded();
    let (tx_pipeconf, rx_pipeconf) = crossbeam_channel::unbounded();
    let barrier = Arc::new(Barrier::new(4));

    // Processing thread
    processing::run_processing(
        active_config.clone(),
        device_group,
        barrier.clone(),
        tx_pb,
        rx_cap,
        rx_pipeconf,
        status_structs.processing.clone(),
    );

    // Playback thread
    let mut playback_dev = audiodevice::new_playback_device(devices.clone());
    let pb_handle = playback_dev
        .start(
            rx_pb,
            barrier.clone(),
            tx_status.clone(),
            status_structs.playback.clone(),
        )
        .unwrap();

    let used_channels = config::used_capture_channels(active_config, device_group);
    debug!("Device group {device_group} using channels {used_channels:?}");
    {
        let mut capture_status = status_structs.capture.write();
        crate::update_capture_state(&mut capture_status, ProcessingState::Starting);
        capture_status.used_channels = used_channels;
    }

    // Capture thread
    let mut capture_dev = audiodevice::new_capture_device(devices);
    let cap_handle = capture_dev
        .start(
            tx_cap,
            barrier.clone(),
            tx_status,
            rx_command_cap,
            status_structs.capture.clone(),
            status_structs.processing.clone(),
        )
        .unwrap();

    EnginePipeline {
        device_group,
        tx_command_cap,
        tx_pipeconf,
        rx_status,
        barrier,
        pb_handle,
        cap_handle,
        status: status_structs,
        pb_ready: false,
        cap_ready: false,
        barrier_released: false,
    }
}
