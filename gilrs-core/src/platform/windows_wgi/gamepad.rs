// Copyright 2016-2018 Mateusz Sieczko and other GilRs Developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use super::FfDevice;
use crate::event_queue::{
    bounded, EnqueueResult, EventReceiver, EventSender, QueueItem, DEFAULT_EVENT_QUEUE_CAPACITY,
};
use crate::native_ev_codes as nec;
use crate::{
    utils, AxisInfo, Event, EventType, PlatformError, PowerInfo, ResetError, ShutdownError,
};

#[cfg(feature = "serde-serialize")]
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::hash::{Hash, Hasher};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};
use uuid::Uuid;
use windows::core::HSTRING;
use windows::Devices::Power::BatteryReport;
use windows::Foundation::EventHandler;
use windows::Gaming::Input::{
    GameControllerSwitchPosition, Gamepad as WgiGamepad, GamepadButtons, GamepadReading,
    RawGameController,
};
use windows::System::Power::BatteryStatus;

const SDL_HARDWARE_BUS_USB: u32 = 0x03;
// const SDL_HARDWARE_BUS_BLUETOOTH: u32 = 0x05;

// The general consensus is that standard xbox controllers poll at ~125 hz which
// means 8 ms between updates.
// Seems like a good target for how often we update the background thread.
const EVENT_THREAD_SLEEP_TIME: u64 = 8;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerExit {
    Stopped,
    Panicked,
    TimedOut,
    StopSignalFailed,
}

#[derive(Debug)]
enum Control {
    Stop,
    Reset(SyncSender<Result<(), ResetError>>),
}

const WGI_TO_GILRS_BUTTON_MAP: [(GamepadButtons, crate::EvCode); 14] = [
    (GamepadButtons::DPadUp, nec::BTN_DPAD_UP),
    (GamepadButtons::DPadDown, nec::BTN_DPAD_DOWN),
    (GamepadButtons::DPadLeft, nec::BTN_DPAD_LEFT),
    (GamepadButtons::DPadRight, nec::BTN_DPAD_RIGHT),
    (GamepadButtons::Menu, nec::BTN_START),
    (GamepadButtons::View, nec::BTN_SELECT),
    (GamepadButtons::LeftThumbstick, nec::BTN_LTHUMB),
    (GamepadButtons::RightThumbstick, nec::BTN_RTHUMB),
    (GamepadButtons::LeftShoulder, nec::BTN_LT),
    (GamepadButtons::RightShoulder, nec::BTN_RT),
    (GamepadButtons::A, nec::BTN_SOUTH),
    (GamepadButtons::B, nec::BTN_EAST),
    (GamepadButtons::X, nec::BTN_WEST),
    (GamepadButtons::Y, nec::BTN_NORTH),
];

/// This is similar to `gilrs_core::Event` but has a raw_game_controller that still needs to be
/// converted to a gilrs gamepad id.
#[derive(Debug)]
struct WgiEvent {
    raw_game_controller: RawGameController,
    event: EventType,
    pub time: SystemTime,
}

impl WgiEvent {
    fn new(raw_game_controller: RawGameController, event: EventType) -> Self {
        let time = utils::time_now();
        WgiEvent {
            raw_game_controller,
            event,
            time,
        }
    }
}

struct WorkerEventHandler {
    #[allow(dead_code)]
    inner: EventHandler<RawGameController>,
}

// SAFETY: `EventHandler<T>` exposes `IAgileObject` for its COM vtable and its
// callback closure is required to be `Send`. The handler is therefore safe to
// release from the worker thread; the registration token keeps the callback
// alive until it is removed.
unsafe impl Send for WorkerEventHandler {}

fn send_wgi_event(tx: &EventSender<WgiEvent>, epoch: u64, event: WgiEvent) {
    let result = tx.push_with_epoch(epoch, event);
    if matches!(result, EnqueueResult::Overflowed | EnqueueResult::Closed) {
        warn!("WGI event queue rejected an event: {result:?}");
    }
}

fn controller_axis_key(controller: &RawGameController, code: u32) -> Option<u64> {
    let id = controller.NonRoamableId().ok()?.to_string_lossy();
    let mut hasher = DefaultHasher::new();
    id.hash(&mut hasher);
    code.hash(&mut hasher);
    Some(hasher.finish())
}

fn send_wgi_axis_event(
    tx: &EventSender<WgiEvent>,
    epoch: u64,
    controller: &RawGameController,
    code: u32,
    event: WgiEvent,
) {
    let result = match controller_axis_key(controller, code) {
        Some(key) => tx.push_latest_with_epoch(epoch, key, event),
        None => tx.push_with_epoch(epoch, event),
    };
    if matches!(result, EnqueueResult::Overflowed | EnqueueResult::Closed) {
        warn!("WGI axis queue rejected an event: {result:?}");
    }
}

#[derive(Debug)]
pub struct Gilrs {
    gamepads: Vec<Gamepad>,
    rx: EventReceiver<WgiEvent>,
    join_handle: Option<JoinHandle<()>>,
    control_tx: Option<SyncSender<Control>>,
    completion: Option<Receiver<WorkerExit>>,
}

impl Gilrs {
    pub(crate) fn new() -> Result<Self, PlatformError> {
        let raw_game_controllers = RawGameController::RawGameControllers()
            .map_err(|e| PlatformError::Other(Box::new(e)))?;
        let count = raw_game_controllers
            .Size()
            .map_err(|e| PlatformError::Other(Box::new(e)))?;
        // Intentionally avoiding using RawGameControllers.into_iter() as it triggers a crash when
        // the app is run through steam.
        // https://gitlab.com/gilrs-project/gilrs/-/issues/132
        let gamepads = (0..count)
            .map(|i| {
                let controller = raw_game_controllers
                    .GetAt(i)
                    .map_err(|e| PlatformError::Other(Box::new(e)))?;
                Gamepad::new(i, controller)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let (tx, rx) = bounded(DEFAULT_EVENT_QUEUE_CAPACITY);
        let (control_tx, control_rx) = mpsc::sync_channel(1);
        let (join_handle, completion) = Self::spawn_thread(tx, control_rx)?;
        Ok(Gilrs {
            gamepads,
            rx,
            join_handle: Some(join_handle),
            control_tx: Some(control_tx),
            completion: Some(completion),
        })
    }

    fn spawn_thread(
        tx: EventSender<WgiEvent>,
        control_rx: Receiver<Control>,
    ) -> Result<(JoinHandle<()>, Receiver<WorkerExit>), PlatformError> {
        let added_tx = tx.clone();
        let added_handler = EventHandler::<RawGameController>::new(move |_, g| {
            if let Some(g) = g.as_ref() {
                let epoch = added_tx.epoch();
                send_wgi_event(
                    &added_tx,
                    epoch,
                    WgiEvent::new(g.clone(), EventType::Connected),
                );
            }
            Ok(())
        });
        let controller_added_token = RawGameController::RawGameControllerAdded(&added_handler)
            .map_err(|e| PlatformError::Other(Box::new(e)))?;

        let removed_tx = tx.clone();
        let removed_handler = EventHandler::<RawGameController>::new(move |_, g| {
            if let Some(g) = g.as_ref() {
                let epoch = removed_tx.epoch();
                send_wgi_event(
                    &removed_tx,
                    epoch,
                    WgiEvent::new(g.clone(), EventType::Disconnected),
                );
            }
            Ok(())
        });
        let controller_removed_token =
            match RawGameController::RawGameControllerRemoved(&removed_handler) {
                Ok(token) => token,
                Err(error) => {
                    if let Err(remove_error) =
                        RawGameController::RemoveRawGameControllerAdded(controller_added_token)
                    {
                        error!(
                            "Failed to remove RawGameControllerAdded event handler after \
                             registration failure: {remove_error}"
                        );
                    }
                    return Err(PlatformError::Other(Box::new(error)));
                }
            };

        let (completion_tx, completion) = mpsc::sync_channel(1);
        let added_handler = WorkerEventHandler {
            inner: added_handler,
        };
        let removed_handler = WorkerEventHandler {
            inner: removed_handler,
        };
        let join_handle =
            match std::thread::Builder::new()
                .name("gilrs".to_owned())
                .spawn(move || {
                    // Keep both handlers alive until the matching WinRT registrations are removed.
                    let _added_handler = added_handler;
                    let _removed_handler = removed_handler;

                    let result =
                        catch_unwind(AssertUnwindSafe(|| Self::run_worker(tx, control_rx)));
                    let exit = match result {
                        Ok(()) => WorkerExit::Stopped,
                        Err(_) => {
                            error!("WGI worker panicked");
                            WorkerExit::Panicked
                        }
                    };

                    if let Err(error) =
                        RawGameController::RemoveRawGameControllerAdded(controller_added_token)
                    {
                        error!("Failed to remove RawGameControllerAdded event handler: {error}");
                    }

                    if let Err(error) =
                        RawGameController::RemoveRawGameControllerRemoved(controller_removed_token)
                    {
                        error!("Failed to remove RawGameControllerRemoved event handler: {error}");
                    }

                    let _ = completion_tx.send(exit);
                }) {
                Ok(handle) => handle,
                Err(error) => {
                    if let Err(remove_error) =
                        RawGameController::RemoveRawGameControllerAdded(controller_added_token)
                    {
                        error!(
                            "Failed to remove RawGameControllerAdded event handler after spawn \
                         failure: {remove_error}"
                        );
                    }
                    if let Err(remove_error) =
                        RawGameController::RemoveRawGameControllerRemoved(controller_removed_token)
                    {
                        error!(
                            "Failed to remove RawGameControllerRemoved event handler after spawn \
                         failure: {remove_error}"
                        );
                    }
                    return Err(PlatformError::Other(Box::new(error)));
                }
            };

        Ok((join_handle, completion))
    }

    fn run_worker(tx: EventSender<WgiEvent>, control_rx: Receiver<Control>) {
        let mut controllers: Vec<RawGameController> = Vec::new();
        // To avoid allocating every update, store old and new readings for every controller
        // and swap their memory
        let mut readings: Vec<(HSTRING, Reading, Reading)> = Vec::new();
        let mut last_failed_get_id: Option<Instant> = None;
        loop {
            match control_rx.try_recv() {
                Ok(Control::Stop) | Err(TryRecvError::Disconnected) => break,
                Ok(Control::Reset(ack)) => {
                    readings.clear();
                    let _ = ack.send(Ok(()));
                }
                Err(TryRecvError::Empty) => {}
            }
            let epoch = tx.epoch();
            controllers.clear();
            // Avoiding using RawGameControllers().into_iter() here due to it causing an
            // unhandled exception when the app is running through steam.
            // https://gitlab.com/gilrs-project/gilrs/-/issues/132
            if let Ok(raw_game_controllers) = RawGameController::RawGameControllers() {
                let count = raw_game_controllers.Size().unwrap_or_default();
                for index in 0..count {
                    if let Ok(controller) = raw_game_controllers.GetAt(index) {
                        controllers.push(controller);
                    }
                }
            }

            for controller in controllers.iter() {
                let id: HSTRING = match controller.NonRoamableId() {
                    Ok(id) => id,
                    Err(e) => {
                        if last_failed_get_id.map_or(true, |x| x.elapsed().as_secs() > 59) {
                            error!(
                                "Failed to get gamepad id: {e}! Skipping reading events \
                                 for this gamepad."
                            );
                            last_failed_get_id = Some(Instant::now());
                        }

                        continue;
                    }
                };
                // Find readings for this controller or insert new ones.
                let index = match readings.iter().position(|(other_id, ..)| id == *other_id) {
                    None => {
                        let reading = match reading_kind(controller) {
                            ReadingKind::Gamepad => {
                                match WgiGamepad::FromGameController(controller) {
                                    Ok(wgi_gamepad) => {
                                        wgi_gamepad.GetCurrentReading().map(Reading::Gamepad)
                                    }
                                    Err(_) => RawGamepadReading::new(controller).map(Reading::Raw),
                                }
                            }
                            ReadingKind::Raw => {
                                RawGamepadReading::new(controller).map(Reading::Raw)
                            }
                        };
                        let reading = match reading {
                            Ok(reading) => reading,
                            Err(error) => {
                                error!("Failed to read initial WGI controller state: {error}");
                                continue;
                            }
                        };

                        let zero = reading.zero_like();
                        Reading::send_events_for_differences(
                            &zero, &reading, controller, &tx, epoch, true,
                        );
                        readings.push((id, reading.clone(), reading));
                        readings.len() - 1
                    }
                    Some(i) => i,
                };

                let (_, old_reading, new_reading) = &mut readings[index];

                // Make last update's reading the old reading and get a new one.
                std::mem::swap(old_reading, new_reading);
                if let Err(e) = new_reading.update(controller) {
                    if e.code().is_err() {
                        error!("Reading::update() function failed with {e}");
                    }
                    continue;
                }

                // Skip if this is the same reading as the last one.
                if old_reading.time() == new_reading.time() {
                    continue;
                }

                Reading::send_events_for_differences(
                    old_reading,
                    new_reading,
                    controller,
                    &tx,
                    epoch,
                    false,
                );
            }
            thread::sleep(Duration::from_millis(EVENT_THREAD_SLEEP_TIME));
        }
    }

    pub(crate) fn next_event(&mut self) -> Option<Event> {
        self.rx
            .try_pop()
            .and_then(|item| self.handle_queue_item(item))
    }

    pub(crate) fn next_event_blocking(&mut self, timeout: Option<Duration>) -> Option<Event> {
        self.rx
            .pop_timeout(timeout)
            .and_then(|item| self.handle_queue_item(item))
    }

    fn handle_queue_item(&mut self, item: QueueItem<WgiEvent>) -> Option<Event> {
        match item {
            QueueItem::Event(wgi_event) => self.handle_event(wgi_event),
            QueueItem::Overflow { dropped } => {
                if let Err(error) = self.reset() {
                    warn!("WGI recovery after queue overflow failed: {error:?}");
                }
                Some(Event::new(0, EventType::Overflow { dropped }))
            }
        }
    }

    pub(crate) fn reset(&mut self) -> Result<(), ResetError> {
        self.rx.advance_epoch();
        let Some(control_tx) = self.control_tx.as_ref() else {
            return Err(ResetError::BackendUnavailable);
        };
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        control_tx
            .send(Control::Reset(ack_tx))
            .map_err(|_| ResetError::BackendUnavailable)?;
        match ack_rx.recv_timeout(SHUTDOWN_TIMEOUT) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => Err(ResetError::TimedOut),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(ResetError::WorkerFailed),
        }
    }

    pub(crate) fn shutdown(&mut self) -> Result<(), ShutdownError> {
        self.stop_and_join().map_err(|error| match error {
            WorkerExit::Stopped => ShutdownError::WorkerFailed,
            WorkerExit::Panicked => ShutdownError::WorkerPanicked,
            WorkerExit::TimedOut => ShutdownError::TimedOut,
            WorkerExit::StopSignalFailed => ShutdownError::StopSignalFailed,
        })
    }

    fn handle_event(&mut self, wgi_event: WgiEvent) -> Option<Event> {
        // Find the index of the gamepad in our vec or insert it
        let id = match self.gamepads.iter().position(|gamepad| {
            match wgi_event.raw_game_controller.NonRoamableId() {
                Ok(id) => id == gamepad.non_roamable_id,
                _ => false,
            }
        }) {
            Some(id) => id,
            None => match Gamepad::new(self.gamepads.len() as u32, wgi_event.raw_game_controller) {
                Ok(gamepad) => {
                    self.gamepads.push(gamepad);
                    self.gamepads.len() - 1
                }
                Err(_) => {
                    return None;
                }
            },
        };

        match wgi_event.event {
            EventType::Connected => self.gamepads[id].is_connected = true,
            EventType::Disconnected => self.gamepads[id].is_connected = false,
            _ => (),
        }
        Some(Event {
            id,
            event: wgi_event.event,
            time: wgi_event.time,
        })
    }

    pub fn gamepad(&self, id: usize) -> Option<&Gamepad> {
        self.gamepads.get(id)
    }

    pub fn last_gamepad_hint(&self) -> usize {
        self.gamepads.len()
    }
}

impl Gilrs {
    fn stop_and_join(&mut self) -> Result<(), WorkerExit> {
        let Some(join_handle) = self.join_handle.take() else {
            return Ok(());
        };

        let stop_result = self
            .control_tx
            .take()
            .map(|control_tx| control_tx.send(Control::Stop))
            .unwrap_or(Ok(()));
        let completion = self
            .completion
            .take()
            .map(|completion| completion.recv_timeout(SHUTDOWN_TIMEOUT));

        let result = match completion {
            Some(Ok(WorkerExit::Stopped)) => Ok(()),
            Some(Ok(WorkerExit::Panicked)) => Err(WorkerExit::Panicked),
            Some(Ok(WorkerExit::TimedOut | WorkerExit::StopSignalFailed)) => {
                Err(WorkerExit::StopSignalFailed)
            }
            Some(Err(mpsc::RecvTimeoutError::Timeout)) => Err(WorkerExit::TimedOut),
            Some(Err(mpsc::RecvTimeoutError::Disconnected)) => Err(WorkerExit::StopSignalFailed),
            None => Err(WorkerExit::StopSignalFailed),
        };

        if stop_result.is_err()
            && !matches!(result, Ok(()))
            && !matches!(result, Err(WorkerExit::Panicked))
        {
            return Err(WorkerExit::StopSignalFailed);
        }

        if matches!(result, Ok(()) | Err(WorkerExit::Panicked)) {
            let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
            while !join_handle.is_finished() {
                if Instant::now() >= deadline {
                    return Err(WorkerExit::TimedOut);
                }
                thread::sleep(Duration::from_millis(1));
            }
            return join_handle.join().map_err(|_| WorkerExit::Panicked);
        }

        Err(result.err().unwrap_or(WorkerExit::StopSignalFailed))
    }
}

impl Drop for Gilrs {
    fn drop(&mut self) {
        let dropped = self.rx.dropped_count();
        if dropped > 0 {
            warn!("WGI event queue dropped {dropped} events");
        }
        if self.join_handle.is_some() {
            if let Err(error) = self.stop_and_join() {
                warn!("WGI worker shutdown was not clean: {error:?}");
            }
        }
    }
}

#[derive(Debug, Clone)]
struct RawGamepadReading {
    axes: Vec<f64>,
    buttons: Vec<bool>,
    switches: Vec<GameControllerSwitchPosition>,
    time: u64,
}

impl RawGamepadReading {
    fn new(raw_game_controller: &RawGameController) -> windows::core::Result<Self> {
        let axis_count = raw_game_controller.AxisCount()? as usize;
        let button_count = raw_game_controller.ButtonCount()? as usize;
        let switch_count = raw_game_controller.SwitchCount()? as usize;
        let mut new = Self {
            axes: vec![0.0; axis_count],
            buttons: vec![false; button_count],
            switches: vec![GameControllerSwitchPosition::default(); switch_count],
            time: 0,
        };
        new.time = raw_game_controller.GetCurrentReading(
            &mut new.buttons,
            &mut new.switches,
            &mut new.axes,
        )?;
        Ok(new)
    }

    fn update(&mut self, raw_game_controller: &RawGameController) -> windows::core::Result<()> {
        self.time = raw_game_controller.GetCurrentReading(
            &mut self.buttons,
            &mut self.switches,
            &mut self.axes,
        )?;
        Ok(())
    }
}

/// Scales a Windows Gaming Input raw analog sample into the `i32` domain that
/// gilrs renormalizes.
///
/// `Gamepad::axis_info` reports `EvCodeKind::Axis` as the full signed `i32`
/// range, which is what `axis_value` maps onto `[-1.0, 1.0]`. The raw sample
/// therefore has to be scaled over that same range, so a neutral device reading
/// lands on `0` and both stick directions reach the ends of the range.
///
/// SDL's Windows Gaming Input backend instead pre-centers the sample with
/// `(value * 65535.0) - 32768.0`, which places a neutral reading on `i16::MIN`.
/// SDL hands that straight to `SDL_PrivateJoystickAxis` and its own axis
/// normalization removes the offset later, but gilrs renormalizes a second time
/// and kept the offset: every neutral stick read as `-1.0` and the entire
/// negative half of its travel clamped to full deflection.
fn raw_axis_value(device: f64) -> i32 {
    (device * i32::MAX as f64) as i32
}

/// Which Windows Gaming Input reading a controller is polled through.
///
/// `Windows.Gaming.Input.Gamepad` is the mapped reading, and Windows only serves
/// it to the process that owns the foreground window. A controller that can only
/// be read through it therefore goes silent for as long as the application runs
/// somewhere else, which for a tray application or a game overlay is the normal
/// case.
///
/// `RawGameController` is served regardless of focus, and it is the reading SDL's
/// own Windows Gaming Input backend uses for every controller, XInput ones
/// included. Prefer it whenever the device exposes a raw report at all, and keep
/// the mapped reading only as the fallback for a device that exposes none.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ReadingKind {
    Gamepad,
    Raw,
}

fn reading_kind(controller: &RawGameController) -> ReadingKind {
    reading_kind_for(
        controller.ButtonCount().unwrap_or_default(),
        controller.AxisCount().unwrap_or_default(),
        controller.SwitchCount().unwrap_or_default(),
    )
}

fn reading_kind_for(buttons: i32, axes: i32, switches: i32) -> ReadingKind {
    if buttons > 0 || axes > 0 || switches > 0 {
        ReadingKind::Raw
    } else {
        ReadingKind::Gamepad
    }
}

/// Treats switches like a two axes similar to a Directional pad.
/// Returns a tuple containing the values of the x and y axis.
/// Value's range is -1 to 1.
fn direction_from_switch(switch: GameControllerSwitchPosition) -> (i32, i32) {
    match switch {
        GameControllerSwitchPosition::Up => (0, 1),
        GameControllerSwitchPosition::Down => (0, -1),
        GameControllerSwitchPosition::Right => (1, 0),
        GameControllerSwitchPosition::Left => (-1, 0),
        GameControllerSwitchPosition::UpLeft => (-1, 1),
        GameControllerSwitchPosition::UpRight => (1, 1),
        GameControllerSwitchPosition::DownLeft => (-1, -1),
        GameControllerSwitchPosition::DownRight => (1, -1),
        _ => (0, 0),
    }
}

#[derive(Clone)]
enum Reading {
    Raw(RawGamepadReading),
    Gamepad(GamepadReading),
}

impl Reading {
    fn time(&self) -> u64 {
        match self {
            Reading::Raw(r) => r.time,
            Reading::Gamepad(r) => r.Timestamp,
        }
    }

    fn update(&mut self, controller: &RawGameController) -> windows::core::Result<()> {
        match self {
            Reading::Raw(raw_reading) => {
                raw_reading.update(controller)?;
            }
            Reading::Gamepad(gamepad_reading) => {
                let gamepad: WgiGamepad = WgiGamepad::FromGameController(controller)?;
                *gamepad_reading = gamepad.GetCurrentReading()?;
            }
        }
        Ok(())
    }

    fn zero_like(&self) -> Self {
        match self {
            Reading::Raw(raw) => Reading::Raw(RawGamepadReading {
                axes: vec![0.0; raw.axes.len()],
                buttons: vec![false; raw.buttons.len()],
                switches: vec![GameControllerSwitchPosition::default(); raw.switches.len()],
                time: 0,
            }),
            Reading::Gamepad(_) => Reading::Gamepad(GamepadReading::default()),
        }
    }

    fn send_events_for_differences(
        old: &Self,
        new: &Self,
        controller: &RawGameController,
        tx: &EventSender<WgiEvent>,
        epoch: u64,
        force: bool,
    ) {
        match (old, new) {
            // WGI RawGameController
            (Reading::Raw(old), Reading::Raw(new)) => {
                // Axis changes
                for index in 0..new.axes.len() {
                    if force || old.axes.get(index) != new.axes.get(index) {
                        let value = raw_axis_value(new.axes[index]);
                        let event_type = EventType::AxisValueChanged(
                            value,
                            crate::EvCode(EvCode {
                                kind: EvCodeKind::Axis,
                                index: index as u32,
                            }),
                        );
                        send_wgi_axis_event(
                            tx,
                            epoch,
                            controller,
                            index as u32,
                            WgiEvent::new(controller.clone(), event_type),
                        )
                    }
                }
                for index in 0..new.buttons.len() {
                    if force || old.buttons.get(index) != new.buttons.get(index) {
                        let event_type = match new.buttons[index] {
                            true => EventType::ButtonPressed(crate::EvCode(EvCode {
                                kind: EvCodeKind::Button,
                                index: index as u32,
                            })),
                            false => EventType::ButtonReleased(crate::EvCode(EvCode {
                                kind: EvCodeKind::Button,
                                index: index as u32,
                            })),
                        };
                        send_wgi_event(tx, epoch, WgiEvent::new(controller.clone(), event_type))
                    }
                }

                for index in 0..old.switches.len() {
                    let (old_x, old_y) = direction_from_switch(old.switches[index]);
                    let (new_x, new_y) = direction_from_switch(new.switches[index]);
                    if force || old_x != new_x {
                        let event_type = EventType::AxisValueChanged(
                            new_x,
                            crate::EvCode(EvCode {
                                kind: EvCodeKind::Switch,
                                index: (index * 2) as u32,
                            }),
                        );
                        send_wgi_axis_event(
                            tx,
                            epoch,
                            controller,
                            (index * 2) as u32,
                            WgiEvent::new(controller.clone(), event_type),
                        )
                    }
                    if force || old_y != new_y {
                        let event_type = EventType::AxisValueChanged(
                            -new_y,
                            crate::EvCode(EvCode {
                                kind: EvCodeKind::Switch,
                                index: (index * 2) as u32 + 1,
                            }),
                        );
                        send_wgi_axis_event(
                            tx,
                            epoch,
                            controller,
                            (index * 2) as u32 + 1,
                            WgiEvent::new(controller.clone(), event_type),
                        )
                    }
                }
            }
            // WGI Gamepad
            (Reading::Gamepad(old), Reading::Gamepad(new)) => {
                #[rustfmt::skip]
                let axes = [
                    (new.LeftTrigger, old.LeftTrigger, nec::AXIS_LT2, 1.0),
                    (new.RightTrigger, old.RightTrigger, nec::AXIS_RT2, 1.0),
                    (new.LeftThumbstickX, old.LeftThumbstickX, nec::AXIS_LSTICKX, 1.0),
                    (new.LeftThumbstickY, old.LeftThumbstickY, nec::AXIS_LSTICKY, -1.0),
                    (new.RightThumbstickX, old.RightThumbstickX, nec::AXIS_RSTICKX, 1.0),
                    (new.RightThumbstickY, old.RightThumbstickY, nec::AXIS_RSTICKY, -1.0),
                ];
                for (new, old, code, multiplier) in axes {
                    if force || new != old {
                        send_wgi_axis_event(
                            tx,
                            epoch,
                            controller,
                            code.into_u32(),
                            WgiEvent::new(
                                controller.clone(),
                                EventType::AxisValueChanged(
                                    (multiplier * new * i32::MAX as f64) as i32,
                                    code,
                                ),
                            ),
                        );
                    }
                }

                for (current_button, ev_code) in WGI_TO_GILRS_BUTTON_MAP {
                    if force || (new.Buttons & current_button) != (old.Buttons & current_button) {
                        match new.Buttons & current_button != GamepadButtons::None {
                            true => send_wgi_event(
                                tx,
                                epoch,
                                WgiEvent::new(
                                    controller.clone(),
                                    EventType::ButtonPressed(ev_code),
                                ),
                            ),
                            false => send_wgi_event(
                                tx,
                                epoch,
                                WgiEvent::new(
                                    controller.clone(),
                                    EventType::ButtonReleased(ev_code),
                                ),
                            ),
                        }
                    }
                }
            }
            (a, b) => {
                warn!(
                    "WGI Controller changed from gamepad: {} to gamepad: {}. Could not compare \
                     last update.",
                    a.is_gamepad(),
                    b.is_gamepad()
                );
                #[cfg(debug_assertions)]
                panic!(
                    "Controllers shouldn't change type between updates, likely programmer error"
                );
            }
        }
    }

    fn is_gamepad(&self) -> bool {
        matches!(self, Reading::Gamepad(_))
    }
}

#[derive(Debug)]
pub struct Gamepad {
    id: u32,
    name: String,
    uuid: Uuid,
    is_connected: bool,
    /// This is the generic controller handle without any mappings
    /// https://learn.microsoft.com/en-us/uwp/api/windows.gaming.input.rawgamecontroller
    raw_game_controller: RawGameController,
    /// An ID for this device that will survive disconnects and restarts.
    /// [NonRoamableIds](https://learn.microsoft.com/en-us/uwp/api/windows.gaming.input.rawgamecontroller.nonroamableid)
    ///
    /// Changes if plugged into a different port and is not the same between different applications
    /// or PCs.
    non_roamable_id: HSTRING,
    /// If the controller has a [Gamepad](https://learn.microsoft.com/en-us/uwp/api/windows.gaming.input.gamepad?view=winrt-22621)
    /// mapping, this is used to access the mapped values.
    wgi_gamepad: Option<WgiGamepad>,
    /// Which reading this controller is polled through. It decides the event
    /// codes, the `AxisInfo` ranges and the lookup key for SDL mappings, and it
    /// is independent of whether the device also has a `Gamepad` mapping.
    reading: ReadingKind,
    axes: Option<Vec<EvCode>>,
    buttons: Option<Vec<EvCode>>,
}

impl Gamepad {
    fn new(id: u32, raw_game_controller: RawGameController) -> Result<Gamepad, PlatformError> {
        let is_connected = true;

        let non_roamable_id = raw_game_controller
            .NonRoamableId()
            .map_err(|e| PlatformError::Other(Box::new(e)))?;

        // See if we can cast this to a windows definition of a gamepad
        let wgi_gamepad = WgiGamepad::FromGameController(&raw_game_controller).ok();
        let reading = reading_kind(&raw_game_controller);
        let name = match raw_game_controller.DisplayName() {
            Ok(hstring) => hstring.to_string_lossy(),
            Err(_) => "unknown".to_string(),
        };

        let uuid = match reading {
            // The mappings this key resolves to were written against SDL's own
            // Windows drivers, not against Windows Gaming Input, so the element
            // order they name is not the one a `RawGameController` reports.
            // `collect_axes_and_buttons` translates the axis order and
            // `native_ev_codes` carries the raw indices, so a lookup can still
            // resolve -- and the comment there explains the difference.
            ReadingKind::Raw => {
                let vendor_id = raw_game_controller.HardwareVendorId().unwrap_or(0).to_be();
                let product_id = raw_game_controller.HardwareProductId().unwrap_or(0).to_be();
                let version = 0;

                // SDL uses the SDL_HARDWARE_BUS_BLUETOOTH bustype for IsWireless devices:
                // https://github.com/libsdl-org/SDL/blob/294ccba0a23b37fffef62189423444f93732e565/src/joystick/windows/SDL_windows_gaming_input.c#L335-L338
                // In my testing though, it caused my controllers to not find mappings.
                // SDL only uses their WGI implementation for UWP apps so I guess it hasn't been
                // used enough for people to submit mappings with the different bustype.
                let bustype = SDL_HARDWARE_BUS_USB.to_be();

                Uuid::from_fields(
                    bustype,
                    vendor_id,
                    0,
                    &[
                        (product_id >> 8) as u8,
                        product_id as u8,
                        0,
                        0,
                        (version >> 8) as u8,
                        version as u8,
                        0,
                        0,
                    ],
                )
            }
            // The mapped reading reports the fixed `native_ev_codes` layout, for
            // which there is no SDL mapping to look up.
            ReadingKind::Gamepad => Uuid::nil(),
        };

        let mut gamepad = Gamepad {
            id,
            name,
            uuid,
            is_connected,
            raw_game_controller,
            non_roamable_id,
            wgi_gamepad,
            reading,
            axes: None,
            buttons: None,
        };

        if gamepad.reading == ReadingKind::Raw {
            gamepad
                .collect_axes_and_buttons()
                .map_err(|e| PlatformError::Other(Box::new(e)))?;
        }

        Ok(gamepad)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn vendor_id(&self) -> Option<u16> {
        self.raw_game_controller.HardwareVendorId().ok()
    }

    pub fn product_id(&self) -> Option<u16> {
        self.raw_game_controller.HardwareProductId().ok()
    }

    pub fn is_connected(&self) -> bool {
        self.is_connected
    }

    pub fn power_info(&self) -> PowerInfo {
        self.power_info_err().unwrap_or(PowerInfo::Unknown)
    }

    /// Using this function so we can easily map errors to unknown
    fn power_info_err(&self) -> windows::core::Result<PowerInfo> {
        if !self.raw_game_controller.IsWireless()? {
            return Ok(PowerInfo::Wired);
        }
        let report: BatteryReport = self.raw_game_controller.TryGetBatteryReport()?;
        let status: BatteryStatus = report.Status()?;

        let power_info = match status {
            BatteryStatus::Discharging | BatteryStatus::Charging => {
                let full = report.FullChargeCapacityInMilliwattHours()?.GetInt32()? as f32;
                let remaining = report.RemainingCapacityInMilliwattHours()?.GetInt32()? as f32;
                let percent: u8 = ((remaining / full) * 100.0) as u8;
                match status {
                    _ if percent == 100 => PowerInfo::Charged,
                    BatteryStatus::Discharging => PowerInfo::Discharging(percent),
                    BatteryStatus::Charging => PowerInfo::Charging(percent),
                    _ => unreachable!(),
                }
            }
            BatteryStatus::NotPresent => PowerInfo::Wired,
            BatteryStatus::Idle => PowerInfo::Charged,
            BatteryStatus(_) => PowerInfo::Unknown,
        };
        Ok(power_info)
    }

    pub fn is_ff_supported(&self) -> bool {
        self.wgi_gamepad.is_some()
            && self
                .raw_game_controller
                .ForceFeedbackMotors()
                .ok()
                .map(|motors| motors.First())
                .is_some()
    }

    pub fn ff_device(&self) -> Option<FfDevice> {
        Some(FfDevice::new(self.id, self.wgi_gamepad.clone()))
    }

    pub fn buttons(&self) -> &[EvCode] {
        match &self.buttons {
            None => &native_ev_codes::BUTTONS,
            Some(buttons) => buttons,
        }
    }

    pub fn axes(&self) -> &[EvCode] {
        match &self.axes {
            None => &native_ev_codes::AXES,
            Some(axes) => axes,
        }
    }

    pub(crate) fn axis_info(&self, nec: EvCode) -> Option<&AxisInfo> {
        // A controller polled through the raw report is described by its own
        // element list, so return what we want SDL mappings to be able to use
        if self.reading == ReadingKind::Raw {
            return match nec.kind {
                EvCodeKind::Button => None,
                // Raw samples are scaled by `raw_axis_value` over the full
                // signed range, so report that range here instead of the
                // `i16` range SDL works with.
                EvCodeKind::Axis => Some(&AxisInfo {
                    min: i32::MIN,
                    max: i32::MAX,
                    deadzone: None,
                }),
                EvCodeKind::Switch => Some(&AxisInfo {
                    min: -1,
                    max: 1,
                    deadzone: None,
                }),
            };
        }

        // For Windows Gamepads, the triggers are 0.0 to 1.0 and the thumbsticks are -1.0 to 1.0
        // https://learn.microsoft.com/en-us/uwp/api/windows.gaming.input.gamepadreading#fields
        // Since Gilrs processes axis data as integers, the input has already been multiplied by
        // i32::MAX in the joy_value method.
        match nec {
            native_ev_codes::AXIS_LT2 | native_ev_codes::AXIS_RT2 => Some(&AxisInfo {
                min: 0,
                max: i32::MAX,
                deadzone: None,
            }),
            _ => Some(&AxisInfo {
                min: i32::MIN,
                max: i32::MAX,
                deadzone: None,
            }),
        }
    }

    fn collect_axes_and_buttons(&mut self) -> windows::core::Result<()> {
        let axis_count = self.raw_game_controller.AxisCount()? as u32;
        let button_count = self.raw_game_controller.ButtonCount()? as u32;
        let switch_count = self.raw_game_controller.SwitchCount()? as u32;
        self.buttons = Some(
            (0..button_count)
                .map(|index| EvCode {
                    kind: EvCodeKind::Button,
                    index,
                })
                .collect(),
        );
        self.axes = Some(
            (0..axis_count)
                .map(|slot| EvCode {
                    kind: EvCodeKind::Axis,
                    index: sdl_axis_to_raw_index(slot, axis_count),
                })
                .chain(
                    // Treat switches as two axes
                    (0..switch_count).flat_map(|index| {
                        [
                            EvCode {
                                kind: EvCodeKind::Switch,
                                index: index * 2,
                            },
                            EvCode {
                                kind: EvCodeKind::Switch,
                                index: (index * 2) + 1,
                            },
                        ]
                    }),
                )
                .collect(),
        );
        Ok(())
    }
}

/// The `RawGameController` axis index behind SDL's `a<slot>`, for a device that
/// reports `axis_count` axes.
///
/// An SDL mapping's `a0` … `a5` name the axes of the layout the mapping was
/// written against, which for the Windows mappings our lookup key resolves to is
/// the DirectInput one: left stick X, left stick Y, **left trigger**, right stick
/// X, right stick Y, right trigger. Windows Gaming Input reports the same six
/// axes as left stick X, left stick Y, right stick X, right stick Y, **left
/// trigger**, right trigger. The two agree on the four stick axes and disagree
/// about where the triggers sit, so the list `Gamepad::axes` returns is
/// presented to the mapping layer in SDL's order while every event still carries
/// the index the device reported.
///
/// Without the translation a mapping's `lefttrigger:a2` resolved to the right
/// stick's X axis, so pulling the left trigger moved the right stick sideways,
/// moving the right stick's Y axis fired the left trigger, and the left trigger
/// read as the right one.
///
/// Only the six-axis shape has a known translation. A device with a different
/// axis count keeps its own order, because nothing describes how its axes are
/// meant to be numbered and a wrong guess would be indistinguishable from having
/// no mapping at all.
fn sdl_axis_to_raw_index(slot: u32, axis_count: u32) -> u32 {
    /// Windows Gaming Input raw index for each SDL axis slot, for a device with
    /// the documented six-axis gamepad layout.
    const WGI_RAW_FOR_SDL_SLOT: [u32; 6] = [0, 1, 4, 2, 3, 5];

    if axis_count == WGI_RAW_FOR_SDL_SLOT.len() as u32 {
        WGI_RAW_FOR_SDL_SLOT[slot as usize]
    } else {
        slot
    }
}

#[cfg_attr(feature = "serde-serialize", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
enum EvCodeKind {
    Button = 0,
    Axis,
    Switch,
}

impl Display for EvCodeKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            EvCodeKind::Button => "Button",
            EvCodeKind::Axis => "Axis",
            EvCodeKind::Switch => "Switch",
        }
        .fmt(f)
    }
}

#[cfg_attr(feature = "serde-serialize", derive(Serialize, Deserialize))]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EvCode {
    kind: EvCodeKind,
    index: u32,
}

impl EvCode {
    pub fn into_u32(self) -> u32 {
        ((self.kind as u32) << 16) | self.index
    }
}

impl Display for EvCode {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}({})", self.kind, self.index)
    }
}

pub mod native_ev_codes {
    use super::{EvCode, EvCodeKind};

    // The indices below are Windows Gaming Input's own element order, not the
    // evdev order the upstream gilrs table was written in. A
    // `RawGameController` reports a standard gamepad's elements by index, and
    // `Mapping::default` binds a position name to an element code by matching
    // these constants against the codes the device actually produces. With the
    // evdev indices that match is wrong: the face buttons read shifted by one,
    // the D-pad falls off the end of a device that reports it as buttons, and
    // the two analog triggers land on the right stick's Y axis and the left
    // trigger.
    //
    // The axis order is Microsoft's: "Xbox controllers have 6 axes: 2 for each
    // stick and one for each trigger", read as left stick X, left stick Y, right
    // stick X, right stick Y, left trigger, right trigger
    // (https://learn.microsoft.com/en-us/uwp/gaming/raw-game-controller). A hat
    // is reported as a switch rather than an axis, which is why the D-pad axes
    // are switch indices.
    //
    // The button order is the order Windows gamepads expose, which is the order
    // the SDL mappings for Windows describe. Microsoft does not fix it across
    // devices -- a device declares its own order in the registry under
    // `GameInput\Devices\<vid><pid>\Labels\Buttons` -- so a device that reports
    // a different order needs an SDL mapping that describes it, which is what
    // `Mapping::default` cannot be. Everything past the guide button is placed
    // after it so that a device which does not have those elements fails
    // `Mapping::default`'s presence check instead of borrowing another
    // button's index.
    pub const AXIS_LSTICKX: EvCode = EvCode {
        kind: EvCodeKind::Axis,
        index: 0,
    };
    pub const AXIS_LSTICKY: EvCode = EvCode {
        kind: EvCodeKind::Axis,
        index: 1,
    };
    pub const AXIS_RSTICKX: EvCode = EvCode {
        kind: EvCodeKind::Axis,
        index: 2,
    };
    pub const AXIS_RSTICKY: EvCode = EvCode {
        kind: EvCodeKind::Axis,
        index: 3,
    };
    pub const AXIS_LT2: EvCode = EvCode {
        kind: EvCodeKind::Axis,
        index: 4,
    };
    pub const AXIS_RT2: EvCode = EvCode {
        kind: EvCodeKind::Axis,
        index: 5,
    };
    pub const AXIS_RT: EvCode = EvCode {
        kind: EvCodeKind::Axis,
        index: 6,
    };
    pub const AXIS_LT: EvCode = EvCode {
        kind: EvCodeKind::Axis,
        index: 7,
    };
    pub const AXIS_LEFTZ: EvCode = EvCode {
        kind: EvCodeKind::Axis,
        index: 8,
    };
    pub const AXIS_RIGHTZ: EvCode = EvCode {
        kind: EvCodeKind::Axis,
        index: 9,
    };

    pub const AXIS_DPADX: EvCode = EvCode {
        kind: EvCodeKind::Switch,
        index: 0,
    };
    pub const AXIS_DPADY: EvCode = EvCode {
        kind: EvCodeKind::Switch,
        index: 1,
    };

    pub const BTN_SOUTH: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 0,
    };
    pub const BTN_EAST: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 1,
    };
    pub const BTN_WEST: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 2,
    };
    pub const BTN_NORTH: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 3,
    };
    pub const BTN_LT: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 4,
    };
    pub const BTN_RT: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 5,
    };
    pub const BTN_SELECT: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 6,
    };
    pub const BTN_START: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 7,
    };
    pub const BTN_LTHUMB: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 8,
    };
    pub const BTN_RTHUMB: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 9,
    };
    // A device that reports its D-pad as buttons occupies these four. Keeping
    // them at their real indices is what makes such a device's D-pad reachable,
    // and `axis_dpad_to_button` then declines to synthesise a second set from
    // the hat, which is exactly what that filter's presence check is for.
    pub const BTN_DPAD_UP: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 10,
    };
    pub const BTN_DPAD_DOWN: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 11,
    };
    pub const BTN_DPAD_LEFT: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 12,
    };
    pub const BTN_DPAD_RIGHT: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 13,
    };
    pub const BTN_MODE: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 14,
    };
    // A device that reports its triggers as buttons has no fixed index for them,
    // so these sit past every element a gamepad is known to report and only bind
    // on a device with more buttons than that.
    pub const BTN_LT2: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 15,
    };
    pub const BTN_RT2: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 16,
    };
    pub const BTN_C: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 17,
    };
    pub const BTN_Z: EvCode = EvCode {
        kind: EvCodeKind::Button,
        index: 18,
    };

    pub(super) static BUTTONS: [EvCode; 14] = [
        BTN_SOUTH,
        BTN_EAST,
        BTN_WEST,
        BTN_NORTH,
        BTN_LT,
        BTN_RT,
        BTN_SELECT,
        BTN_START,
        BTN_LTHUMB,
        BTN_RTHUMB,
        BTN_DPAD_UP,
        BTN_DPAD_DOWN,
        BTN_DPAD_LEFT,
        BTN_DPAD_RIGHT,
    ];

    pub(super) static AXES: [EvCode; 6] = [
        AXIS_LSTICKX,
        AXIS_LSTICKY,
        AXIS_RSTICKX,
        AXIS_RSTICKY,
        AXIS_LT2,
        AXIS_RT2,
    ];
}

#[cfg(test)]
mod tests {
    use super::{
        native_ev_codes, raw_axis_value, reading_kind_for, sdl_axis_to_raw_index, AxisInfo, EvCode,
        EvCodeKind, ReadingKind,
    };

    const AXIS_RANGE: AxisInfo = AxisInfo {
        min: i32::MIN,
        max: i32::MAX,
        deadzone: None,
    };

    fn axis(index: u32) -> EvCode {
        EvCode {
            kind: EvCodeKind::Axis,
            index,
        }
    }

    fn button(index: u32) -> EvCode {
        EvCode {
            kind: EvCodeKind::Button,
            index,
        }
    }

    /// Mirrors `gilrs::gamepad::axis_value`, which renormalizes whatever range
    /// `Gamepad::axis_info` reports onto `[-1.0, 1.0]`.
    fn normalized(device: f64) -> f32 {
        let raw = raw_axis_value(device) as f32;
        let range = AXIS_RANGE.max as f32 - AXIS_RANGE.min as f32;
        ((raw - AXIS_RANGE.min as f32) / range * 2.0 - 1.0).clamp(-1.0, 1.0)
    }

    /// `Gamepad::axis_info` reports `EvCodeKind::Axis` as the full signed
    /// `i32` range, so a raw sample has to be scaled over that same range.
    /// SDL's Windows Gaming Input backend pre-centers instead, and gilrs used
    /// to keep that offset: every neutral stick read as full negative
    /// deflection and the whole negative half of its travel clamped.
    #[test]
    fn raw_axis_value_is_neutral_at_rest_and_spans_the_signed_range() {
        assert_eq!(0, raw_axis_value(0.0));
        assert_eq!(i32::MAX, raw_axis_value(1.0));
        assert_eq!(-i32::MAX, raw_axis_value(-1.0));

        assert!(normalized(0.0).abs() <= f32::EPSILON);
        assert_eq!(-1.0, normalized(-1.0));
        assert_eq!(1.0, normalized(1.0));
    }

    /// The center of the range must not collapse: every step away from neutral
    /// has to move the reading the same way it moves on the device.
    #[test]
    fn raw_axis_value_stays_monotonic_across_the_whole_range() {
        let mut previous = normalized(-1.0);
        for step in 1..=1_000 {
            let device = -1.0 + step as f64 / 500.0;
            let current = normalized(device);
            assert!(
                current >= previous,
                "reading moved backwards at {device}: {previous} -> {current}"
            );
            previous = current;
        }
    }

    /// A device that exposes any raw report is polled through
    /// `RawGameController`, which Windows serves regardless of which window owns
    /// the foreground. Only a device with nothing to report raw falls back to the
    /// mapped `Gamepad` reading.
    #[test]
    fn a_device_with_a_raw_report_is_polled_without_the_foreground_window() {
        for (buttons, axes, switches) in [(14, 6, 1), (1, 0, 0), (0, 1, 0), (0, 0, 1), (20, 4, 0)] {
            assert_eq!(
                ReadingKind::Raw,
                reading_kind_for(buttons, axes, switches),
                "{buttons}/{axes}/{switches} should be read raw"
            );
        }
    }

    /// With nothing to report raw there is no alternative to the mapped reading.
    #[test]
    fn a_device_without_a_raw_report_falls_back_to_the_mapped_reading() {
        assert_eq!(ReadingKind::Gamepad, reading_kind_for(0, 0, 0));
    }

    /// Microsoft's `Raw game controller` documentation fixes the six axes of a
    /// gamepad: left stick X, left stick Y, right stick X, right stick Y, left
    /// trigger, right trigger. `Mapping::default` binds a position name to an
    /// element code by matching these constants against what the device
    /// reports, so a constant carrying the evdev index instead made the left
    /// trigger read the right stick's Y axis, the right trigger read the left
    /// trigger, and the right stick's Y axis read the right trigger. A resting
    /// stick then sat exactly on the axis-to-button threshold, so the trigger it
    /// fed flapped on every sample.
    #[test]
    fn the_position_names_carry_the_windows_gaming_input_axis_order() {
        use native_ev_codes::{
            AXIS_LSTICKX, AXIS_LSTICKY, AXIS_LT2, AXIS_RSTICKX, AXIS_RSTICKY, AXIS_RT2,
        };

        assert_eq!(AXIS_LSTICKX, axis(0));
        assert_eq!(AXIS_LSTICKY, axis(1));
        assert_eq!(AXIS_RSTICKX, axis(2));
        assert_eq!(AXIS_RSTICKY, axis(3));
        assert_eq!(AXIS_LT2, axis(4));
        assert_eq!(AXIS_RT2, axis(5));
    }

    /// The four face buttons are the other half of the same table, and the shift
    /// is what a user sees as "the wrong key lights up": the evdev order put
    /// west at index zero, so a device reporting the Windows order showed its
    /// south button's press as west, east as south and west as east.
    #[test]
    fn the_face_buttons_carry_the_windows_gaming_input_button_order() {
        use native_ev_codes::{BTN_EAST, BTN_NORTH, BTN_SOUTH, BTN_WEST};

        assert_eq!(BTN_SOUTH, button(0));
        assert_eq!(BTN_EAST, button(1));
        assert_eq!(BTN_WEST, button(2));
        assert_eq!(BTN_NORTH, button(3));
    }

    /// The D-pad has to be reachable on a device that reports it as four
    /// buttons. With the codes parked past the end of every device the buttons
    /// fell off the end of the table and every direction was dead, which is the
    /// "the cross does nothing" half of the same report.
    #[test]
    fn a_device_that_reports_its_dpad_as_buttons_reaches_every_direction() {
        use native_ev_codes::{BTN_DPAD_DOWN, BTN_DPAD_LEFT, BTN_DPAD_RIGHT, BTN_DPAD_UP};

        // The order the directions occupy is the one a Windows gamepad reports
        // them in, after the two stick clicks.
        assert_eq!(BTN_DPAD_UP, button(10));
        assert_eq!(BTN_DPAD_DOWN, button(11));
        assert_eq!(BTN_DPAD_LEFT, button(12));
        assert_eq!(BTN_DPAD_RIGHT, button(13));

        // `Mapping::default` keeps a code only when the device reports that many
        // buttons, so all four have to be inside a fifteen-button device and
        // outside a ten-button one.
        let reports = |button_count: u32| (0..button_count).map(button).collect::<Vec<EvCode>>();
        let fifteen = reports(15);
        let ten = reports(10);
        for direction in [BTN_DPAD_UP, BTN_DPAD_DOWN, BTN_DPAD_LEFT, BTN_DPAD_RIGHT] {
            assert!(
                fifteen.contains(&direction),
                "{direction:?} must be reachable on a fifteen-button device"
            );
            assert!(
                !ten.contains(&direction),
                "{direction:?} must not bind on a ten-button device"
            );
        }
    }

    /// An SDL mapping's `a<slot>` names the axis of the layout it was written
    /// against, which for the Windows mappings is the DirectInput one. The raw
    /// reading has to be presented in that order or every mapping resolves the
    /// wrong axis, while the events themselves keep carrying the index the
    /// device reported.
    #[test]
    fn a_mapping_slot_resolves_to_the_raw_axis_behind_that_control() {
        // SDL slot 0 and 1 are the left stick's X and Y in both layouts.
        assert_eq!(sdl_axis_to_raw_index(0, 6), 0);
        assert_eq!(sdl_axis_to_raw_index(1, 6), 1);
        // SDL slot 2 is the left trigger, which Windows reports as raw axis 4.
        assert_eq!(sdl_axis_to_raw_index(2, 6), 4);
        // SDL slots 3 and 4 are the right stick's X and Y, raw axes 2 and 3.
        assert_eq!(sdl_axis_to_raw_index(3, 6), 2);
        assert_eq!(sdl_axis_to_raw_index(4, 6), 3);
        // SDL slot 5 is the right trigger, raw axis 5 in both layouts.
        assert_eq!(sdl_axis_to_raw_index(5, 6), 5);

        // The translation must be a bijection, or an axis would be unreachable
        // and another would answer twice.
        let mut seen = std::collections::BTreeSet::new();
        for slot in 0..6 {
            assert!(
                seen.insert(sdl_axis_to_raw_index(slot, 6)),
                "SDL slot {slot} resolves to an axis another slot already claims"
            );
        }
        assert_eq!(seen.len(), 6);
    }

    /// Only the six-axis shape has a known translation. Any other axis count
    /// keeps the device's own order, because a guess would be indistinguishable
    /// from having no mapping at all.
    #[test]
    fn a_device_without_the_six_axis_shape_keeps_its_own_axis_order() {
        for axis_count in [0, 1, 2, 3, 4, 5, 7, 8] {
            for slot in 0..axis_count {
                assert_eq!(
                    sdl_axis_to_raw_index(slot, axis_count),
                    slot,
                    "{axis_count} axes: slot {slot} must keep its own index"
                );
            }
        }
    }
}
