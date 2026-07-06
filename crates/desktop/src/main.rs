//! Crawl Crew — the friendly desktop frontend over the crawler-node core.
//!
//! Same loop as the CLI (`crawler_node::worker::run`), plus: a warm little
//! visualization of jobs flowing, the fleet leaderboard, a native
//! notification + in-app banner when the Riot key expires, and a tray icon.
//!
//! Resource discipline: closing the window *destroys* the webview (freeing
//! its RAM) while the node keeps crawling; the tray brings it back. The UI
//! rebuilds itself from [`crawler_node::events::NodeHandle::snapshot`].

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crawler_node::config::{self, NodeConfig};
use crawler_node::events::{NodeEvent, NodeHandle, Snapshot};
use crawler_node::worker::{self, ServerClient};
use crawler_proto as proto;
use tauri::menu::{Menu, MenuItem};
use tauri::tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent};
use tauri::{AppHandle, Emitter, Manager};
#[cfg(not(windows))]
use tauri_plugin_notification::NotificationExt;
use tokio::sync::watch;

mod mock;

struct AppData {
    handle: Arc<NodeHandle>,
    config_path: PathBuf,
    cfg: Mutex<Option<NodeConfig>>,
    stop_tx: Mutex<Option<watch::Sender<bool>>>,
    client: Mutex<Option<Arc<ServerClient>>>,
    /// `--mock`: fake server + fake Riot, for frontend work (see `mock`).
    mock: bool,
}

#[derive(serde::Serialize)]
struct UiState {
    enrolled: bool,
    name: String,
    server: String,
    version: String,
    snapshot: Snapshot,
}

fn ui_state(data: &AppData) -> UiState {
    let cfg = data.cfg.lock().unwrap();
    UiState {
        enrolled: cfg.is_some(),
        name: cfg.as_ref().map(|c| c.name.clone()).unwrap_or_default(),
        server: cfg.as_ref().map(|c| c.server.clone()).unwrap_or_default(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        snapshot: data.handle.snapshot(),
    }
}

/// Spawns the node loop for the current config (call at most once per
/// enrollment; there is no re-enroll flow in the UI).
fn start_worker(data: &AppData) {
    let cfg = match data.cfg.lock().unwrap().clone() {
        Some(c) => c,
        None => return,
    };
    let (tx, rx) = watch::channel(false);
    *data.stop_tx.lock().unwrap() = Some(tx);
    if data.mock {
        tauri::async_runtime::spawn(mock::run(data.handle.clone(), rx));
        return;
    }
    *data.client.lock().unwrap() =
        Some(Arc::new(ServerClient::new(&cfg.server, &cfg.token)));
    let handle = data.handle.clone();
    let path = data.config_path.clone();
    tauri::async_runtime::spawn(async move {
        if let Err(e) = worker::run(cfg, path, handle, rx).await {
            tracing::error!(error = %e, "node loop exited");
        }
    });
}

#[tauri::command]
fn get_state(state: tauri::State<'_, AppData>) -> UiState {
    ui_state(&state)
}

#[tauri::command]
async fn enroll(
    server: String,
    name: String,
    invite: String,
    riot_key: String,
    state: tauri::State<'_, AppData>,
) -> Result<UiState, String> {
    if state.cfg.lock().unwrap().is_some() {
        return Err("already enrolled".into());
    }
    let server = server.trim().trim_end_matches('/').to_string();
    let cfg = if state.mock {
        // Accept anything, talk to nobody, save nothing.
        NodeConfig {
            server,
            name: name.trim().to_string(),
            token: "mock-token".to_string(),
            riot_api_key: riot_key.trim().to_string(),
        }
    } else {
        let er = crawler_node::enroll_request(&server, name.trim(), invite.trim())
            .await
            .map_err(|e| e.to_string())?;
        let cfg = NodeConfig {
            server,
            name: er.name,
            token: er.token,
            riot_api_key: riot_key.trim().to_string(),
        };
        config::save(&state.config_path, &cfg).map_err(|e| e.to_string())?;
        cfg
    };
    *state.cfg.lock().unwrap() = Some(cfg);
    start_worker(&state);
    Ok(ui_state(&state))
}

#[tauri::command]
fn set_key(key: String, state: tauri::State<'_, AppData>) -> Result<(), String> {
    let mut guard = state.cfg.lock().unwrap();
    let cfg = guard.as_mut().ok_or("not enrolled")?;
    cfg.riot_api_key = key.trim().to_string();
    if !state.mock {
        config::save(&state.config_path, cfg).map_err(|e| e.to_string())?;
    }
    // Skip the paused loop's 15 s mtime poll (in mock: resume immediately).
    state.handle.key_update.notify_waiters();
    Ok(())
}

#[tauri::command]
async fn fetch_stats(state: tauri::State<'_, AppData>) -> Result<proto::StatsResponse, String> {
    if state.mock {
        let you = state
            .cfg
            .lock()
            .unwrap()
            .as_ref()
            .map(|c| c.name.clone())
            .ok_or("not enrolled")?;
        return Ok(mock::stats(&you, &state.handle));
    }
    let client = state
        .client
        .lock()
        .unwrap()
        .clone()
        .ok_or("not enrolled")?;
    client.stats().await.map_err(|e| e.to_string())
}

/// Show the main window, recreating it if the close button destroyed it.
fn show_main(app: &AppHandle) {
    if let Some(w) = app.get_webview_window("main") {
        let _ = w.unminimize();
        let _ = w.show();
        let _ = w.set_focus();
    } else if let Some(cfg) = app.config().app.windows.first().cloned() {
        if let Err(e) = tauri::WebviewWindowBuilder::from_config(app, &cfg)
            .and_then(|b| b.build())
        {
            tracing::error!(error = %e, "recreating window failed");
        }
    }
}

/// Native WinRT toasts for the key-expiry flow. The notification plugin
/// can't do this on desktop: clicks are ignored, the toast fades after a
/// few seconds, and it can't be withdrawn once the key is fixed.
#[cfg(windows)]
mod win_toast {
    use std::sync::OnceLock;

    use tauri::AppHandle;
    use tauri_winrt_notification::{Scenario, Toast};

    /// AppUserModelID for the toast. Installed builds use the bundle
    /// identifier (the bundler's Start Menu shortcut registers it, giving
    /// the toast our name and icon). Builds running out of target/ have no
    /// such registration, so we register a per-user `<identifier>.dev`
    /// AUMID in HKCU on first use — the shortcut-free route Windows
    /// supports for unpackaged apps. The `.dev` suffix keeps it from ever
    /// shadowing the installed app's identity. If registration fails we
    /// borrow PowerShell's AUMID (toast works, wrong attribution).
    fn app_id(app: &AppHandle) -> String {
        let in_target = std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(|d| d.display().to_string()))
            .is_some_and(|dir| {
                dir.ends_with("\\target\\debug") || dir.ends_with("\\target\\release")
            });
        if !in_target {
            return app.config().identifier.clone();
        }
        static DEV_AUMID: OnceLock<String> = OnceLock::new();
        DEV_AUMID
            .get_or_init(|| {
                register_dev_aumid(app).unwrap_or_else(|e| {
                    tracing::warn!(error = %e, "dev AUMID registration failed");
                    Toast::POWERSHELL_APP_ID.to_string()
                })
            })
            .clone()
    }

    /// Idempotent; the key persists in HKCU (a few bytes) so attribution
    /// also survives for toasts left in the Action Center after exit.
    fn register_dev_aumid(app: &AppHandle) -> std::io::Result<String> {
        let aumid = format!("{}.dev", app.config().identifier);
        let display = format!(
            "{} (dev)",
            app.config().product_name.as_deref().unwrap_or("Crawl Crew")
        );
        let (key, _) = winreg::RegKey::predef(winreg::enums::HKEY_CURRENT_USER)
            .create_subkey(format!(r"Software\Classes\AppUserModelId\{aumid}"))?;
        key.set_value("DisplayName", &display)?;
        // Compile-time path: only ever used by builds run from target/.
        let icon = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("icons")
            .join("icon.ico");
        if icon.exists() {
            key.set_value("IconUri", &icon.display().to_string())?;
        }
        Ok(aumid)
    }

    /// Crawling is paused until the operator acts, so the toast uses the
    /// reminder scenario: pre-expanded, stays on screen until dismissed
    /// (Windows honors that only because a button is attached). Clicking
    /// the body or the button brings the window back.
    pub fn show_key_needed(app: &AppHandle) {
        let handle = app.clone();
        let shown = Toast::new(&app_id(app))
            .title("Crawl Crew needs a fresh key")
            .text1("Riot expired your API key (dev keys last 24h).")
            .text2("Paste a new one and crawling resumes — takes 20 seconds.")
            .scenario(Scenario::Reminder)
            .add_button("Open Crawl Crew", "open")
            .on_activated(move |_action| {
                // Fires on a WinRT thread; window (re)creation needs main.
                let h = handle.clone();
                let _ = handle.run_on_main_thread(move || crate::show_main(&h));
                Ok(())
            })
            .show();
        if let Err(e) = shown {
            tracing::warn!(error = %e, "key toast failed");
        }
    }

    /// Withdraw our toasts from screen and Action Center. Blanket clear is
    /// fine: the key reminder is the only toast this app ever sends.
    pub fn clear(app: &AppHandle) {
        use windows::core::HSTRING;
        use windows::UI::Notifications::ToastNotificationManager;
        if let Ok(history) = ToastNotificationManager::History() {
            let _ = history.ClearWithId(&HSTRING::from(app_id(app)));
        }
    }
}

/// One self-update attempt against the GitHub Releases feed. Returns
/// Ok(true) once a newer build has been downloaded and installed. On
/// Windows `download_and_install` hands off to the NSIS installer, which
/// exits this process and relaunches the app itself; other platforms
/// return here so the caller can restart.
async fn check_update(app: &AppHandle) -> tauri_plugin_updater::Result<bool> {
    use tauri_plugin_updater::UpdaterExt;
    let Some(update) = app.updater()?.check().await? else {
        return Ok(false);
    };
    tracing::info!(version = %update.version, "update available, installing");
    update.download_and_install(|_, _| {}, || {}).await?;
    Ok(true)
}

fn quit(app: &AppHandle) {
    // Best effort: let the uploader flush for a moment before exiting.
    // Anything unflushed is re-issued by the server after the lease.
    if let Some(tx) = app.state::<AppData>().stop_tx.lock().unwrap().as_ref() {
        let _ = tx.send(true);
    }
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        app.exit(0);
    });
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mock = std::env::args().any(|a| a == "--mock")
        || std::env::var("CRAWL_CREW_MOCK").is_ok_and(|v| !v.is_empty() && v != "0");
    if mock {
        tracing::info!("MOCK MODE: fake server + fake Riot, config on disk untouched");
    }
    let config_path = config::default_path();
    // Mock mode ignores any real enrollment on disk so the enrollment
    // form is testable too (fill it with anything).
    let cfg = if mock { None } else { config::load(&config_path).unwrap_or_default() };
    let data = AppData {
        handle: NodeHandle::new(),
        config_path,
        cfg: Mutex::new(cfg),
        stop_tx: Mutex::new(None),
        client: Mutex::new(None),
        mock,
    };

    tauri::Builder::default()
        .plugin(tauri_plugin_notification::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .manage(data)
        .invoke_handler(tauri::generate_handler![get_state, enroll, set_key, fetch_stats])
        .setup(|app| {
            let data = app.state::<AppData>();

            // A toast surviving from a previous run is dead (its click
            // handler died with the process); drop it. If the key is still
            // bad the worker re-emits KeyBad and a live toast replaces it.
            #[cfg(windows)]
            win_toast::clear(app.handle());

            // Tray: left-click opens, menu has Open/Quit.
            let open_item = MenuItem::with_id(app, "open", "Open Crawl Crew", true, None::<&str>)?;
            let quit_item = MenuItem::with_id(app, "quit", "Quit (stops crawling)", true, None::<&str>)?;
            let menu = Menu::with_items(app, &[&open_item, &quit_item])?;
            TrayIconBuilder::with_id("crawl-crew")
                .icon(app.default_window_icon().expect("window icon").clone())
                .tooltip("Crawl Crew — crawling away, thank you!")
                .menu(&menu)
                .show_menu_on_left_click(false)
                .on_menu_event(|app, ev| match ev.id.as_ref() {
                    "open" => show_main(app),
                    "quit" => quit(app),
                    _ => {}
                })
                .on_tray_icon_event(|tray, event| {
                    if let TrayIconEvent::Click {
                        button: MouseButton::Left,
                        button_state: MouseButtonState::Up,
                        ..
                    } = event
                    {
                        show_main(tray.app_handle());
                    }
                })
                .build(app)?;

            // Forward node events to the webview; native-notify on key death.
            let app_handle = app.handle().clone();
            let mut rx = data.handle.events.subscribe();
            tauri::async_runtime::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(ev) => {
                            match &ev {
                                NodeEvent::KeyBad => {
                                    #[cfg(windows)]
                                    win_toast::show_key_needed(&app_handle);
                                    #[cfg(not(windows))]
                                    {
                                        let _ = app_handle
                                            .notification()
                                            .builder()
                                            .title("Crawl Crew needs a fresh key")
                                            .body(
                                                "Riot expired your API key (dev keys last 24h). \
                                                 Open Crawl Crew and paste a new one — takes 20 seconds.",
                                            )
                                            .show();
                                    }
                                }
                                // Key fixed (maybe via CLI): retract the toast.
                                #[cfg(windows)]
                                NodeEvent::KeyOk => win_toast::clear(&app_handle),
                                _ => {}
                            }
                            let _ = app_handle.emit("node", &ev);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(_) => break,
                    }
                }
            });

            // Self-update: check at launch, then every six hours — the
            // node lives in the tray for weeks, so a launch-only check
            // would leave the fleet stale. A restart mid-crawl is fine:
            // the server re-issues anything unflushed after the lease.
            // Skipped in dev so target/ builds don't install over
            // themselves the moment a newer release exists.
            if !data.mock && !cfg!(debug_assertions) {
                let app_handle = app.handle().clone();
                tauri::async_runtime::spawn(async move {
                    loop {
                        match check_update(&app_handle).await {
                            Ok(true) => app_handle.restart(),
                            Ok(false) => {}
                            Err(e) => tracing::warn!(error = %e, "update check failed"),
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(6 * 3600)).await;
                    }
                });
            }

            start_worker(&data);
            Ok(())
        })
        .on_window_event(|window, event| {
            if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                // Keep crawling, free the webview's RAM; tray restores it.
                api.prevent_close();
                let w = window.clone();
                tauri::async_runtime::spawn(async move {
                    let _ = w.destroy();
                });
            }
        })
        .build(tauri::generate_context!())
        .expect("building Crawl Crew")
        .run(|_app, event| {
            if let tauri::RunEvent::ExitRequested { api, code, .. } = event {
                // No windows left ≠ quit: the node lives in the tray.
                if code.is_none() {
                    api.prevent_exit();
                }
            }
        });
}
