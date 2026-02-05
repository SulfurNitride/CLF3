//! GUI module for CLF3 using Slint framework
//!
//! Provides a graphical interface for the modlist installer.

mod settings;
mod image_cache;

pub use settings::Settings;
pub use image_cache::ImageCache;

// Main window component - full installer UI with setup form
slint::slint! {
    import { Button, CheckBox, ScrollView, ProgressIndicator, ComboBox } from "std-widgets.slint";

    // Catppuccin Mocha color palette
    global Theme {
        // Base colors
        out property <color> base: #1e1e2e;
        out property <color> mantle: #181825;
        out property <color> crust: #11111b;
        out property <color> surface0: #313244;
        out property <color> surface1: #45475a;
        out property <color> surface2: #585b70;

        // Text colors
        out property <color> text: #cdd6f4;
        out property <color> subtext0: #a6adc8;
        out property <color> subtext1: #bac2de;
        out property <color> overlay0: #6c7086;

        // Accent colors
        out property <color> blue: #89b4fa;
        out property <color> teal: #94e2d5;
        out property <color> green: #a6e3a1;
        out property <color> yellow: #f9e2af;
        out property <color> peach: #fab387;
        out property <color> red: #f38ba8;
        out property <color> mauve: #cba6f7;
        out property <color> lavender: #b4befe;
    }

    // Install mode enumeration (Wabbajack only - Collection support removed)
    export enum InstallMode {
        Wabbajack,
    }

    // Installation state
    export enum InstallState {
        Idle,
        Validating,
        Downloading,
        Extracting,
        Installing,
        Complete,
        Error,
        Cancelled,
    }

    // API key validation state
    export enum ApiKeyState {
        Unknown,
        Validating,
        Valid,
        Invalid,
    }

    // Proton option for dropdown
    export struct ProtonOption {
        index: int,
        name: string,
        is_experimental: bool,
    }

    // Custom styled text input with label
    component LabeledInput inherits Rectangle {
        in property <string> label;
        in property <string> placeholder: "";
        in-out property <string> value;
        in property <bool> is_password: false;
        in property <bool> enabled: true;
        callback edited(string);

        height: 70px;

        VerticalLayout {
            spacing: 6px;

            Text {
                text: label;
                font-size: 13px;
                font-weight: 500;
                color: Theme.subtext1;
            }

            Rectangle {
                height: 36px;
                background: Theme.crust;
                border-radius: 6px;

                // Placeholder text (shown when input is empty)
                if value == "": Text {
                    x: 12px;
                    text: placeholder;
                    font-size: 13px;
                    color: Theme.overlay0;
                    vertical-alignment: center;
                }

                // Masked display for password
                if is_password && value != "": Text {
                    x: 12px;
                    text: "●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●";
                    font-size: 13px;
                    color: Theme.text;
                    vertical-alignment: center;
                    overflow: clip;
                    width: parent.width - 24px;
                }

                input := TextInput {
                    x: 12px;
                    y: (parent.height - 18px) / 2;
                    width: parent.width - 24px;
                    height: 18px;
                    text <=> value;
                    font-size: 13px;
                    color: is_password ? transparent : Theme.text;
                    enabled: enabled;
                    single-line: true;
                    vertical-alignment: center;
                    edited => { edited(self.text); }
                }
            }
        }
    }

    // Path input with browse button (compact single-line with inline label)
    component PathInput inherits Rectangle {
        in property <string> label;
        in property <string> placeholder: "";
        in-out property <string> value;
        in property <bool> enabled: true;
        callback browse-clicked();
        callback edited(string);

        height: 28px;

        HorizontalLayout {
            spacing: 6px;

            Text {
                text: label + ":";
                font-size: 11px;
                font-weight: 500;
                color: Theme.subtext1;
                vertical-alignment: center;
                width: 90px;
            }

            Rectangle {
                horizontal-stretch: 1;
                height: 26px;
                background: Theme.crust;
                border-radius: 4px;

                // Placeholder text
                if value == "": Text {
                    x: 8px;
                    text: placeholder;
                    font-size: 11px;
                    color: Theme.overlay0;
                    vertical-alignment: center;
                }

                path-input := TextInput {
                    x: 8px;
                    y: (parent.height - 14px) / 2;
                    width: parent.width - 16px;
                    height: 14px;
                    text <=> value;
                    font-size: 11px;
                    color: Theme.text;
                    enabled: enabled;
                    single-line: true;
                    vertical-alignment: center;
                    edited => { edited(self.text); }
                }
            }

            Rectangle {
                width: 54px;
                height: 26px;
                background: enabled ? (touch.has-hover ? Theme.surface1 : Theme.surface0) : Theme.mantle;
                border-radius: 4px;

                touch := TouchArea {
                    enabled: enabled;
                    clicked => { browse-clicked(); }
                }

                Text {
                    text: "Browse";
                    font-size: 11px;
                    color: enabled ? Theme.text : Theme.overlay0;
                    horizontal-alignment: center;
                    vertical-alignment: center;
                }
            }
        }
    }

    // Tab button component
    component TabButton inherits Rectangle {
        in property <string> label;
        in property <bool> active: false;
        callback clicked();

        height: 36px;
        horizontal-stretch: 1;
        background: active ? Theme.surface0 : transparent;
        border-radius: 6px;

        states [
            hover when touch.has-hover && !active: {
                background: Theme.mantle;
            }
        ]

        touch := TouchArea {
            clicked => { root.clicked(); }
        }

        Text {
            text: label;
            font-size: 14px;
            font-weight: active ? 600 : 400;
            color: active ? Theme.blue : Theme.subtext0;
            horizontal-alignment: center;
            vertical-alignment: center;
        }

        // Active indicator line
        if active: Rectangle {
            y: parent.height - 3px;
            height: 3px;
            background: Theme.blue;
            border-radius: 1.5px;
        }
    }

    // Primary action button
    component PrimaryButton inherits Rectangle {
        in property <string> label;
        in property <bool> enabled: true;
        callback clicked();

        height: 36px;
        background: enabled ? (touch.has-hover ? #7aa2e8 : Theme.blue) : Theme.surface1;
        border-radius: 6px;

        touch := TouchArea {
            enabled: enabled;
            clicked => { root.clicked(); }
        }

        Text {
            text: label;
            font-size: 13px;
            font-weight: 600;
            color: enabled ? Theme.crust : Theme.overlay0;
            horizontal-alignment: center;
            vertical-alignment: center;
        }
    }

    // Secondary action button
    component SecondaryButton inherits Rectangle {
        in property <string> label;
        in property <bool> enabled: true;
        callback clicked();

        height: 36px;
        background: touch.has-hover && enabled ? Theme.surface1 : Theme.surface0;
        border-radius: 6px;
        border-width: 1px;
        border-color: Theme.surface1;

        touch := TouchArea {
            enabled: enabled;
            clicked => { root.clicked(); }
        }

        Text {
            text: label;
            font-size: 13px;
            font-weight: 500;
            color: enabled ? Theme.text : Theme.overlay0;
            horizontal-alignment: center;
            vertical-alignment: center;
        }
    }

    // Link button (text only)
    component LinkButton inherits Rectangle {
        in property <string> label;
        in property <bool> enabled: true;
        callback clicked();

        height: 20px;
        background: transparent;

        touch := TouchArea {
            enabled: enabled;
            clicked => { root.clicked(); }
        }

        Text {
            text: label;
            font-size: 11px;
            color: touch.has-hover && enabled ? Theme.lavender : Theme.blue;
            horizontal-alignment: center;
            vertical-alignment: center;
        }
    }

    // API Key status indicator (clickable for help when state is Unknown)
    component ApiKeyStatus inherits Rectangle {
        in property <ApiKeyState> state: ApiKeyState.Unknown;
        callback clicked();

        width: 24px;
        height: 24px;
        border-radius: 12px;
        background: state == ApiKeyState.Valid ? Theme.green :
                    state == ApiKeyState.Invalid ? Theme.red :
                    state == ApiKeyState.Validating ? Theme.yellow :
                    (touch.has-hover ? Theme.surface2 : Theme.surface1);

        Text {
            text: state == ApiKeyState.Valid ? "OK" :
                  state == ApiKeyState.Invalid ? "X" :
                  state == ApiKeyState.Validating ? "..." :
                  "?";
            font-size: 10px;
            font-weight: 700;
            color: state == ApiKeyState.Unknown ? Theme.overlay0 : Theme.crust;
            horizontal-alignment: center;
            vertical-alignment: center;
        }

        touch := TouchArea {
            mouse-cursor: state == ApiKeyState.Unknown ? pointer : default;
            clicked => {
                if state == ApiKeyState.Unknown {
                    clicked();
                }
            }
        }
    }

    // Log viewer component
    component LogViewer inherits Rectangle {
        in property <string> log_text;

        background: Theme.mantle;
        border-radius: 8px;
        clip: true;

        VerticalLayout {
            padding: 12px;

            Rectangle {
                clip: true;

                ScrollView {
                    viewport-height: log-content.preferred-height;

                    log-content := Text {
                        text: log_text;
                        font-size: 12px;
                        font-family: "monospace";
                        color: Theme.subtext0;
                        wrap: word-wrap;
                        width: parent.width - 24px;
                        overflow: elide;
                    }
                }
            }
        }
    }

    // Main Window Component
    export component MainWindow inherits Window {
        title: "CLF3 - Modlist Installer";
        min-width: 1200px;
        min-height: 640px;
        background: Theme.base;

        // Input properties
        in-out property <InstallMode> mode: InstallMode.Wabbajack;
        in-out property <string> source_path: "";
        in-out property <string> install_dir: "";
        in-out property <string> downloads_dir: "";
        in-out property <string> nexus_api_key: "";
        in-out property <bool> non_premium_mode: false;

        // State properties
        in-out property <InstallState> install_state: InstallState.Idle;
        in-out property <ApiKeyState> api_key_state: ApiKeyState.Unknown;
        in-out property <float> progress: 0.0;
        in-out property <string> status_message: "Ready to install";
        in-out property <string> log_text: "CLF3 - Modlist Installer initialized.\nSelect a modlist or collection to begin.";
        in-out property <string> version: "0.0.5";

        // Activity section properties
        in-out property <string> current_download_file: "";
        in-out property <float> current_download_progress: 0.0;
        in-out property <string> download_speed: "0 B/s";
        in-out property <string> download_eta: "--:--";
        in-out property <string> size_progress: "0 B / 0 B";
        in-out property <int> files_completed: 0;
        in-out property <int> files_total: 0;

        // Detected game info (auto-populated when .wabbajack file is selected)
        in-out property <string> detected_game: "";

        // TTW (Tale of Two Wastelands) properties
        in-out property <bool> ttw_required: false;
        in-out property <string> ttw_mpi_path: "";
        in-out property <bool> ttw_fo3_found: false;
        in-out property <bool> ttw_fnv_found: false;
        in-out property <string> ttw_fo3_path: "";
        in-out property <string> ttw_fnv_path: "";

        // Proton selection (required for installation)
        in-out property <[ProtonOption]> proton_options: [];
        in-out property <[string]> proton_names: [];  // String model for ComboBox
        in-out property <int> selected_proton_index: -1;  // -1 = none selected

        // Computed properties
        property <bool> ttw_ready: !ttw_required || (ttw_mpi_path != "" && ttw_fo3_found && ttw_fnv_found);
        property <bool> can_install: source_path != "" && install_dir != "" &&
                                     downloads_dir != "" && nexus_api_key != "" &&
                                     selected_proton_index >= 0 && ttw_ready &&
                                     (install_state == InstallState.Idle || install_state == InstallState.Error);
        property <bool> is_running: install_state != InstallState.Idle &&
                                    install_state != InstallState.Complete &&
                                    install_state != InstallState.Error &&
                                    install_state != InstallState.Cancelled;

        // Callbacks
        callback browse_source();
        callback browse_install();
        callback browse_downloads();
        callback start_install();
        callback cancel_install();
        callback validate_api_key(string);
        callback open_api_key_page();
        callback open_settings();
        callback browse_modlists();
        callback source_edited(string);
        callback install_edited(string);
        callback downloads_edited(string);
        callback api_key_edited(string);
        callback browse_ttw_mpi();
        callback ttw_mpi_edited(string);

        VerticalLayout {
            padding: 0;

            // Main content area with two panels
            HorizontalLayout {
                padding: 8px;
                spacing: 12px;

                // Left Panel - Configuration
                Rectangle {
                    horizontal-stretch: 1;
                    max-width: 560px;
                    background: transparent;
                    clip: true;

                    VerticalLayout {
                        spacing: 4px;

                        // Header (compact)
                        HorizontalLayout {
                            spacing: 6px;
                            Text {
                                text: "CLF3";
                                font-size: 16px;
                                font-weight: 700;
                                color: Theme.text;
                            }
                        }

                        // Proton Selection Dropdown (compact inline)
                        HorizontalLayout {
                            spacing: 6px;

                            Text {
                                text: "Proton:";
                                font-size: 11px;
                                font-weight: 500;
                                color: Theme.subtext1;
                                vertical-alignment: center;
                                width: 90px;
                            }

                            if proton_names.length > 0: ComboBox {
                                horizontal-stretch: 1;
                                height: 26px;
                                model: proton_names;
                                current-index <=> selected_proton_index;
                                enabled: !is_running;
                            }

                            // No Protons warning
                            if proton_names.length == 0: Rectangle {
                                horizontal-stretch: 1;
                                height: 26px;
                                background: #3b1f2b;
                                border-radius: 4px;
                                border-width: 1px;
                                border-color: Theme.red;

                                Text {
                                    text: "No Proton 10+ found";
                                    font-size: 10px;
                                    color: Theme.red;
                                    horizontal-alignment: center;
                                    vertical-alignment: center;
                                }
                            }
                        }

                        // Source input (Wabbajack only)
                        PathInput {
                            label: "Wabbajack File";
                            placeholder: "/path/to/modlist.wabbajack";
                            value <=> source_path;
                            enabled: !is_running;
                            browse-clicked => { browse_source(); }
                            edited(text) => { source_edited(text); }
                        }

                        // Browse modlists gallery button
                        Rectangle {
                            height: 26px;
                            background: gallery_btn.has-hover ? #45475a : #313244;
                            border-radius: 4px;

                            gallery_btn := TouchArea {
                                enabled: !is_running;
                                clicked => { browse_modlists(); }
                            }

                            HorizontalLayout {
                                padding-left: 10px;
                                padding-right: 10px;
                                spacing: 6px;

                                Text {
                                    text: "Browse Modlist Gallery";
                                    font-size: 11px;
                                    color: #89b4fa;
                                    vertical-alignment: center;
                                    horizontal-stretch: 1;
                                }

                                Text {
                                    text: ">";
                                    font-size: 12px;
                                    color: #6c7086;
                                    vertical-alignment: center;
                                }
                            }
                        }

                        // Detected game indicator (shown when game is auto-detected)
                        if detected_game != "": Rectangle {
                            height: 20px;
                            background: Theme.surface0;
                            border-radius: 3px;

                            HorizontalLayout {
                                padding-left: 6px;
                                padding-right: 6px;
                                spacing: 4px;

                                Text {
                                    text: "Detected:";
                                    color: Theme.subtext0;
                                    font-size: 10px;
                                    vertical-alignment: center;
                                }

                                Text {
                                    text: detected_game;
                                    color: Theme.green;
                                    font-size: 10px;
                                    font-weight: 500;
                                    vertical-alignment: center;
                                }
                            }
                        }

                        // TTW Required notification (compact single-line when MPI set)
                        if ttw_required: Rectangle {
                            height: ttw_mpi_path == "" ? 44px : 20px;
                            background: #2d1f1f;
                            border-radius: 3px;
                            border-width: 1px;
                            border-color: Theme.peach;

                            VerticalLayout {
                                padding-left: 6px;
                                padding-right: 6px;
                                padding-top: 3px;
                                padding-bottom: 3px;
                                spacing: 2px;

                                // Header + Status on one line
                                HorizontalLayout {
                                    spacing: 8px;

                                    Text {
                                        text: "⚠ TTW";
                                        font-size: 10px;
                                        font-weight: 600;
                                        color: Theme.peach;
                                        vertical-alignment: center;
                                    }

                                    Text {
                                        text: (ttw_fo3_found ? "✓" : "✗") + "FO3";
                                        font-size: 9px;
                                        color: ttw_fo3_found ? Theme.green : Theme.red;
                                        vertical-alignment: center;
                                    }

                                    Text {
                                        text: (ttw_fnv_found ? "✓" : "✗") + "FNV";
                                        font-size: 9px;
                                        color: ttw_fnv_found ? Theme.green : Theme.red;
                                        vertical-alignment: center;
                                    }

                                    Text {
                                        text: (ttw_mpi_path != "" ? "✓" : "✗") + "MPI";
                                        font-size: 9px;
                                        color: ttw_mpi_path != "" ? Theme.green : Theme.red;
                                        vertical-alignment: center;
                                    }

                                    // Show path when set
                                    if ttw_mpi_path != "": Text {
                                        text: ttw_mpi_path;
                                        font-size: 8px;
                                        color: Theme.subtext0;
                                        overflow: elide;
                                        horizontal-stretch: 1;
                                        vertical-alignment: center;
                                    }
                                }

                                // MPI input row (only when not set)
                                if ttw_mpi_path == "": HorizontalLayout {
                                    spacing: 4px;

                                    Rectangle {
                                        horizontal-stretch: 1;
                                        height: 20px;
                                        background: Theme.crust;
                                        border-radius: 3px;

                                        mpi_input := TextInput {
                                            x: 5px;
                                            y: (parent.height - 10px) / 2;
                                            width: parent.width - 10px;
                                            height: 10px;
                                            text <=> ttw_mpi_path;
                                            font-size: 9px;
                                            color: Theme.text;
                                            enabled: !is_running;
                                            single-line: true;
                                            edited => { ttw_mpi_edited(self.text); }
                                        }
                                    }

                                    Rectangle {
                                        width: 44px;
                                        height: 20px;
                                        background: mpi_browse.has-hover ? Theme.surface1 : Theme.surface0;
                                        border-radius: 3px;

                                        mpi_browse := TouchArea {
                                            enabled: !is_running;
                                            clicked => { browse_ttw_mpi(); }
                                        }

                                        Text {
                                            text: "Browse";
                                            font-size: 9px;
                                            color: Theme.text;
                                            horizontal-alignment: center;
                                            vertical-alignment: center;
                                        }
                                    }
                                }
                            }
                        }

                        // Separator
                        Rectangle {
                            height: 1px;
                            background: Theme.surface0;
                        }

                        // Path configuration section
                        Text {
                            text: "Paths";
                            font-size: 12px;
                            font-weight: 600;
                            color: Theme.subtext0;
                        }

                        PathInput {
                            label: "Install Directory";
                            placeholder: "/path/to/install/location";
                            value <=> install_dir;
                            enabled: !is_running;
                            browse-clicked => { browse_install(); }
                            edited(text) => { install_edited(text); }
                        }

                        PathInput {
                            label: "Downloads Directory";
                            placeholder: "/path/to/downloads";
                            value <=> downloads_dir;
                            enabled: !is_running;
                            browse-clicked => { browse_downloads(); }
                            edited(text) => { downloads_edited(text); }
                        }

                        // Separator
                        Rectangle {
                            height: 1px;
                            background: Theme.surface0;
                        }

                        // Options section
                        Text {
                            text: "Options";
                            font-size: 12px;
                            font-weight: 600;
                            color: Theme.subtext0;
                        }

                        // API Key status (configured in Settings)
                        HorizontalLayout {
                            spacing: 8px;
                            alignment: start;

                            Text {
                                text: "Nexus API Key:";
                                font-size: 12px;
                                color: Theme.subtext0;
                                vertical-alignment: center;
                            }

                            if nexus_api_key != "": Rectangle {
                                width: 18px;
                                height: 18px;
                                border-radius: 9px;
                                background: api_key_state == ApiKeyState.Valid ? Theme.green :
                                            api_key_state == ApiKeyState.Invalid ? Theme.red :
                                            api_key_state == ApiKeyState.Validating ? Theme.yellow :
                                            Theme.surface1;

                                Text {
                                    text: api_key_state == ApiKeyState.Valid ? "✓" :
                                          api_key_state == ApiKeyState.Invalid ? "✗" :
                                          api_key_state == ApiKeyState.Validating ? "..." : "?";
                                    font-size: 10px;
                                    font-weight: 700;
                                    color: Theme.crust;
                                    horizontal-alignment: center;
                                    vertical-alignment: center;
                                }
                            }

                            Text {
                                text: nexus_api_key != "" ?
                                      (api_key_state == ApiKeyState.Valid ? "Valid" :
                                       api_key_state == ApiKeyState.Invalid ? "Invalid" :
                                       api_key_state == ApiKeyState.Validating ? "Checking..." : "Configured")
                                      : "Not set — configure in Settings";
                                font-size: 12px;
                                color: nexus_api_key != "" ?
                                       (api_key_state == ApiKeyState.Valid ? Theme.green :
                                        api_key_state == ApiKeyState.Invalid ? Theme.red : Theme.subtext0)
                                       : Theme.yellow;
                                vertical-alignment: center;
                            }
                        }

                        HorizontalLayout {
                            spacing: 10px;
                            alignment: start;

                            CheckBox {
                                text: "Non-Premium Mode (browser downloads)";
                                checked <=> non_premium_mode;
                                enabled: !is_running;
                            }
                        }

                        // Spacer
                        Rectangle {
                            vertical-stretch: 1;
                        }

                        // Action buttons
                        HorizontalLayout {
                            spacing: 8px;

                            SecondaryButton {
                                horizontal-stretch: 1;
                                label: "Cancel";
                                enabled: is_running;
                                clicked => { cancel_install(); }
                            }

                            PrimaryButton {
                                horizontal-stretch: 2;
                                label: is_running ? "Installing..." : "Start Installation";
                                enabled: can_install;
                                clicked => { start_install(); }
                            }
                        }
                    }
                }

                // Vertical divider
                Rectangle {
                    width: 1px;
                    background: Theme.surface0;
                }

                // Right Panel - Activity/Log
                Rectangle {
                    horizontal-stretch: 1;
                    background: transparent;
                    clip: true;

                    VerticalLayout {
                        spacing: 6px;

                        // Header
                        Text {
                            text: "Activity";
                            font-size: 14px;
                            font-weight: 600;
                            color: Theme.text;
                        }

                        // Unified Activity Progress section
                        Rectangle {
                            height: 130px;
                            background: Theme.surface0;
                            border-radius: 6px;

                            VerticalLayout {
                                padding: 8px;
                                spacing: 6px;

                                // Header row: Phase status + File count badge
                                HorizontalLayout {
                                    // Current phase/status
                                    Text {
                                        text: status_message;
                                        font-size: 12px;
                                        font-weight: 600;
                                        color: install_state == InstallState.Error ? Theme.red :
                                               install_state == InstallState.Complete ? Theme.green :
                                               Theme.text;
                                        horizontal-stretch: 1;
                                        overflow: elide;
                                    }

                                    // File count badge (dynamic width)
                                    Rectangle {
                                        height: 20px;
                                        background: Theme.surface1;
                                        border-radius: 3px;

                                        HorizontalLayout {
                                            padding-left: 8px;
                                            padding-right: 8px;

                                            Text {
                                                text: files_completed + "/" + files_total;
                                                font-size: 10px;
                                                font-weight: 500;
                                                color: Theme.blue;
                                                vertical-alignment: center;
                                            }
                                        }
                                    }
                                }

                                // Progress bar with percentage
                                Rectangle {
                                    height: 16px;
                                    background: Theme.surface1;
                                    border-radius: 4px;

                                    Rectangle {
                                        x: 0;
                                        y: 0;
                                        width: parent.width * clamp(progress, 0.0, 1.0);
                                        height: parent.height;
                                        background: install_state == InstallState.Error ? Theme.red :
                                                    install_state == InstallState.Complete ? Theme.green :
                                                    Theme.blue;
                                        border-radius: 4px;
                                    }

                                    Text {
                                        text: round(progress * 100) + "%";
                                        font-size: 10px;
                                        font-weight: 600;
                                        color: Theme.text;
                                        horizontal-alignment: center;
                                        vertical-alignment: center;
                                    }
                                }

                                // Stats row: Speed, Size, and ETA
                                HorizontalLayout {
                                    spacing: 12px;

                                    HorizontalLayout {
                                        spacing: 3px;
                                        Text {
                                            text: "Speed:";
                                            font-size: 10px;
                                            color: Theme.overlay0;
                                        }
                                        Text {
                                            text: download_speed;
                                            font-size: 10px;
                                            font-weight: 500;
                                            color: Theme.teal;
                                        }
                                    }

                                    HorizontalLayout {
                                        spacing: 3px;
                                        horizontal-stretch: 1;
                                        Text {
                                            text: size_progress;
                                            font-size: 10px;
                                            font-weight: 500;
                                            color: Theme.lavender;
                                        }
                                    }

                                    HorizontalLayout {
                                        spacing: 3px;
                                        Text {
                                            text: "ETA:";
                                            font-size: 10px;
                                            color: Theme.overlay0;
                                        }
                                        Text {
                                            text: download_eta;
                                            font-size: 10px;
                                            font-weight: 500;
                                            color: Theme.yellow;
                                        }
                                    }

                                }
                            }
                        }

                        // Log header
                        HorizontalLayout {
                            Text {
                                text: "Log";
                                font-size: 12px;
                                font-weight: 500;
                                color: Theme.subtext0;
                                horizontal-stretch: 1;
                            }
                        }

                        // Log viewer
                        LogViewer {
                            vertical-stretch: 1;
                            log_text: log_text;
                        }
                    }
                }
            }

            // Footer
            Rectangle {
                height: 28px;
                background: Theme.mantle;

                HorizontalLayout {
                    padding-left: 12px;
                    padding-right: 12px;
                    spacing: 10px;

                    Text {
                        text: "CLF3 v" + version;
                        font-size: 11px;
                        color: Theme.overlay0;
                        vertical-alignment: center;
                    }

                    Rectangle {
                        horizontal-stretch: 1;
                    }

                    LinkButton {
                        label: "Settings";
                        clicked => { open_settings(); }
                    }
                }
            }
        }
    }
}

// Settings dialog component
slint::slint! {
    import { Button, ComboBox, ScrollView } from "std-widgets.slint";

    // GPU info for dropdown
    export struct GpuOption {
        index: int,
        name: string,
    }

    // Settings dialog window
    export component SettingsDialog inherits Window {
        title: "CLF3 Settings";
        min-width: 500px;
        min-height: 420px;
        background: #1e1e2e;

        // Settings values
        in-out property <string> default_install_dir: "";
        in-out property <string> default_downloads_dir: "";
        in-out property <string> nexus_api_key: "";
        in-out property <[GpuOption]> gpu_options: [];
        in-out property <int> selected_gpu_index: -1;

        // Callbacks
        callback browse_install();
        callback browse_downloads();
        callback save_settings();
        callback cancel_settings();
        callback gpu_selected(int);

        VerticalLayout {
            padding: 20px;
            spacing: 16px;

            // Title
            Text {
                text: "Settings";
                font-size: 24px;
                font-weight: 700;
                color: #cdd6f4;
            }

            Text {
                text: "Configure default paths and preferences";
                font-size: 13px;
                color: #a6adc8;
            }

            // Separator
            Rectangle {
                height: 1px;
                background: #313244;
            }

            // Default Install Directory
            VerticalLayout {
                spacing: 6px;

                Text {
                    text: "Default Install Directory";
                    font-size: 13px;
                    font-weight: 500;
                    color: #bac2de;
                }

                HorizontalLayout {
                    spacing: 8px;

                    Rectangle {
                        horizontal-stretch: 1;
                        height: 36px;
                        background: #11111b;
                        border-radius: 6px;

                        Text {
                            x: 12px;
                            text: default_install_dir == "" ? "Not set" : default_install_dir;
                            font-size: 13px;
                            color: default_install_dir == "" ? #6c7086 : #cdd6f4;
                            vertical-alignment: center;
                            overflow: elide;
                        }
                    }

                    Rectangle {
                        width: 70px;
                        height: 36px;
                        background: #313244;
                        border-radius: 6px;

                        Text {
                            text: "Browse";
                            font-size: 13px;
                            color: #89b4fa;
                            horizontal-alignment: center;
                            vertical-alignment: center;
                        }

                        TouchArea {
                            clicked => { browse_install(); }
                        }
                    }
                }
            }

            // Default Downloads Directory
            VerticalLayout {
                spacing: 6px;

                Text {
                    text: "Default Downloads Directory";
                    font-size: 13px;
                    font-weight: 500;
                    color: #bac2de;
                }

                HorizontalLayout {
                    spacing: 8px;

                    Rectangle {
                        horizontal-stretch: 1;
                        height: 36px;
                        background: #11111b;
                        border-radius: 6px;

                        Text {
                            x: 12px;
                            text: default_downloads_dir == "" ? "Not set" : default_downloads_dir;
                            font-size: 13px;
                            color: default_downloads_dir == "" ? #6c7086 : #cdd6f4;
                            vertical-alignment: center;
                            overflow: elide;
                        }
                    }

                    Rectangle {
                        width: 70px;
                        height: 36px;
                        background: #313244;
                        border-radius: 6px;

                        Text {
                            text: "Browse";
                            font-size: 13px;
                            color: #89b4fa;
                            horizontal-alignment: center;
                            vertical-alignment: center;
                        }

                        TouchArea {
                            clicked => { browse_downloads(); }
                        }
                    }
                }
            }

            // API Key
            VerticalLayout {
                spacing: 6px;

                Text {
                    text: "Nexus Mods API Key";
                    font-size: 13px;
                    font-weight: 500;
                    color: #bac2de;
                }

                Rectangle {
                    height: 36px;
                    background: #11111b;
                    border-radius: 6px;
                    clip: true;

                    HorizontalLayout {
                        padding-left: 12px;
                        padding-right: 12px;

                        TextInput {
                            text <=> nexus_api_key;
                            font-size: 13px;
                            color: #cdd6f4;
                            vertical-alignment: center;
                            horizontal-stretch: 1;
                            input-type: password;
                        }
                    }
                }

                Text {
                    text: "Stored in ~/.config/clf3/settings.json";
                    font-size: 11px;
                    color: #6c7086;
                }
            }

            // GPU Selection
            VerticalLayout {
                spacing: 6px;

                Text {
                    text: "GPU for Texture Processing";
                    font-size: 13px;
                    font-weight: 500;
                    color: #bac2de;
                }

                Rectangle {
                    height: 36px;
                    background: #11111b;
                    border-radius: 6px;

                    HorizontalLayout {
                        padding-left: 12px;
                        padding-right: 12px;

                        Text {
                            text: selected_gpu_index < 0 ? "Auto-select (recommended)" :
                                  (selected_gpu_index < gpu_options.length ? gpu_options[selected_gpu_index].name : "Unknown");
                            font-size: 13px;
                            color: selected_gpu_index < 0 ? #a6adc8 : #cdd6f4;
                            vertical-alignment: center;
                            overflow: elide;
                        }
                    }
                }

                // GPU list (simple clickable items)
                for gpu[idx] in gpu_options: Rectangle {
                    height: 32px;
                    background: selected_gpu_index == gpu.index ? #313244 : transparent;
                    border-radius: 4px;

                    HorizontalLayout {
                        padding-left: 12px;
                        padding-top: 8px;
                        padding-bottom: 8px;
                        spacing: 10px;

                        VerticalLayout {
                            alignment: center;
                            Rectangle {
                                width: 16px;
                                height: 16px;
                                border-radius: 8px;
                                border-width: 2px;
                                border-color: #89b4fa;
                                background: selected_gpu_index == gpu.index ? #89b4fa : transparent;
                            }
                        }

                        Text {
                            text: gpu.name;
                            font-size: 12px;
                            color: #cdd6f4;
                            vertical-alignment: center;
                        }
                    }

                    TouchArea {
                        clicked => { gpu_selected(gpu.index); }
                    }
                }

                // Auto-select option
                Rectangle {
                    height: 32px;
                    background: selected_gpu_index < 0 ? #313244 : transparent;
                    border-radius: 4px;

                    HorizontalLayout {
                        padding-left: 12px;
                        padding-top: 8px;
                        padding-bottom: 8px;
                        spacing: 10px;

                        VerticalLayout {
                            alignment: center;
                            Rectangle {
                                width: 16px;
                                height: 16px;
                                border-radius: 8px;
                                border-width: 2px;
                                border-color: #89b4fa;
                                background: selected_gpu_index < 0 ? #89b4fa : transparent;
                            }
                        }

                        Text {
                            text: "Auto-select best GPU";
                            font-size: 12px;
                            color: #a6adc8;
                            vertical-alignment: center;
                        }
                    }

                    TouchArea {
                        clicked => { gpu_selected(-1); }
                    }
                }
            }

            // Spacer
            Rectangle {
                vertical-stretch: 1;
            }

            // Buttons
            HorizontalLayout {
                spacing: 12px;

                Rectangle {
                    horizontal-stretch: 1;
                }

                Rectangle {
                    width: 100px;
                    height: 40px;
                    background: #313244;
                    border-radius: 8px;

                    Text {
                        text: "Cancel";
                        font-size: 14px;
                        font-weight: 500;
                        color: #cdd6f4;
                        horizontal-alignment: center;
                        vertical-alignment: center;
                    }

                    TouchArea {
                        clicked => { cancel_settings(); }
                    }
                }

                Rectangle {
                    width: 100px;
                    height: 40px;
                    background: #89b4fa;
                    border-radius: 8px;

                    Text {
                        text: "Save";
                        font-size: 14px;
                        font-weight: 600;
                        color: #1e1e2e;
                        horizontal-alignment: center;
                        vertical-alignment: center;
                    }

                    TouchArea {
                        clicked => { save_settings(); }
                    }
                }
            }
        }
    }
}

// Modlist Browser Dialog for browsing and downloading modlists
slint::slint! {
    import { Button, ScrollView, ProgressIndicator } from "std-widgets.slint";

    // Modlist info for display in list
    export struct ModlistInfo {
        index: int,
        machine_name: string,
        title: string,
        author: string,
        game: string,
        game_internal: string,  // Internal game name for filtering (e.g., "skyrimspecialedition")
        download_size: string,
        install_size: string,
        description: string,
        is_nsfw: bool,
        is_official: bool,
        image: image,
        has_image: bool,
        visible: bool,
    }

    // Modlist Browser Dialog
    export component ModlistBrowserDialog inherits Window {
        title: "Browse Modlists";
        min-width: 900px;
        min-height: 750px;
        max-width: 1400px;
        max-height: 1000px;
        background: #1e1e2e;

        // State
        in-out property <string> search_text: "";
        in-out property <string> selected_game: "All Games";
        in-out property <[string]> game_list: ["All Games"];
        in-out property <int> game_index: 0;
        in-out property <[ModlistInfo]> modlists: [];
        in-out property <bool> is_loading: true;
        in-out property <string> status_message: "Loading modlists...";
        in-out property <int> selected_index: -1;
        in-out property <bool> is_downloading: false;
        in-out property <float> download_progress: 0.0;
        in-out property <bool> show_unofficial: true;
        in-out property <bool> show_nsfw: false;
        in-out property <bool> game_dropdown_open: false;
        in-out property <int> visible_count: 0;  // Count of visible modlists for viewport height

        // Callbacks
        callback search_changed(string);
        callback game_filter_changed(string);
        callback filter_changed(bool, bool);
        callback select_modlist(int);
        callback cancel();
        callback refresh();
        callback load_visible_images();

        VerticalLayout {
            padding: 20px;
            spacing: 16px;

            // Header
            HorizontalLayout {
                spacing: 12px;

                Text {
                    text: "Browse Modlists";
                    font-size: 24px;
                    font-weight: 700;
                    color: #cdd6f4;
                    vertical-alignment: center;
                }

                Rectangle { horizontal-stretch: 1; }

                // Refresh button
                Rectangle {
                    width: 36px;
                    height: 36px;
                    background: refresh_touch.has-hover ? #45475a : #313244;
                    border-radius: 6px;

                    refresh_touch := TouchArea {
                        enabled: !is_loading && !is_downloading;
                        clicked => { refresh(); }
                    }

                    Text {
                        text: "↻";
                        font-size: 18px;
                        color: #cdd6f4;
                        horizontal-alignment: center;
                        vertical-alignment: center;
                    }
                }
            }

            // Search and filter row
            HorizontalLayout {
                spacing: 12px;

                // Search box
                Rectangle {
                    horizontal-stretch: 2;
                    height: 40px;
                    background: #11111b;
                    border-radius: 8px;

                    HorizontalLayout {
                        padding-left: 12px;
                        padding-right: 12px;
                        spacing: 8px;

                        Text {
                            text: "🔍";
                            font-size: 14px;
                            color: #6c7086;
                            vertical-alignment: center;
                        }

                        search_input := TextInput {
                            text <=> search_text;
                            font-size: 14px;
                            color: #cdd6f4;
                            vertical-alignment: center;
                            horizontal-stretch: 1;
                            enabled: !is_loading && !is_downloading;
                            accepted => { search_changed(self.text); }
                            edited => { search_changed(self.text); }
                        }
                    }
                }

                // Game filter dropdown
                Rectangle {
                    width: 220px;
                    height: 40px;
                    background: game_dropdown_btn.has-hover ? #252536 : #11111b;
                    border-radius: 8px;

                    game_dropdown_btn := TouchArea {
                        enabled: !is_loading && !is_downloading;
                        clicked => {
                            game_dropdown_open = !game_dropdown_open;
                        }
                    }

                    HorizontalLayout {
                        padding-left: 12px;
                        padding-right: 12px;

                        Text {
                            text: selected_game;
                            font-size: 14px;
                            color: #cdd6f4;
                            vertical-alignment: center;
                            horizontal-stretch: 1;
                            overflow: elide;
                        }

                        Text {
                            text: game_dropdown_open ? "▲" : "▼";
                            font-size: 10px;
                            color: #6c7086;
                            vertical-alignment: center;
                        }
                    }
                }
            }

            // Filter checkboxes row
            HorizontalLayout {
                spacing: 24px;
                height: 30px;

                // Show Unofficial checkbox
                HorizontalLayout {
                    spacing: 8px;

                    Rectangle {
                        width: 20px;
                        height: 20px;
                        background: show_unofficial ? #89b4fa : #313244;
                        border-radius: 4px;
                        border-width: 1px;
                        border-color: #45475a;

                        unofficial_check := TouchArea {
                            clicked => {
                                show_unofficial = !show_unofficial;
                                filter_changed(show_unofficial, show_nsfw);
                            }
                        }

                        if show_unofficial: Text {
                            text: "✓";
                            font-size: 14px;
                            font-weight: 700;
                            color: #1e1e2e;
                            horizontal-alignment: center;
                            vertical-alignment: center;
                        }
                    }

                    Text {
                        text: "Show Unofficial";
                        font-size: 13px;
                        color: #cdd6f4;
                        vertical-alignment: center;
                    }
                }

                // Show NSFW checkbox
                HorizontalLayout {
                    spacing: 8px;

                    Rectangle {
                        width: 20px;
                        height: 20px;
                        background: show_nsfw ? #f38ba8 : #313244;
                        border-radius: 4px;
                        border-width: 1px;
                        border-color: #45475a;

                        nsfw_check := TouchArea {
                            clicked => {
                                show_nsfw = !show_nsfw;
                                filter_changed(show_unofficial, show_nsfw);
                            }
                        }

                        if show_nsfw: Text {
                            text: "✓";
                            font-size: 14px;
                            font-weight: 700;
                            color: #1e1e2e;
                            horizontal-alignment: center;
                            vertical-alignment: center;
                        }
                    }

                    Text {
                        text: "Show NSFW";
                        font-size: 13px;
                        color: #cdd6f4;
                        vertical-alignment: center;
                    }
                }

                Rectangle { horizontal-stretch: 1; }

                // Status/count
                Text {
                    text: is_loading ? status_message : modlists.length + " modlists";
                    font-size: 12px;
                    color: #6c7086;
                    vertical-alignment: center;
                }
            }

            // Main content area with game dropdown overlay
            Rectangle {
                vertical-stretch: 1;
                background: #1e1e2e;  // Match window background
                clip: true;

                // Modlist list (using Flickable to avoid scrollbar black box)
                modlist_scroll := Flickable {
                    x: 0;
                    y: 0;
                    width: parent.width;
                    height: parent.height;
                    viewport-height: is_loading ? 200px : (modlists.length * 228px + 16px);

                    VerticalLayout {
                        spacing: 8px;
                        padding-right: 8px;

                        // Loading indicator
                        if is_loading: Rectangle {
                            height: 200px;

                            Text {
                                text: status_message;
                                font-size: 16px;
                                color: #6c7086;
                                horizontal-alignment: center;
                                vertical-alignment: center;
                            }
                        }

                        // Modlist cards with images (only show visible ones based on filters)
                        for modlist[idx] in modlists: Rectangle {
                            visible: modlist.visible;
                            height: modlist.visible ? 220px : 0px;
                            background: selected_index == modlist.index ? #313244 : (card_touch.has-hover ? #252536 : #1e1e2e);
                            border-radius: 8px;
                            border-width: 1px;
                            border-color: selected_index == modlist.index ? #89b4fa : #313244;
                            clip: true;

                            card_touch := TouchArea {
                                enabled: !is_downloading && !game_dropdown_open;
                                clicked => {
                                    selected_index = modlist.index;
                                }
                            }

                            HorizontalLayout {
                                padding: 10px;
                                spacing: 12px;

                                // Left: Modlist image (320x200)
                                Rectangle {
                                    width: 320px;
                                    height: 200px;
                                    background: #11111b;
                                    border-radius: 6px;
                                    clip: true;

                                    if modlist.has_image: Image {
                                        source: modlist.image;
                                        width: parent.width;
                                        height: parent.height;
                                        image-fit: cover;
                                    }

                                    // Placeholder when no image
                                    if !modlist.has_image: Text {
                                        text: "🎮";
                                        font-size: 48px;
                                        color: #45475a;
                                        horizontal-alignment: center;
                                        vertical-alignment: center;
                                    }
                                }

                                // Middle: Title and info
                                Rectangle {
                                    horizontal-stretch: 1;
                                    height: 200px;
                                    clip: true;

                                    VerticalLayout {
                                        width: parent.width;
                                        spacing: 6px;
                                        padding-top: 4px;

                                    // Title row with badges
                                    HorizontalLayout {
                                        spacing: 8px;

                                        Text {
                                            text: modlist.title;
                                            font-size: 16px;
                                            font-weight: 600;
                                            color: #cdd6f4;
                                            overflow: elide;
                                            horizontal-stretch: 1;
                                        }

                                        if modlist.is_official: Rectangle {
                                            width: 55px;
                                            height: 18px;
                                            background: #89b4fa;
                                            border-radius: 4px;

                                            Text {
                                                text: "Official";
                                                font-size: 10px;
                                                font-weight: 600;
                                                color: #1e1e2e;
                                                horizontal-alignment: center;
                                                vertical-alignment: center;
                                            }
                                        }

                                        if modlist.is_nsfw: Rectangle {
                                            width: 45px;
                                            height: 18px;
                                            background: #f38ba8;
                                            border-radius: 4px;

                                            Text {
                                                text: "NSFW";
                                                font-size: 10px;
                                                font-weight: 600;
                                                color: #1e1e2e;
                                                horizontal-alignment: center;
                                                vertical-alignment: center;
                                            }
                                        }
                                    }

                                    // Author
                                    Text {
                                        text: "by " + modlist.author;
                                        font-size: 13px;
                                        color: #a6adc8;
                                    }

                                    // Description (limited height so bottom row stays visible)
                                    Rectangle {
                                        height: 90px;
                                        clip: true;

                                        Text {
                                            width: parent.width;
                                            text: modlist.description;
                                            font-size: 12px;
                                            color: #6c7086;
                                            wrap: word-wrap;
                                        }
                                    }

                                    // Bottom row: game tag and sizes
                                    HorizontalLayout {
                                        spacing: 16px;

                                        // Game tag
                                        Rectangle {
                                            height: 24px;
                                            width: game_text.preferred-width + 16px;
                                            background: #313244;
                                            border-radius: 4px;

                                            game_text := Text {
                                                text: modlist.game;
                                                font-size: 11px;
                                                color: #94e2d5;
                                                horizontal-alignment: center;
                                                vertical-alignment: center;
                                            }
                                        }

                                        // Download size
                                        HorizontalLayout {
                                            spacing: 4px;
                                            Text {
                                                text: "↓";
                                                font-size: 14px;
                                                color: #89b4fa;
                                                vertical-alignment: center;
                                            }
                                            Text {
                                                text: modlist.download_size;
                                                font-size: 12px;
                                                color: #89b4fa;
                                                vertical-alignment: center;
                                            }
                                        }

                                        // Install size
                                        HorizontalLayout {
                                            spacing: 4px;
                                            Text {
                                                text: "⛁";
                                                font-size: 14px;
                                                color: #a6e3a1;
                                                vertical-alignment: center;
                                            }
                                            Text {
                                                text: modlist.install_size;
                                                font-size: 12px;
                                                color: #a6e3a1;
                                                vertical-alignment: center;
                                            }
                                        }

                                        Rectangle { horizontal-stretch: 1; }
                                    }
                                    }
                                }

                                // Right: Select button
                                VerticalLayout {
                                    alignment: center;

                                    Rectangle {
                                        width: 80px;
                                        height: 36px;
                                        background: select_btn.has-hover ? #7aa2f7 : #89b4fa;
                                        border-radius: 6px;

                                        select_btn := TouchArea {
                                            enabled: !is_downloading && !game_dropdown_open;
                                            clicked => { select_modlist(modlist.index); }
                                        }

                                        Text {
                                            text: "Select";
                                            font-size: 13px;
                                            font-weight: 600;
                                            color: #1e1e2e;
                                            horizontal-alignment: center;
                                            vertical-alignment: center;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

            }

            // Bottom bar
            HorizontalLayout {
                height: 50px;
                spacing: 12px;

                // Download progress (shown when downloading)
                if is_downloading: Rectangle {
                    horizontal-stretch: 1;
                    background: #11111b;
                    border-radius: 8px;

                    VerticalLayout {
                        padding: 8px;
                        padding-left: 12px;
                        padding-right: 12px;
                        spacing: 4px;

                        HorizontalLayout {
                            spacing: 12px;

                            Text {
                                text: status_message != "" ? status_message : "Downloading...";
                                font-size: 12px;
                                color: #cdd6f4;
                                vertical-alignment: center;
                                horizontal-stretch: 1;
                            }

                            ProgressIndicator {
                                width: 150px;
                                progress: download_progress;
                            }
                        }

                        Text {
                            text: "Saving to Downloads folder";
                            font-size: 10px;
                            color: #6c7086;
                        }
                    }
                }

                // Spacer when not downloading
                if !is_downloading: Rectangle {
                    horizontal-stretch: 1;
                }

                // Cancel button
                Rectangle {
                    width: 100px;
                    height: 40px;
                    background: cancel_btn.has-hover ? #45475a : #313244;
                    border-radius: 6px;

                    cancel_btn := TouchArea {
                        clicked => { cancel(); }
                    }

                    Text {
                        text: "Cancel";
                        font-size: 14px;
                        color: #cdd6f4;
                        horizontal-alignment: center;
                        vertical-alignment: center;
                    }
                }
            }
        }

        // Game dropdown popup (overlay at Window root level - renders on top of everything)
        if game_dropdown_open: Rectangle {
            x: parent.width - 252px;
            y: 125px;  // Position below the dropdown button
            width: 232px;
            height: min(400px, game_list.length * 36px + 8px);
            background: #181825;
            border-radius: 8px;
            border-width: 1px;
            border-color: #45475a;
            drop-shadow-blur: 10px;
            drop-shadow-color: #00000080;
            clip: true;

            Flickable {
                width: parent.width;
                height: parent.height;
                viewport-height: game_list.length * 36px + 8px;

                VerticalLayout {
                    padding: 4px;

                    for game[g_idx] in game_list: Rectangle {
                        height: 36px;
                        background: game_item_touch.has-hover ? #313244 : transparent;
                        border-radius: 4px;

                        game_item_touch := TouchArea {
                            clicked => {
                                selected_game = game;
                                game_index = g_idx;
                                game_dropdown_open = false;
                                game_filter_changed(game);
                            }
                        }

                        Text {
                            x: 12px;
                            text: game;
                            font-size: 13px;
                            color: selected_game == game ? #89b4fa : #cdd6f4;
                            vertical-alignment: center;
                        }
                    }
                }
            }
        }
    }
}

// Download view component for NXM browser-based downloads
slint::slint! {
    import { Button, ProgressIndicator, VerticalBox, HorizontalBox, GroupBox } from "std-widgets.slint";

    // Download state enumeration
    export enum DownloadState {
        Idle,
        WaitingForBrowser,
        Downloading,
        Complete,
        Error,
    }

    // Mod information structure
    export struct ModInfo {
        mod_name: string,
        mod_id: int,
        file_id: int,
        file_size: string,
    }

    // Download View Component
    export component DownloadView inherits Window {
        title: "NXM Download Mode";
        min-width: 600px;
        min-height: 450px;
        background: #1e1e2e;

        // Properties
        in-out property <int> queue_position: 0;
        in-out property <int> queue_total: 0;
        in-out property <ModInfo> current_mod: {
            mod_name: "",
            mod_id: 0,
            file_id: 0,
            file_size: "0 B",
        };
        in-out property <DownloadState> state: DownloadState.Idle;
        in-out property <float> progress: 0.0;
        in-out property <string> status_message: "Ready";
        in-out property <string> error_message: "";
        in-out property <bool> has_error: false;

        // Callbacks
        callback register_nxm_handler();
        callback open_in_browser();
        callback skip_download();
        callback next_download();
        callback cancel_download();

        VerticalBox {
            padding: 20px;
            spacing: 15px;

            // Title
            Text {
                text: "NXM Download Mode";
                font-size: 24px;
                font-weight: 700;
                color: #cdd6f4;
                horizontal-alignment: center;
            }

            // Queue Progress Indicator
            HorizontalBox {
                alignment: center;
                spacing: 10px;

                Text {
                    text: "Queue Progress:";
                    font-size: 14px;
                    color: #a6adc8;
                    vertical-alignment: center;
                }

                Rectangle {
                    width: 80px;
                    height: 36px;
                    background: #313244;
                    border-radius: 6px;

                    Text {
                        text: queue_position + "/" + queue_total;
                        font-size: 18px;
                        font-weight: 600;
                        color: #89b4fa;
                        horizontal-alignment: center;
                        vertical-alignment: center;
                    }
                }
            }

            // Current Mod Info Panel
            GroupBox {
                title: "Current Download";

                VerticalBox {
                    padding: 10px;
                    spacing: 8px;

                    // Mod Name
                    HorizontalBox {
                        spacing: 10px;
                        Text {
                            text: "Mod Name:";
                            font-size: 13px;
                            color: #a6adc8;
                            min-width: 80px;
                        }
                        Text {
                            text: current_mod.mod_name == "" ? "-" : current_mod.mod_name;
                            font-size: 13px;
                            color: #cdd6f4;
                            overflow: elide;
                            horizontal-stretch: 1;
                        }
                    }

                    // Mod ID
                    HorizontalBox {
                        spacing: 10px;
                        Text {
                            text: "Mod ID:";
                            font-size: 13px;
                            color: #a6adc8;
                            min-width: 80px;
                        }
                        Text {
                            text: current_mod.mod_id == 0 ? "-" : current_mod.mod_id;
                            font-size: 13px;
                            color: #cdd6f4;
                        }
                    }

                    // File ID
                    HorizontalBox {
                        spacing: 10px;
                        Text {
                            text: "File ID:";
                            font-size: 13px;
                            color: #a6adc8;
                            min-width: 80px;
                        }
                        Text {
                            text: current_mod.file_id == 0 ? "-" : current_mod.file_id;
                            font-size: 13px;
                            color: #cdd6f4;
                        }
                    }

                    // File Size
                    HorizontalBox {
                        spacing: 10px;
                        Text {
                            text: "File Size:";
                            font-size: 13px;
                            color: #a6adc8;
                            min-width: 80px;
                        }
                        Text {
                            text: current_mod.file_size;
                            font-size: 13px;
                            color: #cdd6f4;
                        }
                    }
                }
            }

            // State Indicator
            Rectangle {
                height: 40px;
                background: state == DownloadState.WaitingForBrowser ? #45475a :
                            state == DownloadState.Downloading ? #313244 :
                            state == DownloadState.Complete ? #1e3a28 :
                            state == DownloadState.Error ? #3b1f2b :
                            #313244;
                border-radius: 8px;

                HorizontalBox {
                    padding-left: 15px;
                    padding-right: 15px;
                    alignment: center;
                    spacing: 10px;

                    // State Icon (represented as colored circle)
                    Rectangle {
                        width: 12px;
                        height: 12px;
                        border-radius: 6px;
                        background: state == DownloadState.Idle ? #6c7086 :
                                    state == DownloadState.WaitingForBrowser ? #f9e2af :
                                    state == DownloadState.Downloading ? #89b4fa :
                                    state == DownloadState.Complete ? #a6e3a1 :
                                    state == DownloadState.Error ? #f38ba8 :
                                    #6c7086;
                    }

                    Text {
                        text: state == DownloadState.Idle ? "Idle" :
                              state == DownloadState.WaitingForBrowser ? "Waiting for Browser..." :
                              state == DownloadState.Downloading ? "Downloading..." :
                              state == DownloadState.Complete ? "Complete" :
                              state == DownloadState.Error ? "Error" :
                              "Unknown";
                        font-size: 14px;
                        font-weight: 500;
                        color: #cdd6f4;
                    }
                }
            }

            // Progress Bar
            Rectangle {
                height: 24px;
                background: #313244;
                border-radius: 4px;

                Rectangle {
                    x: 0;
                    y: 0;
                    width: parent.width * clamp(progress, 0.0, 1.0);
                    height: parent.height;
                    background: state == DownloadState.Error ? #f38ba8 :
                                state == DownloadState.Complete ? #a6e3a1 :
                                #89b4fa;
                    border-radius: 4px;
                }

                Text {
                    text: round(progress * 100) + "%";
                    font-size: 12px;
                    font-weight: 600;
                    color: #cdd6f4;
                    horizontal-alignment: center;
                    vertical-alignment: center;
                }
            }

            // Status Message Area
            Rectangle {
                height: 30px;
                background: transparent;

                Text {
                    text: status_message;
                    font-size: 13px;
                    color: #a6adc8;
                    horizontal-alignment: center;
                    vertical-alignment: center;
                }
            }

            // Error Message Area (only visible when has_error is true)
            if has_error: Rectangle {
                height: 50px;
                background: #45303a;
                border-radius: 6px;
                border-width: 1px;
                border-color: #f38ba8;

                HorizontalBox {
                    padding: 10px;
                    spacing: 10px;

                    Text {
                        text: "Error:";
                        font-size: 13px;
                        font-weight: 600;
                        color: #f38ba8;
                        vertical-alignment: center;
                    }

                    Text {
                        text: error_message;
                        font-size: 13px;
                        color: #f5c2c7;
                        overflow: elide;
                        vertical-alignment: center;
                        horizontal-stretch: 1;
                    }
                }
            }

            // Spacer
            Rectangle {
                vertical-stretch: 1;
            }

            // Button Row
            HorizontalBox {
                spacing: 10px;
                alignment: center;

                Button {
                    text: "Register NXM Handler";
                    clicked => { register_nxm_handler(); }
                }

                Button {
                    text: "Open in Browser";
                    enabled: state == DownloadState.WaitingForBrowser || state == DownloadState.Idle;
                    clicked => { open_in_browser(); }
                }

                Button {
                    text: "Skip";
                    enabled: state != DownloadState.Idle;
                    clicked => { skip_download(); }
                }

                Button {
                    text: "Next";
                    enabled: state == DownloadState.Complete || state == DownloadState.Error;
                    clicked => { next_download(); }
                }

                Button {
                    text: "Cancel";
                    clicked => { cancel_download(); }
                }
            }
        }
    }
}

// Installation Progress View component for showing installation progress
slint::slint! {
    import { Button, VerticalBox, HorizontalBox, ScrollView } from "std-widgets.slint";

    // Installation phase enumeration
    export enum InstallPhase {
        Idle,
        Downloading,
        Extracting,
        Installing,
        Verifying,
        Complete,
        Error,
        Paused,
    }

    // Log entry type for color coding
    export enum LogLevel {
        Info,
        Success,
        Warning,
        Error,
    }

    // NXM browser mode state
    export struct NxmBrowserState {
        active: bool,
        mod_name: string,
        mod_id: int,
        file_id: int,
        waiting_for_browser: bool,
    }

    // Installation statistics
    export struct InstallStats {
        downloaded_bytes: string,
        total_bytes: string,
        extracted_count: int,
        total_extract: int,
        installed_count: int,
        total_install: int,
        download_speed: string,
        eta: string,
    }

    // Installation Progress View Component
    export component InstallProgressView inherits Window {
        title: "Installing Modlist";
        min-width: 800px;
        min-height: 650px;
        background: #1e1e2e;

        // Core properties
        in-out property <string> modlist_name: "Modlist";
        in-out property <InstallPhase> phase: InstallPhase.Idle;
        in-out property <string> current_file: "";
        in-out property <float> overall_progress: 0.0;
        in-out property <float> file_progress: 0.0;
        in-out property <int> current_file_index: 0;
        in-out property <int> total_files: 0;
        in-out property <string> log_text: "";
        in-out property <bool> can_cancel: true;
        in-out property <bool> is_paused: false;
        in-out property <string> error_message: "";
        in-out property <bool> has_error: false;

        // Statistics
        in-out property <InstallStats> stats: {
            downloaded_bytes: "0 B",
            total_bytes: "0 B",
            extracted_count: 0,
            total_extract: 0,
            installed_count: 0,
            total_install: 0,
            download_speed: "0 B/s",
            eta: "--:--",
        };

        // NXM browser mode
        in-out property <NxmBrowserState> nxm_state: {
            active: false,
            mod_name: "",
            mod_id: 0,
            file_id: 0,
            waiting_for_browser: false,
        };

        // Callbacks
        callback cancel();
        callback pause();
        callback resume();
        callback open_browser();
        callback skip_download();
        callback retry();

        VerticalBox {
            padding: 20px;
            spacing: 12px;

            // Header with modlist name and phase
            Rectangle {
                height: 60px;
                background: #313244;
                border-radius: 8px;

                HorizontalBox {
                    padding: 15px;
                    spacing: 15px;

                    VerticalBox {
                        spacing: 4px;
                        horizontal-stretch: 1;

                        Text {
                            text: "Installing: " + modlist_name;
                            font-size: 18px;
                            font-weight: 700;
                            color: #cdd6f4;
                            overflow: elide;
                        }

                        Text {
                            text: current_file_index + " of " + total_files + " files";
                            font-size: 12px;
                            color: #a6adc8;
                        }
                    }

                    // Phase indicator badge
                    Rectangle {
                        width: 140px;
                        height: 36px;
                        background: phase == InstallPhase.Downloading ? #1e3a5f :
                                    phase == InstallPhase.Extracting ? #3d3a1e :
                                    phase == InstallPhase.Installing ? #1e3a3a :
                                    phase == InstallPhase.Verifying ? #2e1e3a :
                                    phase == InstallPhase.Complete ? #1e3a28 :
                                    phase == InstallPhase.Error ? #3b1f2b :
                                    phase == InstallPhase.Paused ? #3a3a1e :
                                    #45475a;
                        border-radius: 18px;

                        HorizontalBox {
                            padding-left: 12px;
                            padding-right: 12px;
                            alignment: center;
                            spacing: 8px;

                            // Phase indicator dot
                            Rectangle {
                                width: 10px;
                                height: 10px;
                                border-radius: 5px;
                                background: phase == InstallPhase.Idle ? #6c7086 :
                                            phase == InstallPhase.Downloading ? #89b4fa :
                                            phase == InstallPhase.Extracting ? #f9e2af :
                                            phase == InstallPhase.Installing ? #94e2d5 :
                                            phase == InstallPhase.Verifying ? #cba6f7 :
                                            phase == InstallPhase.Complete ? #a6e3a1 :
                                            phase == InstallPhase.Error ? #f38ba8 :
                                            phase == InstallPhase.Paused ? #fab387 :
                                            #6c7086;
                            }

                            Text {
                                text: phase == InstallPhase.Idle ? "Idle" :
                                      phase == InstallPhase.Downloading ? "Downloading" :
                                      phase == InstallPhase.Extracting ? "Extracting" :
                                      phase == InstallPhase.Installing ? "Installing" :
                                      phase == InstallPhase.Verifying ? "Verifying" :
                                      phase == InstallPhase.Complete ? "Complete" :
                                      phase == InstallPhase.Error ? "Error" :
                                      phase == InstallPhase.Paused ? "Paused" :
                                      "Unknown";
                                font-size: 13px;
                                font-weight: 600;
                                color: phase == InstallPhase.Idle ? #6c7086 :
                                       phase == InstallPhase.Downloading ? #89b4fa :
                                       phase == InstallPhase.Extracting ? #f9e2af :
                                       phase == InstallPhase.Installing ? #94e2d5 :
                                       phase == InstallPhase.Verifying ? #cba6f7 :
                                       phase == InstallPhase.Complete ? #a6e3a1 :
                                       phase == InstallPhase.Error ? #f38ba8 :
                                       phase == InstallPhase.Paused ? #fab387 :
                                       #cdd6f4;
                            }
                        }
                    }
                }
            }

            // Current file section
            Rectangle {
                height: 80px;
                background: #313244;
                border-radius: 8px;

                VerticalBox {
                    padding: 12px;
                    spacing: 8px;

                    HorizontalBox {
                        spacing: 10px;

                        Text {
                            text: "Current:";
                            font-size: 12px;
                            color: #a6adc8;
                            min-width: 60px;
                            vertical-alignment: center;
                        }

                        Text {
                            text: current_file == "" ? "Waiting..." : current_file;
                            font-size: 13px;
                            color: #cdd6f4;
                            overflow: elide;
                            horizontal-stretch: 1;
                            vertical-alignment: center;
                        }

                        // Speed and ETA display
                        Rectangle {
                            width: 180px;
                            height: 24px;
                            background: #45475a;
                            border-radius: 4px;

                            HorizontalBox {
                                padding-left: 8px;
                                padding-right: 8px;
                                spacing: 8px;

                                Text {
                                    text: stats.download_speed;
                                    font-size: 11px;
                                    color: #89b4fa;
                                    vertical-alignment: center;
                                }

                                Rectangle {
                                    width: 1px;
                                    background: #6c7086;
                                }

                                Text {
                                    text: "ETA: " + stats.eta;
                                    font-size: 11px;
                                    color: #a6adc8;
                                    vertical-alignment: center;
                                }
                            }
                        }
                    }

                    // File progress bar
                    Rectangle {
                        height: 20px;
                        background: #45475a;
                        border-radius: 4px;

                        Rectangle {
                            x: 0;
                            y: 0;
                            width: parent.width * clamp(file_progress, 0.0, 1.0);
                            height: parent.height;
                            background: phase == InstallPhase.Error ? #f38ba8 :
                                        phase == InstallPhase.Paused ? #fab387 :
                                        #89b4fa;
                            border-radius: 4px;
                        }

                        Text {
                            text: round(file_progress * 100) + "%";
                            font-size: 11px;
                            font-weight: 600;
                            color: #cdd6f4;
                            horizontal-alignment: center;
                            vertical-alignment: center;
                        }
                    }
                }
            }

            // Overall progress section
            Rectangle {
                height: 70px;
                background: #313244;
                border-radius: 8px;

                VerticalBox {
                    padding: 12px;
                    spacing: 8px;

                    HorizontalBox {
                        Text {
                            text: "Overall Progress";
                            font-size: 13px;
                            font-weight: 600;
                            color: #cdd6f4;
                            horizontal-stretch: 1;
                        }

                        Text {
                            text: round(overall_progress * 100) + "%";
                            font-size: 13px;
                            font-weight: 600;
                            color: phase == InstallPhase.Complete ? #a6e3a1 :
                                   phase == InstallPhase.Error ? #f38ba8 :
                                   #94e2d5;
                        }
                    }

                    // Overall progress bar
                    Rectangle {
                        height: 24px;
                        background: #45475a;
                        border-radius: 6px;

                        Rectangle {
                            x: 0;
                            y: 0;
                            width: parent.width * clamp(overall_progress, 0.0, 1.0);
                            height: parent.height;
                            background: phase == InstallPhase.Complete ? #a6e3a1 :
                                        phase == InstallPhase.Error ? #f38ba8 :
                                        phase == InstallPhase.Paused ? #fab387 :
                                        #94e2d5;
                            border-radius: 6px;
                        }
                    }
                }
            }

            // Statistics row
            Rectangle {
                height: 50px;
                background: #313244;
                border-radius: 8px;

                HorizontalBox {
                    padding: 10px;
                    spacing: 20px;

                    // Downloaded stat
                    HorizontalBox {
                        horizontal-stretch: 1;
                        spacing: 6px;

                        Rectangle {
                            width: 8px;
                            height: 8px;
                            border-radius: 4px;
                            background: #89b4fa;
                            y: (parent.height - self.height) / 2;
                        }

                        VerticalBox {
                            spacing: 2px;
                            Text {
                                text: "Downloaded";
                                font-size: 11px;
                                color: #a6adc8;
                            }
                            Text {
                                text: stats.downloaded_bytes + " / " + stats.total_bytes;
                                font-size: 12px;
                                font-weight: 600;
                                color: #cdd6f4;
                            }
                        }
                    }

                    Rectangle {
                        width: 1px;
                        background: #45475a;
                    }

                    // Extracted stat
                    HorizontalBox {
                        horizontal-stretch: 1;
                        spacing: 6px;

                        Rectangle {
                            width: 8px;
                            height: 8px;
                            border-radius: 4px;
                            background: #f9e2af;
                            y: (parent.height - self.height) / 2;
                        }

                        VerticalBox {
                            spacing: 2px;
                            Text {
                                text: "Extracted";
                                font-size: 11px;
                                color: #a6adc8;
                            }
                            Text {
                                text: stats.extracted_count + " / " + stats.total_extract;
                                font-size: 12px;
                                font-weight: 600;
                                color: #cdd6f4;
                            }
                        }
                    }

                    Rectangle {
                        width: 1px;
                        background: #45475a;
                    }

                    // Installed stat
                    HorizontalBox {
                        horizontal-stretch: 1;
                        spacing: 6px;

                        Rectangle {
                            width: 8px;
                            height: 8px;
                            border-radius: 4px;
                            background: #a6e3a1;
                            y: (parent.height - self.height) / 2;
                        }

                        VerticalBox {
                            spacing: 2px;
                            Text {
                                text: "Installed";
                                font-size: 11px;
                                color: #a6adc8;
                            }
                            Text {
                                text: stats.installed_count + " / " + stats.total_install;
                                font-size: 12px;
                                font-weight: 600;
                                color: #cdd6f4;
                            }
                        }
                    }
                }
            }

            // NXM Browser Mode Panel (conditionally visible)
            if nxm_state.active: Rectangle {
                height: 90px;
                background: #45475a;
                border-radius: 8px;
                border-width: 1px;
                border-color: nxm_state.waiting_for_browser ? #f9e2af : #6c7086;

                VerticalBox {
                    padding: 12px;
                    spacing: 8px;

                    HorizontalBox {
                        spacing: 10px;

                        // Status indicator
                        Rectangle {
                            width: 10px;
                            height: 10px;
                            border-radius: 5px;
                            background: nxm_state.waiting_for_browser ? #f9e2af : #89b4fa;
                            y: (parent.height - self.height) / 2;

                            // Pulsing animation for waiting state
                            animate background {
                                duration: 1000ms;
                            }
                        }

                        Text {
                            text: nxm_state.waiting_for_browser ? "Waiting for Browser Download..." : "NXM Browser Mode";
                            font-size: 13px;
                            font-weight: 600;
                            color: nxm_state.waiting_for_browser ? #f9e2af : #cdd6f4;
                            horizontal-stretch: 1;
                            vertical-alignment: center;
                        }
                    }

                    HorizontalBox {
                        spacing: 15px;

                        VerticalBox {
                            spacing: 2px;
                            horizontal-stretch: 1;

                            Text {
                                text: "Mod: " + (nxm_state.mod_name == "" ? "Unknown" : nxm_state.mod_name);
                                font-size: 12px;
                                color: #cdd6f4;
                                overflow: elide;
                            }

                            Text {
                                text: "ID: " + nxm_state.mod_id + " | File: " + nxm_state.file_id;
                                font-size: 11px;
                                color: #a6adc8;
                            }
                        }

                        Button {
                            text: "Open in Browser";
                            enabled: nxm_state.waiting_for_browser;
                            clicked => { open_browser(); }
                        }

                        Button {
                            text: "Skip";
                            clicked => { skip_download(); }
                        }
                    }
                }
            }

            // Error panel (conditionally visible)
            if has_error: Rectangle {
                height: 60px;
                background: #45303a;
                border-radius: 8px;
                border-width: 1px;
                border-color: #f38ba8;

                HorizontalBox {
                    padding: 12px;
                    spacing: 12px;

                    Rectangle {
                        width: 24px;
                        height: 24px;
                        border-radius: 12px;
                        background: #f38ba8;

                        Text {
                            text: "!";
                            font-size: 16px;
                            font-weight: 700;
                            color: #1e1e2e;
                            horizontal-alignment: center;
                            vertical-alignment: center;
                        }
                    }

                    VerticalBox {
                        spacing: 2px;
                        horizontal-stretch: 1;

                        Text {
                            text: "Installation Error";
                            font-size: 13px;
                            font-weight: 600;
                            color: #f38ba8;
                        }

                        Text {
                            text: error_message;
                            font-size: 12px;
                            color: #f5c2c7;
                            overflow: elide;
                        }
                    }

                    Button {
                        text: "Retry";
                        clicked => { retry(); }
                    }
                }
            }

            // Activity log section
            Rectangle {
                vertical-stretch: 1;
                background: #313244;
                border-radius: 8px;

                VerticalBox {
                    padding: 12px;
                    spacing: 8px;

                    HorizontalBox {
                        Text {
                            text: "Activity Log";
                            font-size: 13px;
                            font-weight: 600;
                            color: #cdd6f4;
                            horizontal-stretch: 1;
                        }

                        // Log legend
                        HorizontalBox {
                            spacing: 12px;

                            HorizontalBox {
                                spacing: 4px;
                                Rectangle {
                                    width: 8px;
                                    height: 8px;
                                    border-radius: 4px;
                                    background: #a6adc8;
                                    y: (parent.height - self.height) / 2;
                                }
                                Text {
                                    text: "Info";
                                    font-size: 10px;
                                    color: #6c7086;
                                    vertical-alignment: center;
                                }
                            }

                            HorizontalBox {
                                spacing: 4px;
                                Rectangle {
                                    width: 8px;
                                    height: 8px;
                                    border-radius: 4px;
                                    background: #a6e3a1;
                                    y: (parent.height - self.height) / 2;
                                }
                                Text {
                                    text: "Success";
                                    font-size: 10px;
                                    color: #6c7086;
                                    vertical-alignment: center;
                                }
                            }

                            HorizontalBox {
                                spacing: 4px;
                                Rectangle {
                                    width: 8px;
                                    height: 8px;
                                    border-radius: 4px;
                                    background: #f38ba8;
                                    y: (parent.height - self.height) / 2;
                                }
                                Text {
                                    text: "Error";
                                    font-size: 10px;
                                    color: #6c7086;
                                    vertical-alignment: center;
                                }
                            }
                        }
                    }

                    // Scrollable log area
                    Rectangle {
                        vertical-stretch: 1;
                        background: #1e1e2e;
                        border-radius: 4px;

                        VerticalLayout {
                            padding: 8px;

                            ScrollView {
                                viewport-width: self.width;
                                viewport-height: log-text-content.preferred-height;

                                log-text-content := Text {
                                    text: log_text == "" ? "No activity yet..." : log_text;
                                    font-size: 11px;
                                    font-family: "monospace";
                                    color: #a6adc8;
                                    wrap: word-wrap;
                                    width: parent.viewport-width;
                                    horizontal-alignment: left;
                                    vertical-alignment: top;
                                }
                            }
                        }
                    }
                }
            }

            // Bottom button row
            Rectangle {
                height: 50px;
                background: #313244;
                border-radius: 8px;

                HorizontalBox {
                    padding: 8px;
                    padding-left: 15px;
                    padding-right: 15px;
                    spacing: 12px;

                    // Status text on left
                    Text {
                        text: phase == InstallPhase.Complete ? "Installation complete!" :
                              phase == InstallPhase.Error ? "Installation failed" :
                              phase == InstallPhase.Paused ? "Installation paused" :
                              "Installing...";
                        font-size: 13px;
                        color: phase == InstallPhase.Complete ? #a6e3a1 :
                               phase == InstallPhase.Error ? #f38ba8 :
                               phase == InstallPhase.Paused ? #fab387 :
                               #a6adc8;
                        vertical-alignment: center;
                        horizontal-stretch: 1;
                    }

                    // Pause/Resume button
                    Button {
                        text: is_paused ? "Resume" : "Pause";
                        enabled: phase != InstallPhase.Complete && phase != InstallPhase.Error && phase != InstallPhase.Idle;
                        clicked => {
                            if (is_paused) {
                                resume();
                            } else {
                                pause();
                            }
                        }
                    }

                    // Cancel button
                    Button {
                        text: phase == InstallPhase.Complete ? "Close" : "Cancel";
                        enabled: can_cancel;
                        clicked => { cancel(); }
                    }
                }
            }
        }
    }
}

// =============================================================================
// Progress Channel for GUI Updates
// =============================================================================

use once_cell::sync::Lazy;
use slint::Model;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Mutex;

/// Progress update messages sent from background installation thread to GUI
#[derive(Clone, Debug)]
pub enum ProgressUpdate {
    /// Installation phase changed ("Downloading", "Extracting", etc.)
    Phase(String),
    /// Status message to display
    Status(String),
    /// Current file name and progress (0-1)
    FileProgress(String, f32),
    /// Overall progress (0-1)
    OverallProgress(f32),
    /// File count (completed, total)
    FileCount(i32, i32),
    /// Download speed string ("5.2 MiB/s")
    DownloadSpeed(String),
    /// Size progress string ("1.5 GiB / 10.2 GiB")
    SizeProgress(String),
    /// ETA string ("12:34")
    Eta(String),
    /// Log message to append
    Log(String),
    /// Installation completed successfully
    Complete,
    /// Installation failed with error message
    Error(String),
}

/// Static channel for communication between background thread and GUI
static PROGRESS_CHANNEL: Lazy<(Mutex<Sender<ProgressUpdate>>, Mutex<Receiver<ProgressUpdate>>)> =
    Lazy::new(|| {
        let (tx, rx) = channel();
        (Mutex::new(tx), Mutex::new(rx))
    });

/// Get a clone of the progress sender for use in background threads
pub fn get_progress_sender() -> Sender<ProgressUpdate> {
    PROGRESS_CHANNEL.0.lock().unwrap().clone()
}

/// Normalize game name for display (add spaces, proper casing)
fn normalize_game_name(name: &str) -> String {
    // Known game name mappings for clean display
    let lower = name.to_lowercase();
    match lower.as_str() {
        "skyrimspecialedition" => "Skyrim Special Edition".to_string(),
        "skyrimvr" => "Skyrim VR".to_string(),
        "fallout4" => "Fallout 4".to_string(),
        "fallout4vr" => "Fallout 4 VR".to_string(),
        "falloutnewvegas" | "newvegas" => "Fallout New Vegas".to_string(),
        "fallout3" => "Fallout 3".to_string(),
        "oblivion" => "Oblivion".to_string(),
        "morrowind" => "Morrowind".to_string(),
        "starfield" => "Starfield".to_string(),
        "baldursgate3" => "Baldur's Gate 3".to_string(),
        "cyberpunk2077" => "Cyberpunk 2077".to_string(),
        "dragonageorigins" => "Dragon Age Origins".to_string(),
        "dragonage2" => "Dragon Age 2".to_string(),
        "dragonageinquisition" => "Dragon Age Inquisition".to_string(),
        "witcher3" | "thewitcher3" => "The Witcher 3".to_string(),
        "enderalspecialedition" | "enderal" => "Enderal".to_string(),
        "nomanssky" => "No Man's Sky".to_string(),
        "mountandblade2bannerlord" | "bannerlord" => "Mount & Blade II: Bannerlord".to_string(),
        "stardewvalley" => "Stardew Valley".to_string(),
        "darkestdungeon" => "Darkest Dungeon".to_string(),
        "kingdomcomedeliverance" => "Kingdom Come: Deliverance".to_string(),
        "dishonored" => "Dishonored".to_string(),
        _ => {
            // Fallback: capitalize first letter
            let mut chars = name.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => name.to_string(),
            }
        }
    }
}

/// Convert display game name back to internal format for filtering
fn denormalize_game_name(display_name: &str) -> String {
    // Reverse mapping from display names to internal format
    match display_name {
        "Skyrim Special Edition" => "skyrimspecialedition".to_string(),
        "Skyrim VR" => "skyrimvr".to_string(),
        "Fallout 4" => "fallout4".to_string(),
        "Fallout 4 VR" => "fallout4vr".to_string(),
        "Fallout New Vegas" => "falloutnewvegas".to_string(),
        "Fallout 3" => "fallout3".to_string(),
        "Oblivion" => "oblivion".to_string(),
        "Morrowind" => "morrowind".to_string(),
        "Starfield" => "starfield".to_string(),
        "Baldur's Gate 3" => "baldursgate3".to_string(),
        "Cyberpunk 2077" => "cyberpunk2077".to_string(),
        "Dragon Age Origins" => "dragonageorigins".to_string(),
        "Dragon Age 2" => "dragonage2".to_string(),
        "Dragon Age Inquisition" => "dragonageinquisition".to_string(),
        "The Witcher 3" => "witcher3".to_string(),
        "Enderal" => "enderalspecialedition".to_string(),
        "No Man's Sky" => "nomanssky".to_string(),
        "Mount & Blade II: Bannerlord" => "mountandblade2bannerlord".to_string(),
        "Stardew Valley" => "stardewvalley".to_string(),
        "Darkest Dungeon" => "darkestdungeon".to_string(),
        "Kingdom Come: Deliverance" => "kingdomcomedeliverance".to_string(),
        "Dishonored" => "dishonored".to_string(),
        _ => display_name.to_lowercase().replace(' ', ""),
    }
}

/// Initialize and run the main GUI application
pub fn run() -> Result<(), slint::PlatformError> {
    let window = MainWindow::new()?;

    // Load settings and apply defaults
    let loaded_settings = Settings::load();
    if !loaded_settings.default_install_dir.is_empty() {
        window.set_install_dir(loaded_settings.default_install_dir.clone().into());
    }
    if !loaded_settings.default_downloads_dir.is_empty() {
        window.set_downloads_dir(loaded_settings.default_downloads_dir.clone().into());
    }
    if !loaded_settings.nexus_api_key.is_empty() {
        window.set_nexus_api_key(loaded_settings.nexus_api_key.clone().into());

        // Validate saved API key on startup
        let api_key = loaded_settings.nexus_api_key.clone();
        window.set_api_key_state(ApiKeyState::Validating);
        let window_weak = window.as_weak();
        std::thread::spawn(move || {
            let is_valid = validate_nexus_api_key(&api_key);
            slint::invoke_from_event_loop(move || {
                if let Some(window) = window_weak.upgrade() {
                    if is_valid {
                        window.set_api_key_state(ApiKeyState::Valid);
                    } else {
                        window.set_api_key_state(ApiKeyState::Invalid);
                    }
                }
            }).ok();
        });
    }

    // Load available Protons (Proton 10+ required for NaK)
    let protons = crate::game_finder::find_steam_protons();
    let proton_options: Vec<ProtonOption> = protons
        .iter()
        .enumerate()
        .map(|(idx, p)| ProtonOption {
            index: idx as i32,
            name: p.name.clone().into(),
            is_experimental: p.is_experimental,
        })
        .collect();
    window.set_proton_options(std::rc::Rc::new(slint::VecModel::from(proton_options)).into());

    // String model for the ComboBox dropdown
    let proton_names: Vec<slint::SharedString> = protons
        .iter()
        .map(|p| p.name.clone().into())
        .collect();
    window.set_proton_names(std::rc::Rc::new(slint::VecModel::from(proton_names)).into());

    // Store proton info for later use (when starting installation with NaK)
    let _protons = std::rc::Rc::new(protons);

    // Store settings in a shared cell for access in callbacks
    let settings = std::rc::Rc::new(std::cell::RefCell::new(loaded_settings));

    // Set up browse source callback with rfd file dialog
    // Parsing is done in a background thread to avoid UI freeze
    window.on_browse_source({
        let window_weak = window.as_weak();
        move || {
            let window = window_weak.unwrap();
            if let Some(path) = rfd::FileDialog::new()
                .add_filter("Wabbajack", &["wabbajack"])
                .pick_file()
            {
                window.set_source_path(path.display().to_string().into());

                // Reset install state so user can start a new installation
                window.set_install_state(InstallState::Idle);
                window.set_progress(0.0);
                window.set_files_completed(0);
                window.set_files_total(0);
                window.set_status_message("Ready to install".into());

                // Auto-detect game from .wabbajack file (async to avoid UI freeze)
                if path.extension().map(|e| e == "wabbajack").unwrap_or(false) {
                    // Show loading state immediately
                    window.set_detected_game("Loading...".into());
                    window.set_ttw_required(false);
                    window.set_ttw_mpi_path("".into());

                    // Parse in background thread to keep UI responsive
                    let window_weak_bg = window.as_weak();
                    let path_clone = path.clone();
                    std::thread::spawn(move || {
                        let result = detect_modlist_info(&path_clone);

                        // Update UI from main thread
                        slint::invoke_from_event_loop(move || {
                            if let Some(window) = window_weak_bg.upgrade() {
                                match result {
                                    Ok(info) => {
                                        window.set_detected_game(info.game_display.into());

                                        // Set TTW properties
                                        window.set_ttw_required(info.ttw_required);
                                        window.set_ttw_fo3_found(info.fo3_path.is_some());
                                        window.set_ttw_fnv_found(info.fnv_path.is_some());

                                        if let Some(fo3) = info.fo3_path {
                                            window.set_ttw_fo3_path(fo3.display().to_string().into());
                                        } else {
                                            window.set_ttw_fo3_path("".into());
                                        }

                                        if let Some(fnv) = info.fnv_path {
                                            window.set_ttw_fnv_path(fnv.display().to_string().into());
                                        } else {
                                            window.set_ttw_fnv_path("".into());
                                        }

                                        // Apply cached configuration (always overwrite with saved values for this modlist)
                                        if let Some(install_dir) = info.cached_install_dir {
                                            window.set_install_dir(install_dir.into());
                                        }
                                        if let Some(downloads_dir) = info.cached_downloads_dir {
                                            window.set_downloads_dir(downloads_dir.into());
                                        }
                                        if let Some(ttw_mpi) = info.cached_ttw_mpi_path {
                                            window.set_ttw_mpi_path(ttw_mpi.into());
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to detect game: {}", e);
                                        window.set_detected_game(format!("Error: {}", e).into());
                                        window.set_ttw_required(false);
                                    }
                                }
                            }
                        }).ok();
                    });
                } else {
                    // Clear detected game for non-wabbajack files
                    window.set_detected_game("".into());
                    window.set_ttw_required(false);
                }
            }
        }
    });

    // Set up browse install directory callback
    window.on_browse_install({
        let window_weak = window.as_weak();
        move || {
            let window = window_weak.unwrap();
            if let Some(path) = rfd::FileDialog::new()
                .set_title("Select Install Directory")
                .pick_folder()
            {
                window.set_install_dir(path.display().to_string().into());
            }
        }
    });

    // Set up browse downloads directory callback
    window.on_browse_downloads({
        let window_weak = window.as_weak();
        move || {
            let window = window_weak.unwrap();
            if let Some(path) = rfd::FileDialog::new()
                .set_title("Select Downloads Directory")
                .pick_folder()
            {
                window.set_downloads_dir(path.display().to_string().into());
            }
        }
    });

    // Set up TTW MPI file browse callback
    window.on_browse_ttw_mpi({
        let window_weak = window.as_weak();
        move || {
            let window = window_weak.unwrap();
            if let Some(path) = rfd::FileDialog::new()
                .set_title("Select TTW MPI File")
                .add_filter("MPI Files", &["mpi"])
                .add_filter("All Files", &["*"])
                .pick_file()
            {
                window.set_ttw_mpi_path(path.display().to_string().into());
            }
        }
    });

    // Set up TTW MPI path edited callback (manual entry)
    window.on_ttw_mpi_edited({
        let window_weak = window.as_weak();
        move |text| {
            let window = window_weak.unwrap();
            window.set_ttw_mpi_path(text);
        }
    });

    // Set up start install callback with validation
    window.on_start_install({
        let window_weak = window.as_weak();
        move || {
            let window = window_weak.unwrap();

            // Validate inputs
            let source_path = window.get_source_path().to_string();
            let install_dir = window.get_install_dir().to_string();
            let downloads_dir = window.get_downloads_dir().to_string();
            let api_key = window.get_nexus_api_key().to_string();
            let proton_index = window.get_selected_proton_index();

            // Get selected Proton name
            let proton_name = if proton_index >= 0 {
                let protons = crate::game_finder::find_steam_protons();
                protons.get(proton_index as usize).map(|p| p.name.clone())
            } else {
                None
            };

            // Check required fields
            if proton_name.is_none() {
                window.set_status_message("Error: Please select a Proton version".into());
                return;
            }
            let proton_name = proton_name.unwrap();

            if source_path.is_empty() {
                window.set_status_message("Error: Please select a source file".into());
                return;
            }
            if install_dir.is_empty() {
                window.set_status_message("Error: Please select an install directory".into());
                return;
            }
            if downloads_dir.is_empty() {
                window.set_status_message("Error: Please select a downloads directory".into());
                return;
            }
            if api_key.is_empty() {
                window.set_status_message("Error: Please enter your Nexus API key".into());
                return;
            }

            // Validate paths exist
            let source = std::path::Path::new(&source_path);
            if !source.exists() {
                window.set_status_message("Error: Source file does not exist".into());
                return;
            }

            // Create directories if they don't exist
            let install = std::path::Path::new(&install_dir);
            let downloads = std::path::Path::new(&downloads_dir);

            if !install.exists() {
                if let Err(e) = std::fs::create_dir_all(install) {
                    window.set_status_message(format!("Error creating install dir: {}", e).into());
                    return;
                }
            }
            if !downloads.exists() {
                if let Err(e) = std::fs::create_dir_all(downloads) {
                    window.set_status_message(format!("Error creating downloads dir: {}", e).into());
                    return;
                }
            }

            // Set state to validating
            window.set_install_state(InstallState::Validating);
            window.set_status_message("Validating installation...".into());
            window.set_progress(0.0);

            // Log start
            let current_log = window.get_log_text();
            let new_log = format!("{}\n[INFO] Starting installation...\n[INFO] Source: {}\n[INFO] Install: {}\n[INFO] Downloads: {}",
                current_log, source_path, install_dir, downloads_dir);
            window.set_log_text(new_log.into());

            println!("Installation started:");
            println!("  Source: {}", source_path);
            println!("  Install: {}", install_dir);
            println!("  Downloads: {}", downloads_dir);

            let non_premium = window.get_non_premium_mode();

            // Get TTW settings
            let ttw_required = window.get_ttw_required();
            let ttw_mpi_path = window.get_ttw_mpi_path().to_string();
            let ttw_fo3_path = window.get_ttw_fo3_path().to_string();
            let ttw_fnv_path = window.get_ttw_fnv_path().to_string();

            // Validate TTW paths before starting (fail early with clear message)
            if ttw_required && !ttw_mpi_path.is_empty() {
                let mpi = std::path::Path::new(&ttw_mpi_path);
                if !mpi.exists() {
                    window.set_status_message(format!("Error: MPI file not found: {}", ttw_mpi_path).into());
                    let current_log = window.get_log_text();
                    window.set_log_text(format!("{}\n[ERROR] TTW MPI file not found. Please re-select the MPI file.", current_log).into());
                    return;
                }
            }

            // Save configuration to cache for future use
            if let Ok(modlist) = crate::modlist::parse_wabbajack_file(std::path::Path::new(&source_path)) {
                if let Ok(cache) = crate::installer::ConfigCache::open() {
                    let config = crate::installer::ModlistConfig {
                        install_dir: Some(install_dir.clone()),
                        downloads_dir: Some(downloads_dir.clone()),
                        ttw_mpi_path: if ttw_mpi_path.is_empty() { None } else { Some(ttw_mpi_path.clone()) },
                        fo3_path: if ttw_fo3_path.is_empty() { None } else { Some(ttw_fo3_path.clone()) },
                        fnv_path: if ttw_fnv_path.is_empty() { None } else { Some(ttw_fnv_path.clone()) },
                        ttw_required: Some(ttw_required),
                        ..Default::default()
                    };
                    if let Err(e) = cache.save_config(&modlist.name, &modlist.version, &modlist.game_type, None, &config) {
                        eprintln!("[GUI] Failed to save config cache: {}", e);
                    } else {
                        println!("[GUI] Saved configuration for {} v{}", modlist.name, modlist.version);
                    }
                }
            }

            // Clone values for the spawned thread
            let source_clone = source_path.clone();
            let install_clone = install_dir.clone();
            let downloads_clone = downloads_dir.clone();
            let api_key_clone = api_key.clone();
            let proton_name_clone = proton_name.clone();
            let ttw_mpi_clone = ttw_mpi_path.clone();
            let ttw_fo3_clone = ttw_fo3_path.clone();
            let ttw_fnv_clone = ttw_fnv_path.clone();

            // Spawn installation in background thread
            println!("[GUI] Spawning installation thread (non_premium={}, proton={})...", non_premium, proton_name);
            std::thread::spawn(move || {
                println!("[GUI] Background thread started");

                // Create a new tokio runtime for this thread
                let rt = match tokio::runtime::Runtime::new() {
                    Ok(rt) => {
                        println!("[GUI] Tokio runtime created successfully");
                        rt
                    }
                    Err(e) => {
                        eprintln!("[GUI] Failed to create runtime: {}", e);
                        return;
                    }
                };

                println!("[GUI] Starting async block...");
                rt.block_on(async {
                    // Wabbajack modlist installation
                    let result = run_wabbajack_install(
                        &source_clone,
                        &install_clone,
                        &downloads_clone,
                        &api_key_clone,
                        non_premium,
                    ).await;

                    match &result {
                        Ok(_) => {
                            println!("[GUI] Installation complete!");
                            let tx = get_progress_sender();

                            // Run TTW installation if required
                            let mut ttw_success = true;
                            if ttw_required && !ttw_mpi_clone.is_empty() && !ttw_fo3_clone.is_empty() && !ttw_fnv_clone.is_empty() {
                                tx.send(ProgressUpdate::Status("Installing TTW...".to_string())).ok();
                                tx.send(ProgressUpdate::Log("[INFO] Starting TTW installation...".to_string())).ok();

                                let install_path = std::path::Path::new(&install_clone);
                                let mpi_path = std::path::Path::new(&ttw_mpi_clone);
                                let fo3_path = std::path::Path::new(&ttw_fo3_clone);
                                let fnv_path = std::path::Path::new(&ttw_fnv_clone);

                                match crate::ttw::finalize_ttw_from_paths(install_path, mpi_path, fo3_path, fnv_path) {
                                    Ok(ttw_output) => {
                                        tx.send(ProgressUpdate::Log(format!(
                                            "[INFO] TTW installation completed! Output: {}",
                                            ttw_output.display()
                                        ))).ok();
                                    }
                                    Err(e) => {
                                        ttw_success = false;
                                        tx.send(ProgressUpdate::Log(format!(
                                            "[ERROR] TTW installation failed: {}",
                                            e
                                        ))).ok();
                                        tx.send(ProgressUpdate::Log(
                                            "[ERROR] Skipping Steam integration - please install TTW manually and re-run.".to_string()
                                        )).ok();
                                    }
                                }
                            } else if ttw_required {
                                ttw_success = false;
                                tx.send(ProgressUpdate::Log(
                                    "[ERROR] TTW required but paths not configured. Please configure and re-run.".to_string()
                                )).ok();
                            }

                            // Only run NaK if TTW succeeded (or wasn't required)
                            if !ttw_success {
                                tx.send(ProgressUpdate::Status("Installation incomplete - TTW failed".to_string())).ok();
                                tx.send(ProgressUpdate::Error("TTW installation failed".to_string())).ok();
                                return;
                            }

                            // Fix GPU settings in INI files (sD3DDevice)
                            tx.send(ProgressUpdate::Status("Fixing GPU settings...".to_string())).ok();
                            let install_path = std::path::Path::new(&install_clone);
                            match crate::gpu::fix_ini_gpu_settings(install_path) {
                                Ok(count) => {
                                    if count > 0 {
                                        tx.send(ProgressUpdate::Log(format!(
                                            "[INFO] Fixed sD3DDevice in {} INI files",
                                            count
                                        ))).ok();
                                    }
                                }
                                Err(e) => {
                                    tx.send(ProgressUpdate::Log(format!(
                                        "[WARN] Could not fix GPU settings: {}",
                                        e
                                    ))).ok();
                                }
                            }

                            // Run NaK for MO2 setup with Steam/Proton integration
                            tx.send(ProgressUpdate::Status("Setting up MO2...".to_string())).ok();
                            tx.send(ProgressUpdate::Log("[INFO] Running NaK for MO2/Steam integration...".to_string())).ok();

                            match crate::nak::NakManager::new() {
                                Ok(nak) => {
                                    // MO2 is installed in install_dir
                                    let mo2_path = std::path::Path::new(&install_clone);

                                    // Use modlist name for Steam shortcut
                                    let shortcut_name = std::path::Path::new(&source_clone)
                                        .file_stem()
                                        .map(|s| s.to_string_lossy().to_string())
                                        .unwrap_or_else(|| "Mod Organizer 2".to_string());

                                    tx.send(ProgressUpdate::Log(format!("[INFO] Creating Steam shortcut: {}", shortcut_name))).ok();

                                    match nak.setup_mo2(mo2_path, &shortcut_name, &proton_name_clone) {
                                        Ok(result) => {
                                            tx.send(ProgressUpdate::Log(format!(
                                                "[INFO] NaK setup complete! Steam AppID: {}",
                                                result.app_id
                                            ))).ok();

                                            // Restart Steam to pick up the new shortcut
                                            tx.send(ProgressUpdate::Status("Restarting Steam...".to_string())).ok();
                                            match nak.restart_steam() {
                                                Ok(_) => {
                                                    tx.send(ProgressUpdate::Log(
                                                        "[INFO] Steam restarted successfully!".to_string()
                                                    )).ok();
                                                }
                                                Err(e) => {
                                                    tx.send(ProgressUpdate::Log(format!(
                                                        "[WARN] Could not restart Steam: {} - please restart manually",
                                                        e
                                                    ))).ok();
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tx.send(ProgressUpdate::Log(format!(
                                                "[WARN] NaK setup failed: {}. You can run NaK manually later.",
                                                e
                                            ))).ok();
                                        }
                                    }
                                }
                                Err(e) => {
                                    tx.send(ProgressUpdate::Log(format!(
                                        "[WARN] Failed to initialize NaK: {}. You can run NaK manually later.",
                                        e
                                    ))).ok();
                                }
                            }

                            // Send final completion AFTER NaK finishes
                            tx.send(ProgressUpdate::Status("Installation complete!".to_string())).ok();
                            tx.send(ProgressUpdate::Log("[INFO] All done! You can now launch from Steam.".to_string())).ok();
                            tx.send(ProgressUpdate::Complete).ok();
                        }
                        Err(e) => eprintln!("[GUI] Installation failed: {}", e),
                    }
                });
            });
        }
    });

    window.on_cancel_install({
        let window_weak = window.as_weak();
        move || {
            let window = window_weak.unwrap();
            window.set_install_state(InstallState::Cancelled);
            window.set_status_message("Installation cancelled".into());

            let current_log = window.get_log_text();
            let new_log = format!("{}\n[WARN] Installation cancelled by user", current_log);
            window.set_log_text(new_log.into());

            println!("Cancel install clicked");
        }
    });

    window.on_validate_api_key({
        let window_weak = window.as_weak();
        move |key| {
            let key_str = key.to_string();

            // Don't validate empty keys
            if key_str.is_empty() {
                if let Some(window) = window_weak.upgrade() {
                    window.set_api_key_state(ApiKeyState::Unknown);
                }
                return;
            }

            // Set to validating state
            if let Some(window) = window_weak.upgrade() {
                window.set_api_key_state(ApiKeyState::Validating);
            }

            // Validate in background thread
            let window_weak_bg = window_weak.clone();
            std::thread::spawn(move || {
                // Make request to Nexus API to validate key
                let is_valid = validate_nexus_api_key(&key_str);

                // Update UI from main thread
                slint::invoke_from_event_loop(move || {
                    if let Some(window) = window_weak_bg.upgrade() {
                        if is_valid {
                            window.set_api_key_state(ApiKeyState::Valid);
                        } else {
                            window.set_api_key_state(ApiKeyState::Invalid);
                        }
                    }
                }).ok();
            });
        }
    });

    window.on_open_api_key_page(|| {
        // Open Nexus API key page in browser
        let _ = std::process::Command::new("xdg-open")
            .arg("https://www.nexusmods.com/users/myaccount?tab=api")
            .spawn();
    });

    // Set up browse modlists callback (opens modlist gallery dialog)
    window.on_browse_modlists({
        let window_weak = window.as_weak();
        move || {
            let main_window = window_weak.unwrap();

            // Create the browser dialog
            let dialog = match ModlistBrowserDialog::new() {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("Failed to create modlist browser: {}", e);
                    main_window.set_log_text(format!("Error opening modlist browser: {}\n", e).into());
                    return;
                }
            };

            // Helper function to format file sizes
            fn format_size(bytes: u64) -> String {
                const KB: u64 = 1024;
                const MB: u64 = KB * 1024;
                const GB: u64 = MB * 1024;

                if bytes >= GB {
                    format!("{:.1} GB", bytes as f64 / GB as f64)
                } else if bytes >= MB {
                    format!("{:.1} MB", bytes as f64 / MB as f64)
                } else if bytes >= KB {
                    format!("{:.1} KB", bytes as f64 / KB as f64)
                } else {
                    format!("{} B", bytes)
                }
            }

            // Store modlist data for callbacks (Arc+Mutex for thread safety)
            let modlist_data: std::sync::Arc<std::sync::Mutex<Vec<crate::modlist::ModlistMetadata>>> =
                std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

            // Create image cache (shared across callbacks)
            let image_cache: std::sync::Arc<std::sync::Mutex<image_cache::ImageCache>> =
                match image_cache::ImageCache::new() {
                    Ok(cache) => std::sync::Arc::new(std::sync::Mutex::new(cache)),
                    Err(e) => {
                        eprintln!("Warning: Could not create image cache: {}", e);
                        std::sync::Arc::new(std::sync::Mutex::new(image_cache::ImageCache::new().unwrap()))
                    }
                };

            // Helper to convert modlists to UI items
            fn modlists_to_ui(
                modlists: &[crate::modlist::ModlistMetadata],
                show_unofficial: bool,
                show_nsfw: bool,
            ) -> (Vec<ModlistInfo>, Vec<String>) {
                // Build game list
                let mut games: Vec<String> = vec!["All Games".to_string()];
                let mut seen_games: std::collections::HashMap<String, String> = std::collections::HashMap::new();
                for m in modlists {
                    if !m.game.is_empty() {
                        let key = m.game.to_lowercase();
                        seen_games.entry(key).or_insert_with(|| normalize_game_name(&m.game));
                    }
                }
                let mut game_names: Vec<String> = seen_games.into_values().collect();
                game_names.sort_by(|a, b| a.to_lowercase().cmp(&b.to_lowercase()));
                games.extend(game_names);

                // Convert to UI model
                let ui_modlists: Vec<ModlistInfo> = modlists
                    .iter()
                    .enumerate()
                    .map(|(idx, m)| {
                        let dl_size = m.download_metadata.as_ref()
                            .map(|d| format_size(d.size_of_archives))
                            .unwrap_or_else(|| "Unknown".into());
                        let inst_size = m.download_metadata.as_ref()
                            .map(|d| format_size(d.size_of_installed_files))
                            .unwrap_or_else(|| "Unknown".into());
                        let visible = (show_unofficial || m.official) && (show_nsfw || !m.nsfw);

                        ModlistInfo {
                            index: idx as i32,
                            machine_name: m.machine_name.clone().into(),
                            title: m.title.clone().into(),
                            author: m.author.clone().into(),
                            game: normalize_game_name(&m.game).into(),
                            game_internal: m.game.to_lowercase().into(),
                            download_size: dl_size.into(),
                            install_size: inst_size.into(),
                            description: m.description.clone().into(),
                            is_nsfw: m.nsfw,
                            is_official: m.official,
                            image: slint::Image::default(),
                            has_image: false,
                            visible,
                        }
                    })
                    .collect();

                (ui_modlists, games)
            }

            // Try to load from cache first for instant display
            let dialog_weak = dialog.as_weak();
            let modlist_data_clone = modlist_data.clone();
            let image_cache_clone = image_cache.clone();

            // Check if we have a cache
            let has_cache = crate::modlist::ModlistBrowser::has_recent_cache();

            std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();

                // Step 1: If we have cache, load and display it immediately
                if has_cache {
                    let cache_result: Result<Vec<crate::modlist::ModlistMetadata>, anyhow::Error> = (|| {
                        let mut browser = crate::modlist::ModlistBrowser::new()?;
                        browser.load_cache()?;
                        Ok(browser.modlists().to_vec())
                    })();

                    if let Ok(cached_modlists) = cache_result {
                        let dialog_weak_cache = dialog_weak.clone();
                        let modlist_data_for_cache = modlist_data_clone.clone();
                        let cached_modlists_for_ui = cached_modlists.clone();

                        slint::invoke_from_event_loop(move || {
                            if let Some(dialog) = dialog_weak_cache.upgrade() {
                                let show_unofficial = dialog.get_show_unofficial();
                                let show_nsfw = dialog.get_show_nsfw();
                                let (ui_modlists, games) = modlists_to_ui(&cached_modlists_for_ui, show_unofficial, show_nsfw);
                                let visible_count = ui_modlists.iter().filter(|m| m.visible).count() as i32;

                                *modlist_data_for_cache.lock().unwrap() = cached_modlists_for_ui;

                                let game_model: Vec<slint::SharedString> = games.iter().map(|g| g.clone().into()).collect();
                                dialog.set_game_list(std::rc::Rc::new(slint::VecModel::from(game_model)).into());
                                dialog.set_modlists(std::rc::Rc::new(slint::VecModel::from(ui_modlists)).into());
                                dialog.set_visible_count(visible_count);
                                dialog.set_is_loading(false);
                                dialog.set_status_message("Loading images...".into());
                                dialog.invoke_load_visible_images();
                            }
                        }).ok();
                    }
                }

                // Step 2: Fetch fresh data from network (either as primary or background update)
                let result: Result<Vec<crate::modlist::ModlistMetadata>, anyhow::Error> = rt.block_on(async {
                    let mut browser = crate::modlist::ModlistBrowser::new()?;
                    let modlists = browser.fetch_modlists().await?.to_vec();
                    // Save to cache for next time
                    if let Err(e) = browser.save_cache() {
                        eprintln!("Warning: Failed to save modlist cache: {}", e);
                    }
                    Ok(modlists)
                });

                // If we had cache, we've already displayed UI - just update data and sync images
                // If no cache, we need to build and display the full UI
                let had_cache = has_cache;

                slint::invoke_from_event_loop(move || {
                    if let Some(dialog) = dialog_weak.upgrade() {
                        match result {
                            Ok(modlists) => {
                                // Always store fresh metadata for downloads
                                *modlist_data_clone.lock().unwrap() = modlists.clone();

                                // If we already showed cached data, don't rebuild UI
                                // Just update the data store and continue with image sync
                                if !had_cache {
                                    let show_unofficial = dialog.get_show_unofficial();
                                    let show_nsfw = dialog.get_show_nsfw();
                                    let (ui_modlists, games) = modlists_to_ui(&modlists, show_unofficial, show_nsfw);
                                    let visible_count = ui_modlists.iter().filter(|m| m.visible).count() as i32;

                                    let game_model: Vec<slint::SharedString> = games.iter().map(|g| g.clone().into()).collect();
                                    dialog.set_game_list(std::rc::Rc::new(slint::VecModel::from(game_model)).into());
                                    dialog.set_modlists(std::rc::Rc::new(slint::VecModel::from(ui_modlists)).into());
                                    dialog.set_visible_count(visible_count);
                                    dialog.set_is_loading(false);
                                }

                                dialog.set_status_message("Syncing images...".into());

                                // Sync images in background (download missing, don't load into UI - lazy loader handles that)
                                let dialog_weak2 = dialog.as_weak();
                                let modlist_data_for_sync = modlist_data_clone.clone();
                                let image_cache_for_sync = image_cache_clone.clone();
                                std::thread::spawn(move || {
                                    let rt = tokio::runtime::Runtime::new().unwrap();
                                    rt.block_on(async {
                                        let modlists = modlist_data_for_sync.lock().unwrap().clone();
                                        let image_data: Vec<(String, String)> = modlists
                                            .iter()
                                            .filter_map(|m| {
                                                m.image_url().map(|url| (m.machine_name.clone(), url.to_string()))
                                            })
                                            .collect();

                                        // Prepare sync (hold lock briefly)
                                        let (to_download, skipped, removed, client, cache_dir) = {
                                            let mut cache = image_cache_for_sync.lock().unwrap();
                                            let (to_download, skipped, removed) = cache.prepare_sync(&image_data).unwrap_or_default();
                                            let (client, cache_dir) = cache.get_download_context();
                                            (to_download, skipped, removed, client, cache_dir)
                                        };

                                        // Download without holding lock
                                        let (succeeded, failed) = image_cache::download_images_parallel(
                                            &client,
                                            &cache_dir,
                                            to_download,
                                        ).await;

                                        // Update manifest
                                        {
                                            let mut cache = image_cache_for_sync.lock().unwrap();
                                            let _ = cache.finish_sync(&succeeded);
                                        }

                                        println!("[images] Sync complete: Downloaded: {}, Skipped: {}, Failed: {}, Removed: {}",
                                            succeeded.len(), skipped, failed, removed);

                                        // Clear status and trigger lazy load for visible items
                                        slint::invoke_from_event_loop(move || {
                                            if let Some(dialog) = dialog_weak2.upgrade() {
                                                dialog.set_status_message("".into());
                                                // Trigger lazy image loading
                                                dialog.invoke_load_visible_images();
                                            }
                                        }).ok();
                                    });
                                });
                            }
                            Err(e) => {
                                dialog.set_is_loading(false);
                                dialog.set_status_message(format!("Error: {}", e).into());
                            }
                        }
                    }
                }).ok();
            });

            // Cancel callback
            dialog.on_cancel({
                let dialog_weak = dialog.as_weak();
                move || {
                    if let Some(dialog) = dialog_weak.upgrade() {
                        dialog.hide().ok();
                    }
                }
            });

            // Shared image store - maps machine_name to raw RGBA data (width, height, pixels)
            // This persists images across filter changes (can't store slint::Image across threads)
            let loaded_images: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, (u32, u32, Vec<u8>)>>> =
                std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));

            // Helper to filter and create model with cached images
            fn filter_and_create_model(
                all_modlists: &[crate::modlist::ModlistMetadata],
                query: &str,
                game: &str,
                show_unofficial: bool,
                show_nsfw: bool,
                loaded_images: &std::collections::HashMap<String, (u32, u32, Vec<u8>)>,
            ) -> Vec<ModlistInfo> {
                let query_lower = query.to_lowercase();
                let game_filter_internal = if game == "All Games" { String::new() } else { denormalize_game_name(game) };

                all_modlists
                    .iter()
                    .enumerate()
                    .filter(|(_, m)| {
                        let game_internal = m.game.to_lowercase();
                        let matches_query = query_lower.is_empty() ||
                            m.title.to_lowercase().contains(&query_lower) ||
                            m.author.to_lowercase().contains(&query_lower) ||
                            game_internal.contains(&query_lower);
                        let matches_game = game_filter_internal.is_empty() ||
                            game_internal == game_filter_internal;
                        let matches_unofficial = show_unofficial || m.official;
                        let matches_nsfw = show_nsfw || !m.nsfw;
                        matches_query && matches_game && matches_unofficial && matches_nsfw
                    })
                    .map(|(idx, m)| {
                        let dl_size = m.download_metadata.as_ref()
                            .map(|d| format_size(d.size_of_archives))
                            .unwrap_or_else(|| "Unknown".into());
                        let inst_size = m.download_metadata.as_ref()
                            .map(|d| format_size(d.size_of_installed_files))
                            .unwrap_or_else(|| "Unknown".into());

                        // Look up cached raw image data and convert to slint::Image
                        let (image, has_image) = loaded_images.get(&m.machine_name)
                            .map(|(w, h, pixels)| {
                                let buffer = slint::SharedPixelBuffer::<slint::Rgba8Pixel>::clone_from_slice(
                                    pixels, *w, *h
                                );
                                (slint::Image::from_rgba8(buffer), true)
                            })
                            .unwrap_or_else(|| (slint::Image::default(), false));

                        ModlistInfo {
                            index: idx as i32,
                            machine_name: m.machine_name.clone().into(),
                            title: m.title.clone().into(),
                            author: m.author.clone().into(),
                            game: normalize_game_name(&m.game).into(),
                            game_internal: m.game.to_lowercase().into(),
                            download_size: dl_size.into(),
                            install_size: inst_size.into(),
                            description: m.description.clone().into(),
                            is_nsfw: m.nsfw,
                            is_official: m.official,
                            image,
                            has_image,
                            visible: true,
                        }
                    })
                    .collect()
            }

            // Search callback - filters and recreates model with cached images
            dialog.on_search_changed({
                let dialog_weak = dialog.as_weak();
                let modlist_data = modlist_data.clone();
                let loaded_images = loaded_images.clone();
                move |query| {
                    if let Some(dialog) = dialog_weak.upgrade() {
                        let all_modlists = modlist_data.lock().unwrap();
                        let images = loaded_images.lock().unwrap();
                        let filtered = filter_and_create_model(
                            &all_modlists,
                            &query.to_string(),
                            &dialog.get_selected_game().to_string(),
                            dialog.get_show_unofficial(),
                            dialog.get_show_nsfw(),
                            &images,
                        );
                        dialog.set_modlists(std::rc::Rc::new(slint::VecModel::from(filtered)).into());
                    }
                }
            });

            // Game filter callback
            dialog.on_game_filter_changed({
                let dialog_weak = dialog.as_weak();
                let modlist_data = modlist_data.clone();
                let loaded_images = loaded_images.clone();
                move |game| {
                    if let Some(dialog) = dialog_weak.upgrade() {
                        let all_modlists = modlist_data.lock().unwrap();
                        let images = loaded_images.lock().unwrap();
                        let filtered = filter_and_create_model(
                            &all_modlists,
                            &dialog.get_search_text().to_string(),
                            &game.to_string(),
                            dialog.get_show_unofficial(),
                            dialog.get_show_nsfw(),
                            &images,
                        );
                        dialog.set_modlists(std::rc::Rc::new(slint::VecModel::from(filtered)).into());
                    }
                }
            });

            // Filter changed callback (unofficial/nsfw checkboxes)
            dialog.on_filter_changed({
                let dialog_weak = dialog.as_weak();
                let modlist_data = modlist_data.clone();
                let loaded_images = loaded_images.clone();
                move |show_unofficial, show_nsfw| {
                    if let Some(dialog) = dialog_weak.upgrade() {
                        let all_modlists = modlist_data.lock().unwrap();
                        let images = loaded_images.lock().unwrap();
                        let filtered = filter_and_create_model(
                            &all_modlists,
                            &dialog.get_search_text().to_string(),
                            &dialog.get_selected_game().to_string(),
                            show_unofficial,
                            show_nsfw,
                            &images,
                        );
                        dialog.set_modlists(std::rc::Rc::new(slint::VecModel::from(filtered)).into());
                    }
                }
            });

            // Select modlist callback - downloads and sets path
            dialog.on_select_modlist({
                let dialog_weak = dialog.as_weak();
                let main_window_weak = window_weak.clone();
                let modlist_data = modlist_data.clone();
                move |index| {
                    // Get the machine_name from the UI model by finding the item with matching index
                    let machine_name = if let Some(dialog) = dialog_weak.upgrade() {
                        let model = dialog.get_modlists();
                        // Search through model for item with this index value
                        (0..model.row_count()).find_map(|i| {
                            model.row_data(i).and_then(|m| {
                                if m.index == index { Some(m.machine_name.to_string()) } else { None }
                            })
                        })
                    } else {
                        None
                    };

                    let data = modlist_data.lock().unwrap();
                    // Look up by machine_name instead of index to avoid race conditions
                    let metadata = machine_name.as_ref().and_then(|name| {
                        data.iter().find(|m| &m.machine_name == name)
                    });
                    if let Some(metadata) = metadata {
                        let dialog_weak = dialog_weak.clone();
                        let main_window_weak = main_window_weak.clone();
                        let metadata = metadata.clone();

                        // Update UI to show downloading
                        if let Some(dialog) = dialog_weak.upgrade() {
                            dialog.set_is_downloading(true);
                            dialog.set_download_progress(0.0);
                        }

                        // Download in background
                        std::thread::spawn(move || {
                            eprintln!("[download] Starting download for: {}", metadata.title);
                            eprintln!("[download] Download URL: {:?}", metadata.download_url());

                            let rt = tokio::runtime::Runtime::new().unwrap();
                            let dialog_weak_progress = dialog_weak.clone();
                            let result = rt.block_on(async {
                                let browser = crate::modlist::ModlistBrowser::new()?;
                                let downloads_dir = dirs::download_dir()
                                    .unwrap_or_else(|| std::path::PathBuf::from("."));
                                eprintln!("[download] Downloads dir: {:?}", downloads_dir);
                                eprintln!("[download] .wabbajack files are saved to your Downloads folder");

                                // Use progress callback to update UI
                                browser.download_modlist_with_progress(&metadata, &downloads_dir, move |downloaded, total| {
                                    let progress = if total > 0 { downloaded as f32 / total as f32 } else { 0.0 };
                                    let dialog_weak = dialog_weak_progress.clone();
                                    let _ = slint::invoke_from_event_loop(move || {
                                        if let Some(dialog) = dialog_weak.upgrade() {
                                            dialog.set_download_progress(progress);
                                            let mb_downloaded = downloaded as f64 / 1_048_576.0;
                                            let mb_total = total as f64 / 1_048_576.0;
                                            dialog.set_status_message(
                                                format!("Downloading: {:.1} / {:.1} MB ({:.0}%)", mb_downloaded, mb_total, progress * 100.0).into()
                                            );
                                        }
                                    });
                                }).await
                            });

                            eprintln!("[download] Result: {:?}", result.as_ref().map(|p| p.display().to_string()).map_err(|e| e.to_string()));

                            slint::invoke_from_event_loop(move || {
                                if let Some(dialog) = dialog_weak.upgrade() {
                                    dialog.set_is_downloading(false);

                                    match result {
                                        Ok(path) => {
                                            // Set the path in main window
                                            if let Some(main_window) = main_window_weak.upgrade() {
                                                main_window.set_source_path(path.display().to_string().into());
                                                main_window.set_log_text(format!(
                                                    "Downloaded modlist: {} to {}\n",
                                                    metadata.title,
                                                    path.display()
                                                ).into());

                                                // Trigger game detection in background thread
                                                main_window.set_detected_game("Loading...".into());
                                                main_window.set_ttw_required(false);
                                                main_window.set_ttw_mpi_path("".into());
                                                let window_weak_detect = main_window.as_weak();
                                                let path_clone = path.clone();
                                                std::thread::spawn(move || {
                                                    let result = detect_modlist_info(&path_clone);
                                                    slint::invoke_from_event_loop(move || {
                                                        if let Some(window) = window_weak_detect.upgrade() {
                                                            match result {
                                                                Ok(info) => {
                                                                    window.set_detected_game(info.game_display.into());

                                                                    // Set TTW properties
                                                                    window.set_ttw_required(info.ttw_required);
                                                                    window.set_ttw_fo3_found(info.fo3_path.is_some());
                                                                    window.set_ttw_fnv_found(info.fnv_path.is_some());

                                                                    if let Some(fo3) = info.fo3_path {
                                                                        window.set_ttw_fo3_path(fo3.display().to_string().into());
                                                                    } else {
                                                                        window.set_ttw_fo3_path("".into());
                                                                    }

                                                                    if let Some(fnv) = info.fnv_path {
                                                                        window.set_ttw_fnv_path(fnv.display().to_string().into());
                                                                    } else {
                                                                        window.set_ttw_fnv_path("".into());
                                                                    }

                                                                    // Apply cached configuration (always overwrite)
                                                                    if let Some(install_dir) = info.cached_install_dir {
                                                                        window.set_install_dir(install_dir.into());
                                                                    }
                                                                    if let Some(downloads_dir) = info.cached_downloads_dir {
                                                                        window.set_downloads_dir(downloads_dir.into());
                                                                    }
                                                                    if let Some(ttw_mpi) = info.cached_ttw_mpi_path {
                                                                        window.set_ttw_mpi_path(ttw_mpi.into());
                                                                    }
                                                                }
                                                                Err(e) => {
                                                                    eprintln!("Failed to detect game: {}", e);
                                                                    window.set_detected_game(format!("Error: {}", e).into());
                                                                    window.set_ttw_required(false);
                                                                }
                                                            }
                                                        }
                                                    }).ok();
                                                });
                                            }
                                            dialog.hide().ok();
                                        }
                                        Err(e) => {
                                            dialog.set_status_message(format!("Download failed: {}", e).into());
                                        }
                                    }
                                }
                            }).ok();
                        });
                    }
                }
            });

            // Refresh callback
            dialog.on_refresh({
                let dialog_weak = dialog.as_weak();
                let modlist_data = modlist_data.clone();
                let image_cache = image_cache.clone();
                move || {
                    if let Some(dialog) = dialog_weak.upgrade() {
                        dialog.set_is_loading(true);
                        dialog.set_status_message("Refreshing modlists...".into());

                        let dialog_weak = dialog_weak.clone();
                        let modlist_data = modlist_data.clone();
                        let _image_cache = image_cache.clone();
                        std::thread::spawn(move || {
                            let rt = tokio::runtime::Runtime::new().unwrap();
                            let result: Result<Vec<crate::modlist::ModlistMetadata>, anyhow::Error> = rt.block_on(async {
                                let mut browser = crate::modlist::ModlistBrowser::new()?;
                                let modlists = browser.fetch_modlists().await?.to_vec();
                                // Save to cache
                                if let Err(e) = browser.save_cache() {
                                    eprintln!("Warning: Failed to save modlist cache: {}", e);
                                }
                                Ok(modlists)
                            });

                            slint::invoke_from_event_loop(move || {
                                if let Some(dialog) = dialog_weak.upgrade() {
                                    match result {
                                        Ok(modlists) => {
                                            // Rebuild game list (case-insensitive dedup)
                                            let mut games: Vec<String> = vec!["All Games".to_string()];
                                            let mut seen_games: std::collections::HashMap<String, String> = std::collections::HashMap::new();
                                            for m in &modlists {
                                                if !m.game.is_empty() {
                                                    let key = m.game.to_lowercase();
                                                    seen_games.entry(key).or_insert_with(|| normalize_game_name(&m.game));
                                                }
                                            }
                                            let mut game_names: Vec<String> = seen_games.into_values().collect();
                                            game_names.sort_by(|a, b| a.to_lowercase().cmp(&b.to_lowercase()));
                                            games.extend(game_names);

                                            // Apply current filters with visibility flags
                                            let show_unofficial = dialog.get_show_unofficial();
                                            let show_nsfw = dialog.get_show_nsfw();

                                            let ui_modlists: Vec<ModlistInfo> = modlists
                                                .iter()
                                                .enumerate()
                                                .map(|(idx, m)| {
                                                    let dl_size = m.download_metadata.as_ref()
                                                        .map(|d| format_size(d.size_of_archives))
                                                        .unwrap_or_else(|| "Unknown".into());
                                                    let inst_size = m.download_metadata.as_ref()
                                                        .map(|d| format_size(d.size_of_installed_files))
                                                        .unwrap_or_else(|| "Unknown".into());

                                                    // Determine visibility based on current filters
                                                    let visible = (show_unofficial || m.official) && (show_nsfw || !m.nsfw);

                                                    ModlistInfo {
                                                        index: idx as i32,
                                                        machine_name: m.machine_name.clone().into(),
                                                        title: m.title.clone().into(),
                                                        author: m.author.clone().into(),
                                                        game: normalize_game_name(&m.game).into(),
                                                        game_internal: m.game.to_lowercase().into(),
                                                        download_size: dl_size.into(),
                                                        install_size: inst_size.into(),
                                                        description: m.description.clone().into(),
                                                        is_nsfw: m.nsfw,
                                                        is_official: m.official,
                                                        image: slint::Image::default(),
                                                        has_image: false,
                                                        visible,
                                                    }
                                                })
                                                .collect();

                                            let visible_count = ui_modlists.iter().filter(|m| m.visible).count() as i32;
                                            *modlist_data.lock().unwrap() = modlists.to_vec();

                                            // Update game list
                                            let game_model: Vec<slint::SharedString> = games.iter().map(|g| g.clone().into()).collect();
                                            dialog.set_game_list(std::rc::Rc::new(slint::VecModel::from(game_model)).into());

                                            // Reset game filter to "All Games"
                                            dialog.set_selected_game("All Games".into());
                                            dialog.set_game_index(0);

                                            dialog.set_modlists(std::rc::Rc::new(slint::VecModel::from(ui_modlists)).into());
                                            dialog.set_visible_count(visible_count);
                                            dialog.set_is_loading(false);
                                            dialog.set_status_message("".into());

                                            // Trigger lazy image loading for visible items
                                            dialog.invoke_load_visible_images();
                                        }
                                        Err(e) => {
                                            dialog.set_is_loading(false);
                                            dialog.set_status_message(format!("Error: {}", e).into());
                                        }
                                    }
                                }
                            }).ok();
                        });
                    }
                }
            });

            // Load cached images in background - stores in shared HashMap for filter persistence
            dialog.on_load_visible_images({
                let dialog_weak = dialog.as_weak();
                let image_cache = image_cache.clone();
                let loaded_images = loaded_images.clone();
                move || {
                    let dialog_weak = dialog_weak.clone();
                    let image_cache = image_cache.clone();
                    let loaded_images = loaded_images.clone();

                    // Get items that need images from the model directly
                    let dialog_weak_for_check = dialog_weak.clone();
                    let items_needing_images: Vec<(usize, String)> = if let Some(dialog) = dialog_weak_for_check.upgrade() {
                        let model = dialog.get_modlists();
                        let existing_images = loaded_images.lock().unwrap();
                        (0..model.row_count())
                            .filter_map(|i| {
                                model.row_data(i).and_then(|item| {
                                    let machine_name = item.machine_name.to_string();
                                    // Skip if already in shared cache
                                    if existing_images.contains_key(&machine_name) {
                                        None
                                    } else {
                                        Some((i, machine_name))
                                    }
                                })
                            })
                            .collect()
                    } else {
                        return;
                    };

                    if items_needing_images.is_empty() {
                        eprintln!("[images] All images already loaded");
                        return;
                    }

                    eprintln!("[images] Loading {} images...", items_needing_images.len());

                    std::thread::spawn(move || {
                        let cache = image_cache.lock().unwrap();
                        let cache_dir = cache.cache_dir().to_path_buf();
                        drop(cache);

                        eprintln!("[images] Cache dir: {:?}", cache_dir);

                        // Build list of cached images that exist on disk
                        let image_tasks: Vec<(usize, String, std::path::PathBuf)> = items_needing_images
                            .into_iter()
                            .filter_map(|(row_idx, machine_name)| {
                                for ext in &["png", "jpg", "webp", "gif"] {
                                    let path = cache_dir.join(format!("{}.{}", machine_name, ext));
                                    if path.exists() {
                                        return Some((row_idx, machine_name, path));
                                    }
                                }
                                // Debug: print first few missing images
                                if row_idx < 5 {
                                    eprintln!("[images] No cached image for: '{}' (row {})", machine_name, row_idx);
                                }
                                None
                            })
                            .collect();

                        eprintln!("[images] Found {} cached images on disk", image_tasks.len());

                        if image_tasks.is_empty() {
                            eprintln!("[images] No images found on disk!");
                            return;
                        }

                        // Decode ALL images in parallel using rayon (much faster than sequential)
                        use rayon::prelude::*;
                        let decoded: Vec<(usize, String, u32, u32, Vec<u8>)> = image_tasks
                            .par_iter()
                            .filter_map(|(row_idx, machine_name, path)| {
                                match image::open(path) {
                                    Ok(img) => {
                                        let rgba = img.to_rgba8();
                                        let (w, h) = rgba.dimensions();
                                        Some((*row_idx, machine_name.clone(), w, h, rgba.into_raw()))
                                    }
                                    Err(_) => None,
                                }
                            })
                            .collect();

                        eprintln!("[images] Decoded {} images", decoded.len());

                        // Store raw data in shared cache (before sending to UI thread)
                        {
                            let mut image_store = loaded_images.lock().unwrap();
                            for (_, machine_name, width, height, pixels) in &decoded {
                                image_store.insert(machine_name.clone(), (*width, *height, pixels.clone()));
                            }
                            eprintln!("[images] Cached {} images, total cache: {}", decoded.len(), image_store.len());
                        }

                        // Send to UI thread for display
                        slint::invoke_from_event_loop(move || {
                            if let Some(dialog) = dialog_weak.upgrade() {
                                let model = dialog.get_modlists();

                                for (row_idx, _machine_name, width, height, pixels) in decoded {
                                    let buffer = slint::SharedPixelBuffer::<slint::Rgba8Pixel>::clone_from_slice(
                                        &pixels, width, height
                                    );
                                    let img = slint::Image::from_rgba8(buffer);

                                    // Update current model
                                    if let Some(mut item) = model.row_data(row_idx) {
                                        item.image = img;
                                        item.has_image = true;
                                        model.set_row_data(row_idx, item);
                                    }
                                }
                                eprintln!("[images] Updated UI with images");
                            }
                        }).ok();
                    });
                }
            });

            // Trigger image loading
            dialog.invoke_load_visible_images();

            // Show the dialog
            dialog.show().ok();
        }
    });

    window.on_open_settings({
        let settings = settings.clone();
        let window_weak = window.as_weak();
        move || {
            // Open settings dialog
            let dialog = match SettingsDialog::new() {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("Failed to create settings dialog: {}", e);
                    return;
                }
            };

            // Load current settings into dialog
            let current = settings.borrow();
            dialog.set_default_install_dir(current.default_install_dir.clone().into());
            dialog.set_default_downloads_dir(current.default_downloads_dir.clone().into());
            dialog.set_nexus_api_key(current.nexus_api_key.clone().into());
            dialog.set_selected_gpu_index(current.gpu_index.map(|i| i as i32).unwrap_or(-1));
            drop(current);

            // Load available GPUs
            let gpus = settings::get_available_gpus();
            let gpu_options: Vec<GpuOption> = gpus
                .iter()
                .map(|(idx, name)| GpuOption {
                    index: *idx as i32,
                    name: name.clone().into(),
                })
                .collect();
            dialog.set_gpu_options(std::rc::Rc::new(slint::VecModel::from(gpu_options)).into());

            // Browse install callback
            dialog.on_browse_install({
                let dialog_weak = dialog.as_weak();
                move || {
                    if let Some(dialog) = dialog_weak.upgrade() {
                        if let Some(path) = rfd::FileDialog::new()
                            .set_title("Select Default Install Directory")
                            .pick_folder()
                        {
                            dialog.set_default_install_dir(path.display().to_string().into());
                        }
                    }
                }
            });

            // Browse downloads callback
            dialog.on_browse_downloads({
                let dialog_weak = dialog.as_weak();
                move || {
                    if let Some(dialog) = dialog_weak.upgrade() {
                        if let Some(path) = rfd::FileDialog::new()
                            .set_title("Select Default Downloads Directory")
                            .pick_folder()
                        {
                            dialog.set_default_downloads_dir(path.display().to_string().into());
                        }
                    }
                }
            });

            // GPU selection callback
            dialog.on_gpu_selected({
                let dialog_weak = dialog.as_weak();
                move |idx| {
                    if let Some(dialog) = dialog_weak.upgrade() {
                        dialog.set_selected_gpu_index(idx);
                    }
                }
            });

            // Save callback
            dialog.on_save_settings({
                let dialog_weak = dialog.as_weak();
                let settings = settings.clone();
                let window_weak = window_weak.clone();
                move || {
                    if let Some(dialog) = dialog_weak.upgrade() {
                        // Update settings
                        let mut s = settings.borrow_mut();
                        s.default_install_dir = dialog.get_default_install_dir().to_string();
                        s.default_downloads_dir = dialog.get_default_downloads_dir().to_string();
                        s.nexus_api_key = dialog.get_nexus_api_key().to_string();
                        let gpu_idx = dialog.get_selected_gpu_index();
                        s.gpu_index = if gpu_idx < 0 { None } else { Some(gpu_idx as usize) };

                        // Save to disk
                        if let Err(e) = s.save() {
                            eprintln!("Failed to save settings: {}", e);
                        }

                        // Apply to main window
                        if let Some(window) = window_weak.upgrade() {
                            if !s.default_install_dir.is_empty() && window.get_install_dir().is_empty() {
                                window.set_install_dir(s.default_install_dir.clone().into());
                            }
                            if !s.default_downloads_dir.is_empty() && window.get_downloads_dir().is_empty() {
                                window.set_downloads_dir(s.default_downloads_dir.clone().into());
                            }
                            if !s.nexus_api_key.is_empty() && window.get_nexus_api_key().is_empty() {
                                window.set_nexus_api_key(s.nexus_api_key.clone().into());
                            }
                        }

                        dialog.hide().ok();
                    }
                }
            });

            // Cancel callback
            dialog.on_cancel_settings({
                let dialog_weak = dialog.as_weak();
                move || {
                    if let Some(dialog) = dialog_weak.upgrade() {
                        dialog.hide().ok();
                    }
                }
            });

            // Show dialog
            dialog.show().ok();
        }
    });

    window.on_source_edited(|text| {
        println!("Source edited: {}", text);
    });

    window.on_install_edited(|text| {
        println!("Install dir edited: {}", text);
    });

    window.on_downloads_edited(|text| {
        println!("Downloads dir edited: {}", text);
    });

    window.on_api_key_edited(|_text| {
        // Don't log the API key
    });

    // Set up a timer to poll for progress updates from the background installation thread.
    // The timer must be kept alive (not dropped) for it to continue firing,
    // so we use _timer to prevent unused variable warnings while keeping it in scope.
    let _timer = slint::Timer::default();
    let window_weak = window.as_weak();
    _timer.start(
        slint::TimerMode::Repeated,
        std::time::Duration::from_millis(100),
        move || {
            let window = window_weak.unwrap();

            // Try to receive all pending updates (non-blocking)
            if let Ok(rx) = PROGRESS_CHANNEL.1.try_lock() {
                while let Ok(update) = rx.try_recv() {
                    match update {
                        ProgressUpdate::Phase(phase) => {
                            let state = match phase.as_str() {
                                "Downloading" => InstallState::Downloading,
                                "Extracting" => InstallState::Extracting,
                                "Installing" => InstallState::Installing,
                                "Validating" => InstallState::Validating,
                                _ => InstallState::Validating,
                            };
                            window.set_install_state(state);
                        }
                        ProgressUpdate::Status(msg) => {
                            window.set_status_message(msg.into());
                        }
                        ProgressUpdate::FileProgress(filename, progress) => {
                            window.set_current_download_file(filename.into());
                            window.set_current_download_progress(progress);
                        }
                        ProgressUpdate::OverallProgress(p) => {
                            window.set_progress(p);
                        }
                        ProgressUpdate::FileCount(done, total) => {
                            window.set_files_completed(done);
                            window.set_files_total(total);
                        }
                        ProgressUpdate::DownloadSpeed(speed) => {
                            window.set_download_speed(speed.into());
                        }
                        ProgressUpdate::SizeProgress(size) => {
                            window.set_size_progress(size.into());
                        }
                        ProgressUpdate::Eta(eta) => {
                            window.set_download_eta(eta.into());
                        }
                        ProgressUpdate::Log(msg) => {
                            let current = window.get_log_text();
                            window.set_log_text(format!("{}\n{}", current, msg).into());
                        }
                        ProgressUpdate::Complete => {
                            window.set_install_state(InstallState::Complete);
                            window.set_status_message("Installation complete!".into());
                            window.set_progress(1.0);
                        }
                        ProgressUpdate::Error(e) => {
                            window.set_install_state(InstallState::Error);
                            window.set_status_message(format!("Error: {}", e).into());
                        }
                    }
                }
            }
        },
    );

    window.run()
}

/// Get a new MainWindow instance for custom configuration
pub fn create_main_window() -> Result<MainWindow, slint::PlatformError> {
    MainWindow::new()
}

/// Initialize and run the download view for NXM mode
pub fn run_download_view() -> Result<DownloadView, slint::PlatformError> {
    DownloadView::new()
}

/// Create and return the installation progress view
pub fn create_install_progress_view() -> Result<InstallProgressView, slint::PlatformError> {
    InstallProgressView::new()
}

/// Helper struct for managing main window state
pub struct MainWindowHandle {
    window: MainWindow,
}

impl MainWindowHandle {
    /// Create a new main window handle
    pub fn new() -> Result<Self, slint::PlatformError> {
        let window = MainWindow::new()?;
        Ok(Self { window })
    }

    /// Set source path
    pub fn set_source_path(&self, path: &str) {
        self.window.set_source_path(path.into());
    }

    /// Set install directory
    pub fn set_install_dir(&self, path: &str) {
        self.window.set_install_dir(path.into());
    }

    /// Set downloads directory
    pub fn set_downloads_dir(&self, path: &str) {
        self.window.set_downloads_dir(path.into());
    }

    /// Set Nexus API key
    pub fn set_api_key(&self, key: &str) {
        self.window.set_nexus_api_key(key.into());
    }

    /// Set API key validation state
    pub fn set_api_key_state(&self, state: ApiKeyState) {
        self.window.set_api_key_state(state);
    }

    /// Set installation state
    pub fn set_install_state(&self, state: InstallState) {
        self.window.set_install_state(state);
    }

    /// Set progress (0.0 to 1.0)
    pub fn set_progress(&self, progress: f32) {
        self.window.set_progress(progress);
    }

    /// Set status message
    pub fn set_status(&self, message: &str) {
        self.window.set_status_message(message.into());
    }

    /// Append to log
    pub fn append_log(&self, message: &str) {
        let current = self.window.get_log_text();
        let new_log = format!("{}\n{}", current, message);
        self.window.set_log_text(new_log.into());
    }

    /// Clear log
    pub fn clear_log(&self) {
        self.window.set_log_text("".into());
    }

    /// Set current download file name
    pub fn set_current_download_file(&self, filename: &str) {
        self.window.set_current_download_file(filename.into());
    }

    /// Set current download progress (0.0 to 1.0)
    pub fn set_current_download_progress(&self, progress: f32) {
        self.window.set_current_download_progress(progress);
    }

    /// Set download speed string (e.g., "5.2 MB/s")
    pub fn set_download_speed(&self, speed: &str) {
        self.window.set_download_speed(speed.into());
    }

    /// Set download ETA string (e.g., "12:34")
    pub fn set_download_eta(&self, eta: &str) {
        self.window.set_download_eta(eta.into());
    }

    /// Set file counts (completed/total)
    pub fn set_file_counts(&self, completed: i32, total: i32) {
        self.window.set_files_completed(completed);
        self.window.set_files_total(total);
    }

    /// Update download activity with all fields
    pub fn update_download_activity(
        &self,
        filename: &str,
        progress: f32,
        speed: &str,
        eta: &str,
        completed: i32,
        total: i32,
    ) {
        self.window.set_current_download_file(filename.into());
        self.window.set_current_download_progress(progress);
        self.window.set_download_speed(speed.into());
        self.window.set_download_eta(eta.into());
        self.window.set_files_completed(completed);
        self.window.set_files_total(total);
    }

    /// Set callback for browse source button
    pub fn on_browse_source(&self, callback: impl Fn() + 'static) {
        self.window.on_browse_source(callback);
    }

    /// Set callback for browse install button
    pub fn on_browse_install(&self, callback: impl Fn() + 'static) {
        self.window.on_browse_install(callback);
    }

    /// Set callback for browse downloads button
    pub fn on_browse_downloads(&self, callback: impl Fn() + 'static) {
        self.window.on_browse_downloads(callback);
    }

    /// Set callback for start install button
    pub fn on_start_install(&self, callback: impl Fn() + 'static) {
        self.window.on_start_install(callback);
    }

    /// Set callback for cancel button
    pub fn on_cancel_install(&self, callback: impl Fn() + 'static) {
        self.window.on_cancel_install(callback);
    }

    /// Set callback for API key validation
    pub fn on_validate_api_key(&self, callback: impl Fn(slint::SharedString) + 'static) {
        self.window.on_validate_api_key(callback);
    }

    /// Get source path
    pub fn get_source_path(&self) -> String {
        self.window.get_source_path().to_string()
    }

    /// Get install directory
    pub fn get_install_dir(&self) -> String {
        self.window.get_install_dir().to_string()
    }

    /// Get downloads directory
    pub fn get_downloads_dir(&self) -> String {
        self.window.get_downloads_dir().to_string()
    }

    /// Get API key
    pub fn get_api_key(&self) -> String {
        self.window.get_nexus_api_key().to_string()
    }

    /// Check if non-premium mode is enabled
    pub fn is_non_premium_mode(&self) -> bool {
        self.window.get_non_premium_mode()
    }

    /// Set detected game info (displayed after auto-detection)
    pub fn set_detected_game(&self, game_info: &str) {
        self.window.set_detected_game(game_info.into());
    }

    /// Get detected game info
    pub fn get_detected_game(&self) -> String {
        self.window.get_detected_game().to_string()
    }

    /// Run the window (blocking)
    pub fn run(&self) -> Result<(), slint::PlatformError> {
        self.window.run()
    }

    /// Get direct access to the window
    pub fn window(&self) -> &MainWindow {
        &self.window
    }
}

impl Default for MainWindowHandle {
    fn default() -> Self {
        Self::new().expect("Failed to create main window")
    }
}

/// Helper struct for managing download view state
pub struct DownloadViewHandle {
    view: DownloadView,
}

impl DownloadViewHandle {
    /// Create a new download view handle
    pub fn new() -> Result<Self, slint::PlatformError> {
        let view = DownloadView::new()?;
        Ok(Self { view })
    }

    /// Set the queue position (current/total)
    pub fn set_queue_position(&self, current: i32, total: i32) {
        self.view.set_queue_position(current);
        self.view.set_queue_total(total);
    }

    /// Set current mod information
    pub fn set_current_mod(&self, name: &str, mod_id: i64, file_id: i64, file_size: &str) {
        self.view.set_current_mod(ModInfo {
            mod_name: name.into(),
            mod_id: mod_id as i32,
            file_id: file_id as i32,
            file_size: file_size.into(),
        });
    }

    /// Set the download state
    pub fn set_state(&self, state: DownloadState) {
        self.view.set_state(state);
    }

    /// Set the download progress (0.0 to 1.0)
    pub fn set_progress(&self, progress: f32) {
        self.view.set_progress(progress);
    }

    /// Set the status message
    pub fn set_status(&self, message: &str) {
        self.view.set_status_message(message.into());
    }

    /// Set an error message and show the error area
    pub fn set_error(&self, message: &str) {
        self.view.set_error_message(message.into());
        self.view.set_has_error(true);
        self.view.set_state(DownloadState::Error);
    }

    /// Clear the error state
    pub fn clear_error(&self) {
        self.view.set_error_message("".into());
        self.view.set_has_error(false);
    }

    /// Set callback for Register NXM Handler button
    pub fn on_register_nxm_handler(&self, callback: impl Fn() + 'static) {
        self.view.on_register_nxm_handler(callback);
    }

    /// Set callback for Open in Browser button
    pub fn on_open_in_browser(&self, callback: impl Fn() + 'static) {
        self.view.on_open_in_browser(callback);
    }

    /// Set callback for Skip button
    pub fn on_skip_download(&self, callback: impl Fn() + 'static) {
        self.view.on_skip_download(callback);
    }

    /// Set callback for Next button
    pub fn on_next_download(&self, callback: impl Fn() + 'static) {
        self.view.on_next_download(callback);
    }

    /// Set callback for Cancel button
    pub fn on_cancel_download(&self, callback: impl Fn() + 'static) {
        self.view.on_cancel_download(callback);
    }

    /// Run the download view window (blocking)
    pub fn run(&self) -> Result<(), slint::PlatformError> {
        self.view.run()
    }

    /// Get direct access to the view for advanced usage
    pub fn view(&self) -> &DownloadView {
        &self.view
    }
}

impl Default for DownloadViewHandle {
    fn default() -> Self {
        Self::new().expect("Failed to create download view")
    }
}

/// Helper struct for managing installation progress view state
pub struct InstallProgressViewHandle {
    view: InstallProgressView,
}

impl InstallProgressViewHandle {
    /// Create a new installation progress view handle
    pub fn new() -> Result<Self, slint::PlatformError> {
        let view = InstallProgressView::new()?;
        Ok(Self { view })
    }

    /// Set the modlist name being installed
    pub fn set_modlist_name(&self, name: &str) {
        self.view.set_modlist_name(name.into());
    }

    /// Set the current installation phase
    pub fn set_phase(&self, phase: InstallPhase) {
        self.view.set_phase(phase);
    }

    /// Set the current file being processed
    pub fn set_current_file(&self, filename: &str) {
        self.view.set_current_file(filename.into());
    }

    /// Set the overall progress (0.0 to 1.0)
    pub fn set_overall_progress(&self, progress: f32) {
        self.view.set_overall_progress(progress);
    }

    /// Set the current file progress (0.0 to 1.0)
    pub fn set_file_progress(&self, progress: f32) {
        self.view.set_file_progress(progress);
    }

    /// Set the file count (current/total)
    pub fn set_file_counts(&self, current: i32, total: i32) {
        self.view.set_current_file_index(current);
        self.view.set_total_files(total);
    }

    /// Set the installation statistics
    pub fn set_stats(
        &self,
        downloaded_bytes: &str,
        total_bytes: &str,
        extracted_count: i32,
        total_extract: i32,
        installed_count: i32,
        total_install: i32,
        download_speed: &str,
        eta: &str,
    ) {
        self.view.set_stats(InstallStats {
            downloaded_bytes: downloaded_bytes.into(),
            total_bytes: total_bytes.into(),
            extracted_count,
            total_extract,
            installed_count,
            total_install,
            download_speed: download_speed.into(),
            eta: eta.into(),
        });
    }

    /// Set the log text
    pub fn set_log_text(&self, text: &str) {
        self.view.set_log_text(text.into());
    }

    /// Append to the log text
    pub fn append_log(&self, line: &str) {
        let current = self.view.get_log_text();
        let new_text = if current.is_empty() {
            line.to_string()
        } else {
            format!("{}\n{}", current, line)
        };
        self.view.set_log_text(new_text.into());
    }

    /// Set whether the cancel button is enabled
    pub fn set_can_cancel(&self, can_cancel: bool) {
        self.view.set_can_cancel(can_cancel);
    }

    /// Set the paused state
    pub fn set_paused(&self, paused: bool) {
        self.view.set_is_paused(paused);
        if paused {
            self.view.set_phase(InstallPhase::Paused);
        }
    }

    /// Set an error message and show the error panel
    pub fn set_error(&self, message: &str) {
        self.view.set_error_message(message.into());
        self.view.set_has_error(true);
        self.view.set_phase(InstallPhase::Error);
    }

    /// Clear the error state
    pub fn clear_error(&self) {
        self.view.set_error_message("".into());
        self.view.set_has_error(false);
    }

    /// Enable NXM browser mode
    pub fn set_nxm_browser_mode(&self, mod_name: &str, mod_id: i32, file_id: i32, waiting: bool) {
        self.view.set_nxm_state(NxmBrowserState {
            active: true,
            mod_name: mod_name.into(),
            mod_id,
            file_id,
            waiting_for_browser: waiting,
        });
    }

    /// Disable NXM browser mode
    pub fn clear_nxm_browser_mode(&self) {
        self.view.set_nxm_state(NxmBrowserState {
            active: false,
            mod_name: "".into(),
            mod_id: 0,
            file_id: 0,
            waiting_for_browser: false,
        });
    }

    /// Set callback for cancel button
    pub fn on_cancel(&self, callback: impl Fn() + 'static) {
        self.view.on_cancel(callback);
    }

    /// Set callback for pause button
    pub fn on_pause(&self, callback: impl Fn() + 'static) {
        self.view.on_pause(callback);
    }

    /// Set callback for resume button
    pub fn on_resume(&self, callback: impl Fn() + 'static) {
        self.view.on_resume(callback);
    }

    /// Set callback for open in browser button (NXM mode)
    pub fn on_open_browser(&self, callback: impl Fn() + 'static) {
        self.view.on_open_browser(callback);
    }

    /// Set callback for skip download button (NXM mode)
    pub fn on_skip_download(&self, callback: impl Fn() + 'static) {
        self.view.on_skip_download(callback);
    }

    /// Set callback for retry button
    pub fn on_retry(&self, callback: impl Fn() + 'static) {
        self.view.on_retry(callback);
    }

    /// Run the progress view window (blocking)
    pub fn run(&self) -> Result<(), slint::PlatformError> {
        self.view.run()
    }

    /// Get direct access to the view for advanced usage
    pub fn view(&self) -> &InstallProgressView {
        &self.view
    }

    /// Show the window
    pub fn show(&self) -> Result<(), slint::PlatformError> {
        self.view.show()
    }

    /// Hide the window
    pub fn hide(&self) -> Result<(), slint::PlatformError> {
        self.view.hide()
    }
}

impl Default for InstallProgressViewHandle {
    fn default() -> Self {
        Self::new().expect("Failed to create install progress view")
    }
}

// ============================================================================
// Game Detection Helpers
// ============================================================================

/// Map game name from modlist to Steam App ID
fn game_name_to_app_id(game: &str) -> Option<&'static str> {
    // Returns the primary app ID for a game
    // For games with multiple versions, use game_name_to_app_ids for fallbacks
    match game {
        "SkyrimSE" | "SkyrimSpecialEdition" | "Skyrim Special Edition" => Some("489830"),
        "Skyrim" | "SkyrimLE" => Some("72850"),
        "SkyrimVR" => Some("611670"),
        "Fallout4" | "Fallout4SE" | "Fallout 4" => Some("377160"),
        "Fallout4VR" => Some("611660"),
        "FalloutNV" | "FalloutNewVegas" | "Fallout New Vegas" => Some("22380"),
        "Fallout3" | "FO3" => Some("22370"), // GOTY edition (most common)
        "Oblivion" => Some("22330"),
        "Morrowind" => Some("22320"),
        "Enderal" => Some("933480"),
        "EnderalSE" | "EnderalSpecialEdition" => Some("976620"),
        "Starfield" => Some("1716740"),
        "BaldursGate3" | "Baldur's Gate 3" => Some("1086940"),
        _ => None,
    }
}

/// Get all possible app IDs for a game (for games with multiple editions)
fn game_name_to_app_ids(game: &str) -> Vec<&'static str> {
    match game {
        "Fallout3" | "FO3" => vec!["22370", "22300"], // GOTY first, then base
        _ => game_name_to_app_id(game).map(|id| vec![id]).unwrap_or_default(),
    }
}

/// Map game name to human-readable display name
fn game_name_to_display(game: &str) -> &str {
    match game {
        "SkyrimSE" | "SkyrimSpecialEdition" => "Skyrim Special Edition",
        "Skyrim" | "SkyrimLE" => "Skyrim (Legendary Edition)",
        "SkyrimVR" => "Skyrim VR",
        "Fallout4" | "Fallout4SE" => "Fallout 4",
        "Fallout4VR" => "Fallout 4 VR",
        "FalloutNV" | "FalloutNewVegas" => "Fallout New Vegas",
        "Fallout3" | "FO3" => "Fallout 3",
        "Oblivion" => "Oblivion",
        "Morrowind" => "Morrowind",
        "Enderal" => "Enderal",
        "EnderalSE" | "EnderalSpecialEdition" => "Enderal Special Edition",
        "Starfield" => "Starfield",
        "BaldursGate3" => "Baldur's Gate 3",
        _ => game,
    }
}

/// Validate a Nexus Mods API key by making a test request
///
/// Returns true if the key is valid, false otherwise
fn validate_nexus_api_key(api_key: &str) -> bool {
    // Use a simple blocking HTTP client to validate
    // The Nexus API returns user info for valid keys
    let client = match reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };

    let response = client
        .get("https://api.nexusmods.com/v1/users/validate.json")
        .header("apikey", api_key)
        .header("User-Agent", "CLF3/0.0.5")
        .send();

    match response {
        Ok(resp) => resp.status().is_success(),
        Err(_) => false,
    }
}

/// Detect game from a .wabbajack file and log modlist metadata
///
/// Parses the modlist, extracts the game type, looks up the Steam App ID,
/// and attempts to find the game installation path. Also logs metadata
/// (author, game, download size, install size) to the GUI log.
///
/// Returns a formatted string like "Skyrim SE (Steam)"
fn detect_game_from_wabbajack(path: &std::path::Path) -> anyhow::Result<String> {
    use anyhow::Context;

    // Parse the modlist to get game type and metadata
    let modlist = crate::modlist::parse_wabbajack_file(path)
        .context("Failed to parse wabbajack file")?;

    // Calculate sizes
    let download_size: u64 = modlist.archives.iter().map(|a| a.size).sum();
    let install_size: u64 = modlist.directives.iter().map(|d| d.size()).sum();

    // Log metadata to GUI
    let tx = get_progress_sender();
    tx.send(ProgressUpdate::Log(format!(
        "[INFO] Modlist: {} v{}",
        modlist.name, modlist.version
    ))).ok();

    if !modlist.author.is_empty() {
        tx.send(ProgressUpdate::Log(format!(
            "[INFO] Author: {}",
            modlist.author
        ))).ok();
    }

    let game_type = &modlist.game_type;
    let display_name = game_name_to_display(game_type);

    tx.send(ProgressUpdate::Log(format!(
        "[INFO] Game: {}",
        display_name
    ))).ok();
    tx.send(ProgressUpdate::Log(format!(
        "[INFO] Download Size: {}",
        format_bytes(download_size)
    ))).ok();
    tx.send(ProgressUpdate::Log(format!(
        "[INFO] Install Size: {}",
        format_bytes(install_size)
    ))).ok();
    tx.send(ProgressUpdate::Log(format!(
        "[INFO] Archives: {}, Directives: {}",
        modlist.archives.len(),
        modlist.directives.len()
    ))).ok();

    // Try to find the game installation with platform info (try all possible app IDs)
    let app_ids = game_name_to_app_ids(game_type);
    println!("[DEBUG] Looking for game_type={}, app_ids={:?}", game_type, app_ids);
    if !app_ids.is_empty() {
        // Use detect_all_games to get full game info including launcher
        let scan_result = crate::game_finder::detect_all_games();
        println!("[DEBUG] Found {} games total", scan_result.games.len());
        for g in &scan_result.games {
            println!("[DEBUG]   - {} (app_id={})", g.name, g.app_id);
        }
        for app_id in &app_ids {
            println!("[DEBUG] Checking for app_id={}", app_id);
            if let Some(game) = scan_result.find_by_app_id(app_id) {
                let platform = game.launcher.display_name();
                tx.send(ProgressUpdate::Log(format!(
                    "[INFO] Found game at: {}",
                    game.install_path.display()
                ))).ok();
                return Ok(format!("{} ({})", display_name, platform));
            }
        }
    }

    // Game type known but not installed
    tx.send(ProgressUpdate::Log(format!(
        "[WARN] Game not found: {}",
        display_name
    ))).ok();
    Ok(format!("{} (not found)", display_name))
}

/// Result of detecting game and TTW requirements from a wabbajack file
struct ModlistDetectionResult {
    /// Game display string (e.g., "Fallout New Vegas (Steam)")
    game_display: String,
    /// Whether TTW is required
    ttw_required: bool,
    /// Fallout 3 path if found
    fo3_path: Option<std::path::PathBuf>,
    /// Fallout New Vegas path if found
    fnv_path: Option<std::path::PathBuf>,
    /// Modlist name (for config cache)
    modlist_name: String,
    /// Modlist version (for config cache)
    modlist_version: String,
    /// Cached install directory
    cached_install_dir: Option<String>,
    /// Cached downloads directory
    cached_downloads_dir: Option<String>,
    /// Cached TTW MPI path
    cached_ttw_mpi_path: Option<String>,
}

/// Detect game and TTW requirements from a wabbajack file
fn detect_modlist_info(path: &std::path::Path) -> anyhow::Result<ModlistDetectionResult> {
    use anyhow::Context;

    // Parse the modlist
    let modlist = crate::modlist::parse_wabbajack_file(path)
        .context("Failed to parse wabbajack file")?;

    // Calculate sizes
    let download_size: u64 = modlist.archives.iter().map(|a| a.size).sum();
    let install_size: u64 = modlist.directives.iter().map(|d| d.size()).sum();

    // Log metadata
    let tx = get_progress_sender();
    tx.send(ProgressUpdate::Log(format!(
        "[INFO] Modlist: {} v{}",
        modlist.name, modlist.version
    ))).ok();

    if !modlist.author.is_empty() {
        tx.send(ProgressUpdate::Log(format!(
            "[INFO] Author: {}",
            modlist.author
        ))).ok();
    }

    let game_type = &modlist.game_type;
    let display_name = game_name_to_display(game_type);

    tx.send(ProgressUpdate::Log(format!("[INFO] Game: {}", display_name))).ok();
    tx.send(ProgressUpdate::Log(format!("[INFO] Download Size: {}", format_bytes(download_size)))).ok();
    tx.send(ProgressUpdate::Log(format!("[INFO] Install Size: {}", format_bytes(install_size)))).ok();
    tx.send(ProgressUpdate::Log(format!(
        "[INFO] Archives: {}, Directives: {}",
        modlist.archives.len(),
        modlist.directives.len()
    ))).ok();

    // Detect TTW requirement
    let ttw_result = modlist.requires_ttw();
    let ttw_required = ttw_result.required;

    if ttw_required {
        tx.send(ProgressUpdate::Log(format!(
            "[INFO] TTW Required: Yes (detected: {})",
            ttw_result.markers_found.join(", ")
        ))).ok();
    }

    // Use detect_all_games for game detection
    let scan_result = crate::game_finder::detect_all_games();

    // Find Fallout 3 (for TTW)
    let fo3_path = crate::ttw::find_fallout3(&scan_result);
    if ttw_required {
        if let Some(ref path) = fo3_path {
            tx.send(ProgressUpdate::Log(format!("[INFO] Fallout 3 found: {}", path.display()))).ok();
        } else {
            tx.send(ProgressUpdate::Log("[WARN] Fallout 3 not found - required for TTW".to_string())).ok();
        }
    }

    // Find Fallout New Vegas (for TTW)
    let fnv_path = crate::ttw::find_fallout_nv(&scan_result);
    if ttw_required {
        if let Some(ref path) = fnv_path {
            tx.send(ProgressUpdate::Log(format!("[INFO] Fallout NV found: {}", path.display()))).ok();
        }
    }

    // Find the main game for display
    let app_ids = game_name_to_app_ids(game_type);
    let game_display = if !app_ids.is_empty() {
        let mut found = None;
        for app_id in &app_ids {
            if let Some(game) = scan_result.find_by_app_id(app_id) {
                let platform = game.launcher.display_name();
                tx.send(ProgressUpdate::Log(format!(
                    "[INFO] Found game at: {}",
                    game.install_path.display()
                ))).ok();
                found = Some(format!("{} ({})", display_name, platform));
                break;
            }
        }
        found.unwrap_or_else(|| {
            tx.send(ProgressUpdate::Log(format!("[WARN] Game not found: {}", display_name))).ok();
            format!("{} (not found)", display_name)
        })
    } else {
        format!("{} (unknown)", display_name)
    };

    // Load cached config for this modlist
    let (cached_install_dir, cached_downloads_dir, cached_ttw_mpi_path) =
        match crate::installer::ConfigCache::open() {
            Ok(cache) => {
                match cache.get_config(&modlist.name, &modlist.version) {
                    Ok(Some(config)) => {
                        tx.send(ProgressUpdate::Log("[INFO] Loaded cached configuration".to_string())).ok();
                        (config.install_dir, config.downloads_dir, config.ttw_mpi_path)
                    }
                    _ => (None, None, None),
                }
            }
            Err(_) => (None, None, None),
        };

    Ok(ModlistDetectionResult {
        game_display,
        ttw_required,
        fo3_path,
        fnv_path,
        modlist_name: modlist.name,
        modlist_version: modlist.version,
        cached_install_dir,
        cached_downloads_dir,
        cached_ttw_mpi_path,
    })
}

/// Format bytes into human-readable string (B, KiB, MiB, GiB)
fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;

    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.0} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format ETA in seconds to human-readable string (HH:MM:SS or MM:SS)
fn format_eta(seconds: f64) -> String {
    if seconds.is_nan() || seconds.is_infinite() || seconds < 0.0 {
        return "--:--".to_string();
    }

    let secs = seconds as u64;
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    let secs = secs % 60;

    if hours > 0 {
        format!("{}:{:02}:{:02}", hours, mins, secs)
    } else {
        format!("{:02}:{:02}", mins, secs)
    }
}

/// Run a Wabbajack modlist installation
///
/// This is called from a background thread with its own tokio runtime.
async fn run_wabbajack_install(
    source_path: &str,
    install_dir: &str,
    downloads_dir: &str,
    api_key: &str,
    non_premium: bool,
) -> anyhow::Result<()> {
    use crate::installer::{InstallConfig, Installer, ProgressCallback, ProgressEvent};
    use std::path::PathBuf;
    use std::sync::Arc;

    let tx = get_progress_sender();

    // Send initial status
    tx.send(ProgressUpdate::Phase("Validating".to_string())).ok();
    tx.send(ProgressUpdate::Status("Parsing modlist...".to_string())).ok();
    tx.send(ProgressUpdate::Log("[INFO] Starting Wabbajack installation...".to_string())).ok();

    println!("[GUI] Starting Wabbajack installation...");
    println!("[GUI] Non-premium mode: {}", non_premium);

    // Parse the modlist to detect game
    let source = std::path::Path::new(source_path);
    let modlist = match crate::modlist::parse_wabbajack_file(source) {
        Ok(m) => m,
        Err(e) => {
            tx.send(ProgressUpdate::Error(format!("Failed to parse modlist: {}", e))).ok();
            return Err(e);
        }
    };
    let game_type = &modlist.game_type;

    tx.send(ProgressUpdate::Log(format!("[INFO] Detected game type: {}", game_type))).ok();
    tx.send(ProgressUpdate::Status(format!("Detected game: {}", game_type))).ok();

    // Find the game installation path (try all possible app IDs for games with variants)
    let app_ids = game_name_to_app_ids(game_type);
    let game_dir = if app_ids.is_empty() {
        let err = format!("Unknown game type: {}", game_type);
        tx.send(ProgressUpdate::Error(err.clone())).ok();
        anyhow::bail!(err);
    } else {
        let mut found_path = None;
        for app_id in &app_ids {
            if let Some(path) = crate::game_finder::find_game_install_path(app_id) {
                tx.send(ProgressUpdate::Log(format!("[INFO] Found game at: {}", path.display()))).ok();
                found_path = Some(path);
                break;
            }
        }
        match found_path {
            Some(path) => path,
            None => {
                let err = format!("Game not found: {}. Please ensure the game is installed.", game_type);
                tx.send(ProgressUpdate::Error(err.clone())).ok();
                anyhow::bail!(err);
            }
        }
    };

    println!("[GUI] Detected game: {} at {}", game_type, game_dir.display());

    // Get thread count for concurrency
    let thread_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    // Calculate total download size from archives
    let total_download_size: u64 = modlist.archives.iter().map(|a| a.size).sum();
    tx.send(ProgressUpdate::SizeProgress(format!("0 B / {}", format_bytes(total_download_size)))).ok();

    // Track speeds from all concurrent downloads
    let active_speeds: Arc<std::sync::Mutex<std::collections::HashMap<String, f64>>> =
        Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));

    // Track per-file downloaded bytes for cumulative progress
    let download_progress: Arc<std::sync::Mutex<std::collections::HashMap<String, u64>>> =
        Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));

    // Track completed archive sizes
    let completed_bytes: Arc<std::sync::atomic::AtomicU64> =
        Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Create progress callback that sends updates to GUI
    let progress_tx = tx.clone();
    let speeds = active_speeds.clone();
    let dl_progress = download_progress.clone();
    let completed = completed_bytes.clone();
    let total_size = total_download_size;
    let progress_callback: ProgressCallback = Arc::new(move |event| {
        match event {
            // Download events
            ProgressEvent::DownloadStarted { name, size: _ } => {
                // Register this download
                if let Ok(mut map) = speeds.lock() {
                    map.insert(name.clone(), 0.0);
                }
                if let Ok(mut map) = dl_progress.lock() {
                    map.insert(name, 0);
                }
                progress_tx.send(ProgressUpdate::Status("Downloading...".to_string())).ok();
            }
            ProgressEvent::DownloadProgress { name, downloaded, total: _, speed } => {
                // Update this download's progress
                if let Ok(mut map) = dl_progress.lock() {
                    map.insert(name.clone(), downloaded);
                }

                // Update this download's speed and sum all active speeds
                if let Ok(mut map) = speeds.lock() {
                    map.insert(name, speed);
                    let total_speed: f64 = map.values().sum();
                    let speed_mib = total_speed / (1024.0 * 1024.0);
                    progress_tx.send(ProgressUpdate::DownloadSpeed(format!("{:.1} MiB/s", speed_mib))).ok();

                    // Calculate total downloaded bytes
                    let completed_so_far = completed.load(std::sync::atomic::Ordering::Relaxed);
                    let active_bytes: u64 = dl_progress.lock().map(|m| m.values().sum()).unwrap_or(0);
                    let current_downloaded = completed_so_far + active_bytes;

                    // Send size progress
                    progress_tx.send(ProgressUpdate::SizeProgress(
                        format!("{} / {}", format_bytes(current_downloaded), format_bytes(total_size))
                    )).ok();

                    // Calculate and send ETA
                    if total_speed > 0.0 && current_downloaded < total_size {
                        let remaining = total_size - current_downloaded;
                        let eta_secs = remaining as f64 / total_speed;
                        progress_tx.send(ProgressUpdate::Eta(format_eta(eta_secs))).ok();
                    }
                }
            }
            ProgressEvent::DownloadComplete { name } => {
                // Get the file's final size and add to completed
                if let Ok(mut map) = dl_progress.lock() {
                    if let Some(&bytes) = map.get(&name) {
                        completed.fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
                    }
                    map.remove(&name);
                }

                // Remove from active speeds
                if let Ok(mut map) = speeds.lock() {
                    map.remove(&name);
                    if map.is_empty() {
                        progress_tx.send(ProgressUpdate::DownloadSpeed("--".to_string())).ok();
                        progress_tx.send(ProgressUpdate::Eta("--:--".to_string())).ok();
                    } else {
                        let total_speed: f64 = map.values().sum();
                        let speed_mib = total_speed / (1024.0 * 1024.0);
                        progress_tx.send(ProgressUpdate::DownloadSpeed(format!("{:.1} MiB/s", speed_mib))).ok();
                    }
                }
            }
            ProgressEvent::ArchiveComplete { index, total } => {
                progress_tx.send(ProgressUpdate::FileCount(index as i32, total as i32)).ok();
                let overall_progress = index as f32 / total as f32;
                progress_tx.send(ProgressUpdate::OverallProgress(overall_progress)).ok();
            }
            ProgressEvent::DownloadSkipped { count, total_size: skipped_size } => {
                // Add skipped archive sizes to completed bytes so progress is accurate
                completed.fetch_add(skipped_size, std::sync::atomic::Ordering::Relaxed);
                // Update the size progress display
                let current_downloaded = completed.load(std::sync::atomic::Ordering::Relaxed);
                progress_tx.send(ProgressUpdate::SizeProgress(
                    format!("{} / {}", format_bytes(current_downloaded), format_bytes(total_size))
                )).ok();
                progress_tx.send(ProgressUpdate::Log(
                    format!("[INFO] Skipped {} already-downloaded archives ({} total)", count, format_bytes(skipped_size))
                )).ok();
            }
            // Directive/extraction events
            ProgressEvent::PhaseChange { phase } => {
                progress_tx.send(ProgressUpdate::Phase(phase.clone())).ok();
                // When switching to Installing/Processing phase, reset progress display
                if phase == "Installing" || phase == "Processing" {
                    progress_tx.send(ProgressUpdate::DownloadSpeed("--".to_string())).ok();
                    progress_tx.send(ProgressUpdate::Eta("--:--".to_string())).ok();
                    progress_tx.send(ProgressUpdate::SizeProgress("Processing directives...".to_string())).ok();
                    progress_tx.send(ProgressUpdate::OverallProgress(0.0)).ok();
                }
            }
            ProgressEvent::DirectivePhaseStarted { directive_type, total } => {
                progress_tx.send(ProgressUpdate::Status(format!("Processing {} ({} directives)...", directive_type, total))).ok();
                progress_tx.send(ProgressUpdate::SizeProgress(format!("0 / {} {}", total, directive_type))).ok();
                progress_tx.send(ProgressUpdate::FileCount(0, total as i32)).ok();
            }
            ProgressEvent::DirectiveStarted { index, total, name } => {
                progress_tx.send(ProgressUpdate::FileProgress(name, index as f32 / total as f32)).ok();
                progress_tx.send(ProgressUpdate::SizeProgress(format!("{} / {} directives", index, total))).ok();
            }
            ProgressEvent::DirectiveComplete { index, total } => {
                progress_tx.send(ProgressUpdate::FileCount(index as i32, total as i32)).ok();
                progress_tx.send(ProgressUpdate::OverallProgress(index as f32 / total as f32)).ok();
                progress_tx.send(ProgressUpdate::SizeProgress(format!("{} / {} directives", index, total))).ok();
            }
            ProgressEvent::Status { message } => {
                progress_tx.send(ProgressUpdate::Status(message)).ok();
            }
            ProgressEvent::Log { message } => {
                progress_tx.send(ProgressUpdate::Log(format!("[INFO] {}", message))).ok();
            }
        }
    });

    // Build config
    let config = InstallConfig {
        wabbajack_path: PathBuf::from(source_path),
        output_dir: PathBuf::from(install_dir),
        downloads_dir: PathBuf::from(downloads_dir),
        game_dir,
        nexus_api_key: api_key.to_string(),
        max_concurrent_downloads: thread_count,
        nxm_mode: non_premium,
        nxm_port: 8007,
        browser: "xdg-open".to_string(),
        progress_callback: Some(progress_callback),
    };

    // Create installer
    tx.send(ProgressUpdate::Status("Initializing installer...".to_string())).ok();
    let mut installer = match Installer::new(config) {
        Ok(i) => i,
        Err(e) => {
            tx.send(ProgressUpdate::Error(format!("Failed to create installer: {}", e))).ok();
            return Err(e);
        }
    };

    // Count archives for progress tracking
    let archive_count = modlist.archives.len();
    let directive_count = modlist.directives.len();
    tx.send(ProgressUpdate::Log(format!("[INFO] {} archives, {} directives to process", archive_count, directive_count))).ok();
    tx.send(ProgressUpdate::FileCount(0, archive_count as i32)).ok();

    // Phase: Downloading
    tx.send(ProgressUpdate::Phase("Downloading".to_string())).ok();
    tx.send(ProgressUpdate::Status("Downloading archives...".to_string())).ok();
    tx.send(ProgressUpdate::Log("[INFO] Starting download phase...".to_string())).ok();

    // Use streaming pipeline for better performance
    let stats = match installer.run_streaming(8, 8).await {
        Ok(s) => s,
        Err(e) => {
            tx.send(ProgressUpdate::Error(format!("Installation failed: {}", e))).ok();
            return Err(e);
        }
    };

    // Send completion stats
    tx.send(ProgressUpdate::Log(format!(
        "[INFO] Downloads: {} downloaded, {} skipped, {} manual, {} failed",
        stats.archives_downloaded, stats.archives_skipped,
        stats.archives_manual, stats.archives_failed
    ))).ok();
    let total_processed = stats.directives_completed + stats.directives_skipped + stats.directives_failed;
    tx.send(ProgressUpdate::Log(format!(
        "[INFO] Directives: {} new, {} existing, {} failed ({} total)",
        stats.directives_completed, stats.directives_skipped, stats.directives_failed, total_processed
    ))).ok();

    println!("[GUI] Installation Summary:");
    println!("[GUI]   Downloads: {} downloaded, {} skipped, {} manual, {} failed",
        stats.archives_downloaded, stats.archives_skipped,
        stats.archives_manual, stats.archives_failed);
    println!("[GUI]   Directives: {} new, {} existing, {} failed ({} total)",
        stats.directives_completed, stats.directives_skipped, stats.directives_failed, total_processed);

    if stats.archives_manual > 0 || stats.archives_failed > 0 {
        // Log failed downloads with details
        for fd in &stats.failed_downloads {
            tx.send(ProgressUpdate::Log(format!(
                "[FAILED] {}: {}",
                fd.name, fd.error
            ))).ok();
            tx.send(ProgressUpdate::Log(format!(
                "         URL: {}",
                fd.url
            ))).ok();
        }

        // Log manual downloads with details
        for md in &stats.manual_downloads {
            tx.send(ProgressUpdate::Log(format!(
                "[MANUAL] {}: {}",
                md.name, md.url
            ))).ok();
            if let Some(prompt) = &md.prompt {
                tx.send(ProgressUpdate::Log(format!(
                    "         Note: {}",
                    prompt
                ))).ok();
            }
        }

        let err = format!("Some archives need manual download. {} manual, {} failed.",
            stats.archives_manual, stats.archives_failed);
        tx.send(ProgressUpdate::Error(err.clone())).ok();
        anyhow::bail!(err);
    }

    if stats.directives_failed > 0 {
        let err = format!("Some directives failed: {}", stats.directives_failed);
        tx.send(ProgressUpdate::Error(err.clone())).ok();
        anyhow::bail!(err);
    }

    // Success! (Complete will be sent by the caller after NaK finishes)
    tx.send(ProgressUpdate::Log("[INFO] Installation completed successfully!".to_string())).ok();

    Ok(())
}

