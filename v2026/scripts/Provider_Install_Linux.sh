#!/bin/sh
# Credits: Ar Rakin, Ryan Mello
# urnet-tools -- URnetwork manager script (also acts as an installation script)
# GitHub: <https://github.com/urnetwork/connect>
me="$0"
script_rundir="$(pwd)"

if [ "$me" = "sh" ] || [ "$me" = "bash" ] || [ "$me" = "zsh" ] || [ "$URNETWORK_TOOLS_MODE" = "1" ]; then
    me="urnet-tools"
fi

show_help ()
{
    echo "Usage: "
    
    if [ -n "$URNETWORK_TOOLS_MODE" ]; then
        echo "  $me [options] start"
        echo "  $me [options] stop"
        echo "  $me [options] update"
        echo "  $me [options] status"
        echo "  $me [options] reinstall [-t=TAG] [-B]"
        echo "  $me [options] uninstall [-B]"
        echo "  $me [options] auto-update [on | off] [--interval=INTERVAL]"
        echo "  $me [options] auto-start [on | off]"
        echo "  $me [-h] [-v]"
    else
	    echo "  $me [options] [-t=TAG] [-B]"
    fi
    
    echo ""

    if [ -z "$URNETWORK_TOOLS_MODE" ]; then    
        echo "Installs URnetwork locally for the current user."
    else
	    echo "Manages URnetwork installation."
    fi
    
    if [ -n "$URNETWORK_TOOLS_MODE" ]; then
        echo ""
        echo "Operational modes:"
        echo "  start                   Start URnetwork provider"
        echo "  stop                    Stop URnetwork provider"
        echo "  update                  Upgrade URnetwork, if updates are available"
        echo "  status                  Show the status of URnetwork provider service"
        echo "  reinstall               Reinstall URnetwork"
        echo "  uninstall               Uninstall URnetwork"
        echo "  auto-update             Manage auto update settings.  If no argument is"
        echo "                          specified, it will print the current auto update state."
		echo "  auto-start              Turn auto-start of URnetwork provider on login on or off"
        echo ""
        echo "Options for reinstall:"
        echo "  -t, --tag=TAG           Reinstall a specific version of URnetwork."
        echo "  -B, --no-modify-bashrc  Do not modify ~/.bashrc"
        echo ""
        echo "Options for uninstall:"
        echo "  -B, --no-modify-bashrc  Do not modify ~/.bashrc"
        echo ""
        echo "Options for auto-update:"
        echo "  --interval=INTERVAL     Auto update interval.  Values can be:"
        echo "                          daily, weekly, monthly.  Defaults to daily."
    fi

    echo ""
    echo "Options for install:"
    echo "  -t, --tag=TAG           Reinstall a specific version of URnetwork."
    echo "  -B, --no-modify-bashrc  Do not modify ~/.bashrc"
    
    echo ""
    echo "Global Options:"
    echo "  -h, --help              Show this help and exit"
    echo "  -v, --version           Show the version of URnetwork that's installed"
    echo "  -i, --install=[PATH]    Installation path"
    echo ""
    echo "Refer to the online documentation for more help."
}

get_arch ()
{
    if command -v arch > /dev/null; then
        arch="$(arch)"
    else
        arch="$(uname -m)"
    fi

    case "$arch" in
        i386|i686)
            arch=386
            ;;

        x86_64)
            arch=amd64
            ;;

        aarch64)
            arch=arm64
            ;;
    esac

    echo "$arch"
}

operation=""
arch="$(get_arch)"
has_systemd=0
update_timer_oncalendar=daily

api_base="https://api.github.com/repos/urnetwork/connect"

install_path="$HOME/.local/share/urnetwork-provider"
version_file="$install_path/.version"

if [ -z "$URNETWORK_TOOLS_MODE" ]; then
    operation="install"
fi

if command -v systemctl > /dev/null; then
    has_systemd=1
fi

pr_err ()
{
    argv0="$me"
    fmt="$1"
    shift

    if [ -t 2 ]; then
        argv0="\033[1m$me\033[0m"
    fi

    if [ -n "$operation" ]; then
        argv0="$argv0: $operation"
    fi

    # shellcheck disable=SC2068
    # shellcheck disable=SC2059
    printf "$argv0: $fmt\n" $@ >&2
}

pr_info ()
{
    argv0="$me"
    fmt="$1"
    shift

    if [ -t 1 ]; then
        argv0="\033[1m$me\033[0m"
    fi

    if [ -n "$operation" ]; then
        argv0="$argv0: $operation"
    fi

    # shellcheck disable=SC2068
    # shellcheck disable=SC2059
    printf "$argv0: $fmt\n" $@
}

opt_requires_arg ()
{
    pr_err "Option '%s' requires an argument" "$1"
    pr_err "Try '$me --help' for more information"
}

get_version_from_api_response () 
{    
    if command -v jq > /dev/null; then
        latest_version="$(echo "$1" | tr -d '\000-\037' | jq -r '.tag_name')"
    elif command -v python3 > /dev/null; then
        latest_version="$(echo "$1" | tr -d '\000-\037' | python3 -c 'import sys, json; data = json.load(sys.stdin); print(data["tag_name"])')"
    else
        pr_err "Neither python3 nor jq is available"
        exit 1
    fi

    echo "$latest_version"
}

# shellcheck disable=SC2317
get_release_date_from_api_response () 
{   
    if command -v jq > /dev/null; then
        date="$(echo "$1" | tr -d '\000-\037' | jq -r '.published_at | fromdateiso8601')" 
    elif command -v python3 > /dev/null; then
        date="$(echo "$1" | tr -d '\000-\037' | python3 -c 'import sys, json; from datetime import datetime, timezone; data = json.load(sys.stdin); print(int(datetime.fromisoformat(data["published_at"]).replace(tzinfo=timezone.utc).timestamp() * 1000))')"
    else
        pr_err "Neither python3 nor jq is available"
        exit 1
    fi

    echo "$date"
}

# shellcheck disable=SC2317
get_current_date () 
{
    if command -v jq > /dev/null; then
        date="$(jq -n 'now * 1000 | floor')"
    elif command -v python3 > /dev/null; then
        date="$(python3 -c 'from datetime import datetime, timezone; now = datetime.now(timezone.utc); print(int(now.timestamp() * 1000));')"
    else
        pr_err "Neither python3 nor jq is available"
        exit 1
    fi

    echo "$date"
}

network_fetch ()
{
    if command -v curl > /dev/null; then
        curl -fSsL "$1"
        return $?
    elif command -v wget > /dev/null; then
        wget -qO- "$1"
        return $?
    else
        pr_err "Neither curl nor wget is available"
        exit 1
    fi
}

show_version () 
{
    if [ ! -f "$version_file" ]; then
        pr_err "version file '$version_file' could not be found"
        exit 1
    fi

    version="$(cat "$version_file")"

    echo "Current version: $version"
    
    api_url="$api_base/releases/latest"
    release="$(network_fetch "$api_url")"
    latest_version="$(get_version_from_api_response "$release")"

    if [ -z "$latest_version" ]; then
        pr_err "Could not fetch any information about the latest release"
        exit 1
    fi

    if [ "$latest_version" != "$version" ]; then
        echo "Latest version (Update available): $latest_version"
    fi
}

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            show_help
            exit 0
            ;;

        -v|--version)
            show_version
            exit 0
            ;;

        -i|--install)
            if [ -z "$2" ]; then
                opt_requires_arg "$1"
                exit 1
            fi

            install_path="$2"
            shift 2
            ;;

        --)
            shift
            break
            ;;

        -*)
            pr_err "Invalid option '%s'" "$1"
            exit 1
            ;;

        *)
            break
            ;;
    esac
done

if [ -n "$1" ]; then
    operation="$1"
    shift
fi

if [ -z "$operation" ]; then
    show_help >&2
    exit 1
fi

systemd_userdir="$HOME/.config/systemd/user"
systemd_service="$systemd_userdir/urnetwork.service"
systemd_update_service="$systemd_userdir/urnetwork-update.service"
systemd_update_timer="$systemd_userdir/urnetwork-update.timer"
systemd_units_stopped=0

stop_systemd_units ()
{
    if [ -f "$systemd_service" ]; then
        if [ "$(systemctl --user is-active urnetwork.service)" = "active" ]; then
            pr_err "warning: urnetwork.service is running, it will be stopped to perform an update/reinstall"
	        pr_err "warning: It will be started again automatically, once the update finishes"
            pr_err "warning: You will need to restart this service after this update/reinstall if auto start fails"
	        systemd_units_stopped=1
        fi

        systemctl --user disable --now urnetwork.service || {
            pr_err "Failed to disable urnetwork.service early before update/reinstall; continuing anyway"
        }
	
        systemctl --user disable --now urnetwork-update.timer || {
            pr_err "Failed to disable urnetwork-update.timer early before update/reinstall; continuing anyway"
        }
    fi
}

install_systemd_units ()
{
    start="$systemd_units_stopped"

    pr_info "Installing urnetwork.service in %s" "$systemd_service"
    mkdir -p "$systemd_userdir"
    
    cat > "$systemd_service" <<EOF
[Unit]
Description=URnetwork Provider

[Service]
ExecStart=$install_path/bin/urnetwork provide
Restart=no

[Install]
WantedBy=default.target
EOF
    
    pr_info "Installing urnetwork-update.service in %s" "$systemd_update_service"
    cat > "$systemd_update_service" <<EOF
[Unit]
Description=URnetwork Update

[Service]
Type=oneshot
ExecStart=$install_path/bin/urnet-tools update
EOF
    
    pr_info "Installing urnetwork-update.timer in %s" "$systemd_update_timer"
    cat > "$systemd_update_timer" <<EOF
[Unit]
Description=Run URnetwork Update

[Timer]
OnCalendar=$update_timer_oncalendar
Persistent=true

[Install]
WantedBy=default.target
EOF

    if ! systemctl --user enable urnetwork.service; then
        pr_err "Could not enable the newly installed systemd service"
        exit 1
    fi
    
    if ! systemctl --user enable urnetwork-update.timer; then
        pr_err "Could not enable the newly installed update timer"
        exit 1
    fi

    if [ "$start" -eq 1 ]; then
        systemctl --user daemon-reload
        
        if ! systemctl --user start urnetwork.service; then
            pr_err "warning: Unable to restart urnetwork.service after update; please manually start it"
        fi
    fi
}

do_install ()
{
    tag="latest"
    no_modify_bashrc=0

    case "$operation" in
        install)
            if [ -n "$URNETWORK_TOOLS_MODE" ]; then
                pr_err "Invalid operation '%s'" "$operation"
                exit 1
            fi

            while [ $# -gt 0 ]; do
                case "$1" in
                    -t|--tag)
                        if [ -z "$2" ]; then
                            opt_requires_arg "$1"
                            exit 1
                        fi

                        tag="$2"

                        if [ "$tag" != "latest" ] && [ "$(echo "$tag" | cut -c -1)" != "v" ]; then
                            tag="v$tag"
                        fi 

                        shift 2
                        ;;

                    -B|--no_modify_bashrc)
                        no_modify_bashrc=1
                        shift
                        ;;

                    -*)
                        pr_err "Invalid option '%s'" "$1"
                        exit 1
                        ;;

                    *)
                        pr_err "Invalid argument '%s'" "$1"
                        exit 1
                        ;;
                esac
            done

            ;;

        update)
            no_modify_bashrc=1

            while [ $# -gt 0 ]; do
                case "$1" in
                    -*)
                        pr_err "Invalid option '%s'" "$1"
                        exit 1
                        ;;

                    *)
                        pr_err "Invalid argument '%s'" "$1"
                        exit 1
                        ;;
                esac
            done
            
            ;;

        reinstall)
            if [ ! -f "$version_file" ]; then
                pr_err "Could not determine the currently installed version"
                exit 1
            fi

	   		tag="$(cat "$version_file")"

            while [ $# -gt 0 ]; do
                case "$1" in
                    -t|--tag)
                        if [ -z "$2" ]; then
                            opt_requires_arg "$1"
                            exit 1
                        fi

                        tag="$2"

                        if [ "$tag" != "latest" ] && [ "$(echo "$tag" | cut -c -1)" != "v" ]; then
                            tag="v$tag"
                        fi 

                        shift 2
                        ;;

                    -B|--no_modify_bashrc)
                        no_modify_bashrc=1
                        shift
                        ;;

                    -*)
                        pr_err "Invalid option '%s'" "$1"
                        exit 1
                        ;;

                    *)
                        pr_err "Invalid argument '%s'" "$1"
                        exit 1
                        ;;
                esac
            done
            ;;
    esac

    api_url=""

    if [ "$tag" = "latest" ] || [ -z "$tag" ]; then
        tag=latest
        api_url="$api_base/releases/latest"
    else
        api_url="$api_base/releases/tags/$tag"
    fi

    pr_info "Fetching release information for tag: %s" "$tag"
    
    if ! release="$(network_fetch "$api_url")"; then
        pr_err "Failed to fetch release information for tag: %s" "$tag"
        exit 1
    fi

    version_to_install="$(get_version_from_api_response "$release")"
    release_date="$(get_release_date_from_api_response "$release")"

    if [ "$operation" = "update" ] && [ -f "$install_path/.date" ] && [ -f "$install_path/.version" ]; then
        install_release_date="$(cat "$install_path/.date")"
        installed_version="$(cat "$install_path/.version")"

        if [ "$install_release_date" -lt "$release_date" ]; then
            pr_info "Version %s is newer than the installed version %s" "$version_to_install" "$installed_version"
            pr_info "Continuing upgrade"
        else
            pr_info "Installed version is up-to-date"
            exit 0
        fi
    fi

    dl_url=""

    if command -v jq > /dev/null; then
        asset="$(echo "$release" | tr -d '\000-\037' | jq -r '.assets[] | select(.name | startswith("urnetwork-provider-"))')"

        if [ -z "$asset" ]; then
            pr_err "Could not find a suitable release asset for tag: %s" "$tag"
            exit 1
        fi

        dl_url="$(echo "$asset" | jq -r '.browser_download_url')"
    elif command -v python3 > /dev/null; then
        dl_url="$(echo "$release" | tr -d '\000-\037' | python3 -c 'import sys, json; data = json.load(sys.stdin); assets = data["assets"]; asset = next(a for a in assets if a["name"].startswith("urnetwork-provider")); print(asset["browser_download_url"] if asset else "")')"
    else
        pr_err "Neither python3 nor jq is available"
        exit 1
    fi

    if [ -z "$dl_url" ]; then
        pr_err "No download URL could be found for tag: %s" "$tag"
        exit 1
    fi
    
    pr_info "Downloading: %s" "$dl_url"
    
    if ! workdir="$(mktemp -d)"; then
        pr_err "Failed to create working directory"
        exit 1
    fi
    
    cd "$workdir" || exit 1

    tarball="$workdir/urnetwork.tar.gz"
    bindir="$workdir/linux/$arch"
    bin_program="$bindir/provider"

    trap 'rm -r "$workdir"' EXIT 
    trap 'exit 1' INT TERM

    if [ -z "$URNETWORK_NO_DOWNLOAD_TARBALL" ]; then
        if command -v curl > /dev/null; then
            if ! curl --progress-bar -L "$dl_url" -o "$tarball"; then
                pr_err "Failed to download $dl_url"
                exit 1
            fi
        elif command -v wget > /dev/null; then
            if ! wget -O "$tarball" "$dl_url"; then 
                pr_err "Failed to download $dl_url"
                exit 1
            fi
        else
            pr_err "Neither curl nor wget is available"
            exit 1
        fi

        if ! tar -xf "$tarball" 2>/dev/null; then
            pr_err "Failed to extract tarball: %s" "$tarball"
            exit 1
        fi

        if [ ! -f "$bin_program" ]; then
            pr_err "Provider binary was not found in the tarball!"
            pr_err "This indicates an issue with the tarball that was downloaded."
            exit 1
        fi
    fi
	
    if [ "$has_systemd" -eq 1 ]; then
        stop_systemd_units
    fi

    if [ -d "$install_path" ] && [ "$operation" = "install" ]; then
        pr_info "Found existing installation in $install_path, updating instead"
        operation=update
        no_modify_bashrc=1
    else
        if [ ! -d "$install_path" ]; then
            pr_info "Creating directory '%s'" "$install_path"

            if ! mkdir -p "$install_path"; then
                pr_err "Failed to create directory '%s'" "$install_path"
                exit 1
            fi
        fi

        if ! mkdir -p "$install_path/bin"; then
            pr_err "Failed to create directory '%s'" "$install_path/bin"
            exit 1
        fi
    fi

    if [ -z "$URNETWORK_NO_DOWNLOAD_TARBALL" ]; then
        cp "$bin_program" "$install_path/bin/urnetwork" || { pr_err "Failed to install provider binary"; exit 1; }
        chmod 755 "$install_path/bin/urnetwork" || { pr_err "Failed to install provider binary"; exit 1; }
    fi

    cd "$script_rundir" || exit 1
    script="$(cat "$0" 2>/dev/null)"

    if [ -z "$script" ]; then
        pr_err "Script path is '%s', cannot operate on it" "$0"

        if ! script="$(network_fetch https://raw.githubusercontent.com/urnetwork/connect/refs/heads/main/scripts/Provider_Install_Linux.sh)"; then
            pr_err "Failed to fetch script contents"
            exit 1
        fi
    fi

    cd "$workdir" || exit 1
    
    if [ -z "$script" ]; then
        pr_err "Invalid script contents"
        exit 1
    fi

    rm -f "$install_path/bin/urnet-tools"
    printf "%s\n" "$script" | head -n1 > "$install_path/bin/urnet-tools"

    {
        echo "URNETWORK_TOOLS_MODE=1"; 
    } >> "$install_path/bin/urnet-tools"

    printf "%s\n" "$script" | tail -n +2 >> "$install_path/bin/urnet-tools"
    chmod 755 "$install_path/bin/urnet-tools" || { pr_err "Failed to install urnet-tools"; exit 1; }

    echo "$version_to_install" > "$install_path/.version"
    echo "$release_date" > "$install_path/.date"

    if [ "$has_systemd" -eq 1 ]; then
        install_systemd_units
    fi

    if [ "$no_modify_bashrc" -eq 0 ]; then
	if awk '/^[[:space:]]*# == urnetwork-provider start[[:space:]]*$/ { code=1; } END { exit code; }' "$HOME/.bashrc"; then
	    pr_info "Adding '%s' to ~/.bashrc" "$install_path/bin"
            cat >> "$HOME/.bashrc" <<EOF

# == urnetwork-provider start
export URNETWORK_PROVIDER_INSTALL="$install_path"
export PATH="\$PATH:\$URNETWORK_PROVIDER_INSTALL/bin"
# == urnetwork-provider end
EOF
	else
	    pr_info "~/.bashrc is up-to-date"
	fi
    fi

	loginctl enable-linger

    case "$operation" in
        install)
            pr_info "Installation complete"
            printf "\n"
            printf "Reload shell:          \e[1msource ~/.bashrc\e[0m           # or restart your terminal\n"
            printf "First run:             \e[1murnetwork auth\e[0m             # auth code can be found at <https://ur.io>\n"
            printf "Start:                 \e[1murnetwork provide\e[0m          # in foreground\n"

            if [ "$has_systemd" -eq 1 ]; then
                printf "Start service:         \e[1msystemctl --user start urnetwork\e[0m\n"
                printf "Disable service:       \e[1msystemctl --user disable urnetwork\e[0m\n"
                printf "Disable auto-updates:  \e[1msystemctl --user disable urnetwork-update.timer\e[0m\n"
                printf "\n"
                printf "\e[1mRefer to <https://docs.ur.io/provider#linux-and-macos> for more detailed instructions.\e[0m\n"
            fi
            ;;

        reinstall)
            pr_info "Reinstallation successful"
            ;;

        update)
            pr_info "Updated successfully"
            ;;
    esac
}

do_uninstall ()
{
    no_modify_bashrc=0

    while [ $# -gt 0 ]; do
        case "$1" in
            -B|--no_modify_bashrc)
                no_modify_bashrc=1
                shift
                ;;

            -*)
                pr_err "Invalid option '%s'" "$1"
                exit 1
                ;;

            *)
                pr_err "Invalid argument '%s'" "$1"
                exit 1
                ;;
        esac
    done

    if [ ! -d "$install_path" ]; then
        pr_err "Directory '%s' could not be found, are you sure you have URnetwork installed?" "$install_path"
        exit 1
    fi

    pr_info "Removing: %s" "$install_path"
    
    if ! rm -r "$install_path"; then
        pr_err "Failed to completely remove '%s'" "$install_path"
        exit 1
    fi

    pr_info "Removing: %s" "$HOME/.urnetwork"
    rm -rf "$HOME/.urnetwork"

    if [ "$has_systemd" -eq 1 ]; then
        pr_info "Removing systemd unit files"
        systemctl --user disable --now urnetwork.service
        systemctl --user disable --now urnetwork-update.timer
        rm -f "$HOME/.config/systemd/user/urnetwork.service"
        rm -f "$HOME/.config/systemd/user/urnetwork-update.service"
        rm -f "$HOME/.config/systemd/user/urnetwork-update.timer"
    fi

    if [ "$no_modify_bashrc" -eq 0 ]; then
        if command -v awk > /dev/null; then
            pr_info "Removing PATH exports from ~/.bashrc"
            cp "$HOME/.bashrc" "$HOME/.bashrc.backup.old"
            awk '/# == urnetwork-provider start/ { pr=1 } pr == 0 { print } /# == urnetwork-provider end/ { pr=0 }' "$HOME/.bashrc" > "$HOME/.bashrc.new"
            mv "$HOME/.bashrc.new" "$HOME/.bashrc"
        else
            pr_err "warning: awk not found, cannot update ~/.bashrc"
            pr_err "Please manually remove PATH exports from your ~/.bashrc"
        fi
    fi

    pr_info "Uninstallation successful"
}

change_auto_update_prefs ()
{
    mode=""
    interval="daily"

    while [ $# -gt 0 ]; do
        case "$1" in
            --interval)
                if [ -z "$2" ]; then
                    opt_requires_arg "$1"
                    exit 1
                fi

                if [ "$2" != "daily" ] && [ "$2" != "weekly" ] && [ "$2" != "monthly" ]; then
                    pr_err "Invalid update interval '%s': Must be one of these: daily, weekly, monthly" "$1"
                    exit 1
                fi
                
                interval="$2"
                shift 2
                ;;

            -*)
                pr_err "Invalid option '%s'" "$1"
                exit 1
                ;;

            *)
                if [ -n "$mode" ]; then
                    pr_err "Unexpected argument '%s'" "$1"
                    exit 1
                fi

                if [ "$1" != "on" ] && [ "$1" != "off" ]; then
                    pr_err "Invalid argument '%s': Must be either 'on' or 'off'" "$1"
                    exit 1
                fi

                mode="$1"
                shift
                ;;
        esac
    done

    if [ "$has_systemd" -eq 0 ]; then
        pr_err "This system doesn't seem to have systemd"
        exit 1
    fi

    state="$(systemctl --user is-enabled urnetwork-update.timer)"

    if [ -z "$mode" ]; then
        pr_info "Auto update state: $state"
        exit 0
    fi

    case "$mode" in
        on)
            pr_info "Updating systemd unit files"

            if ! sed -e "s/daily/$interval/g; s/weekly/$interval/g; s/monthly/$interval/g" -i "$HOME/.config/systemd/user/urnetwork-update.timer"; then
                pr_err "Failed to update auto update interval: sed substitution failed"
                exit 1
            fi

            pr_info "Executing \`systemctl --user daemon-reload'"

            if ! systemctl --user daemon-reload; then
                pr_err "Failed to turn on auto updates: systemctl daemon reload failed"
                exit 1
            fi

            pr_info "Executing \`systemctl --user enable --now urnetwork-update.timer'"

            if ! systemctl --user enable --now urnetwork-update.timer; then
                pr_err "Failed to turn on auto updates: systemctl command failed"
                exit 1
            fi
            ;;

        off)
            pr_info "Executing \`systemctl --user disable --now urnetwork-update.timer'"

            if ! systemctl --user disable --now urnetwork-update.timer; then
                pr_err "Failed to turn off auto updates: systemctl command failed"
                exit 1
            fi
            ;;
    esac
}

toggle_auto_start ()
{
	if test -z "$1"; then
		pr_err "Must provide an argument: Either 'on' or 'off'"
		exit 1
	fi

	if test "$1" != on && test "$1" != off; then
		pr_err "Invalid value: %s, must be either on or off" "$1"
		exit 1
	fi

	if test "$1" = on; then
		if systemctl --user is-enabled --quiet urnetwork.service; then
			pr_info "urnetwork.service is already enabled on login"
			exit 0
	    else
			pr_info "Enabling urnetwork.service (on login)"
			systemctl --user enable urnetwork.service
	    fi
	else
		if ! systemctl --user is-enabled --quiet urnetwork.service; then
			pr_info "urnetwork.service is already disabled"
			exit 0
	    else
			pr_info "Disabling urnetwork.service"
			systemctl --user disable urnetwork.service
	    fi
	fi
}

do_start ()
{
    if ! systemctl --user is-active --quiet urnetwork.service; then
		pr_info "Starting urnetwork.service"
		systemctl --user start urnetwork.service || { pr_err "Failed to start urnetwork.service"; exit 1; }
    else
		pr_info "Service urnetwork.service is already active"
		exit 1
    fi
}

do_stop ()
{
    if systemctl --user is-active --quiet urnetwork.service; then
		pr_info "Stopping urnetwork.service"
		systemctl --user stop urnetwork.service || { pr_err "Failed to stop urnetwork.service"; exit 1; }
    else
		pr_info "Service urnetwork.service is not active"
		exit 1
    fi
}

show_status ()
{
	systemctl --user status urnetwork.service
}

case "$operation" in
    install|update|reinstall)
        do_install "$@"
        exit 0
        ;;

    uninstall)
        do_uninstall "$@"
        exit 0
        ;;

    auto-update)
        change_auto_update_prefs "$@"
        exit 0
        ;;

    auto-start)
		toggle_auto_start "$@"
		exit 0
		;;

    start)
		do_start
		exit 0
		;;

    stop)
		do_stop
		exit 0
		;;

	status)
		show_status
		exit 0
		;;
    
    *)
        pr_err "Invalid operation '%s'" "$operation"
        exit 1
        ;;
esac
