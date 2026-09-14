#!/bin/sh

me="$0"
SCRIPT_VERSION="1.0.0"

if [ "$me" = "sh" ] || [ "$me" = "bash" ] || [ "$me" = "zsh" ]; then
    me="urnetwork-uninstaller"
fi

show_help() {
    echo "Usage: "
    echo "  $me [options]"
    echo ""
    echo "Uninstalls Urnetwork for the current user."
    echo ""
    echo "Options:"
    echo "  -h, --help              Show this help and exit"
    echo "  -v, --version           Show the version of this script"
    echo "  -d, --dest=[PATH]       Installation destination"
    echo "  -B, --no-modify-bashrc  Do not modify ~/.bashrc"
    echo ""
    echo "Refer to the online documentation for more help."
}

show_version() {
    echo "Urnetwork uninstaller $SCRIPT_VERSION"
}

require_arg() {
    if [ -z "$2" ]; then
	echo "$me: Option '$1' requires an argument" >&1
	exit 1
    fi
}

install_path="$HOME/.local/share/urnetwork-provider"
no_modify_bashrc=0

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

	-d|--dest)
	    require_arg "$1" "$2"
	    install_path="$2"
	    shift 2
	    ;;

	-B|--no-modify-bashrc)
	    no_modify_bashrc=1
	    shift
	    ;;
	
	--)
	    shift
	    break
	    ;;
	
	-*)
	    echo "$me: Invalid option '$1'" >&1
	    exit 1
	    ;;
	
	*)
	    break
	    ;;
    esac
done

if [ $# -gt 0 ]; then
    echo "$me: Invalid argument '$1'" >&1
    exit 1
fi

if [ ! -d "$install_path" ]; then
    echo "$me: Path '$install_path' does not exist - are you sure you have had Urnetwork installed?" >&2
    exit 1
fi

echo "$me: Removing '$install_path'"
rm -r "$install_path"

echo "$me: Removing '~/.urnetwork'"
rm -rf "$HOME/.urnetwork"

if command -v systemctl > /dev/null; then
    service_path="$HOME/.config/systemd/user/urnetwork.service"

    if [ -f "$service_path" ]; then
	echo "$me: Removing systemd service - urnetwork.service"
	systemctl --user disable --now urnetwork.service
	rm -f "$service_path"
    fi
fi

if [ "$no_modify_bashrc" -eq 0 ]; then
    if ! command -v awk > /dev/null; then
	echo "$me: warning: awk not found, cannot modify ~/.bashrc" >&2
    else
	echo "$me: Removing PATH exports from ~/.bashrc"
	cp "$HOME/.bashrc" "$HOME/.bashrc.backup.old"
	awk '/# == urnetwork-provider start/ { pr=1 } pr == 0 { print } /# == urnetwork-provider end/ { pr=0 }' "$HOME/.bashrc" > "$HOME/.bashrc.new"
	mv "$HOME/.bashrc.new" "$HOME/.bashrc"
    fi
fi

echo "$me: Uninstallation successful"
