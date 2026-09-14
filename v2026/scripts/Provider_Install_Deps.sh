#!/bin/sh

echo "Detecting package manager..."

if command -v apt-get >/dev/null 2>&1; then
    PM="apt-get"
    UPDATE="sudo apt-get update"
    INSTALL="sudo apt-get install -y"
elif command -v dnf >/dev/null 2>&1; then
    PM="dnf"
    UPDATE="sudo dnf makecache"
    INSTALL="sudo dnf install -y"
elif command -v yum >/dev/null 2>&1; then
    PM="yum"
    UPDATE="sudo yum makecache"
    INSTALL="sudo yum install -y"
elif command -v zypper >/dev/null 2>&1; then
    PM="zypper"
    UPDATE="sudo zypper refresh"
    INSTALL="sudo zypper install -y"
elif command -v pacman >/dev/null 2>&1; then
    PM="pacman"
    UPDATE="sudo pacman -Sy"
    INSTALL="sudo pacman -S --noconfirm"
else
    echo "No supported package manager found."
    echo "Please install curl, jq, and Python 3.8+ manually."
    exit 1
fi

echo "Using $PM package manager."
echo "Updating package repositories..."
eval "$UPDATE"

echo "Installing curl..."
eval "$INSTALL curl"

echo "Installing jq..."
eval "$INSTALL jq"

PYINSTALLED=0

for py_pkg in python3 python38 python3.8; do
    if eval "$INSTALL $py_pkg"; then
        PYINSTALLED=1
        break
    fi
done

if [ "$PYINSTALLED" -eq 0 ]; then
    echo "Could not install Python 3.8+ automatically. Please install manually if needed (https://python.org)."
fi

PYTHON_VERSION=`python3 -c "import sys; print('.'.join(map(str, sys.version_info[:2])))" 2>/dev/null || echo "not found"`
REQUIRED_VERSION="3.8"
COMPARE_VERSION=`echo "$PYTHON_VERSION" | awk -F. '{print ($1 * 1000) + $2}'`
REQUIRED_COMPARE=`echo "$REQUIRED_VERSION" | awk -F. '{print ($1 * 1000) + $2}'`

if [ "$PYTHON_VERSION" = "not found" ]; then
    echo "Python3 was not found. Please ensure Python 3.8+ is installed and available as python3."
elif [ "$COMPARE_VERSION" -ge "$REQUIRED_COMPARE" ]; then
    echo "Python $PYTHON_VERSION detected (>= 3.8)."
else
    echo "Python $PYTHON_VERSION detected (< 3.8). Consider upgrading."
fi

echo "Installation complete."
