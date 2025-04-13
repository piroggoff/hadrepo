#!/bin/bash

# Define arch
ARCH=$(uname -m)
case "$ARCH" in
    x86_64|amd64)
        LIST_FILE="wgetlist-x86_64"
        ARCH_NAME="x86_64"
        ;;
    aarch64|armv8*|armv7*)
        LIST_FILE="wgetlist-aarch64"
        ARCH_NAME="ARM"
        ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

# Is file exists
if [ ! -f "$LIST_FILE" ]; then
    echo "File $LIST_FILE for $ARCH_NAME wasn't found!"
    exit 1
fi

echo "Current architecture: $ARCH_NAME"
echo "Using file: $LIST_FILE"


DOWNLOAD_DIR="downloads_$ARCH_NAME"
mkdir -p "$DOWNLOAD_DIR"

SUCCESS=0
FAILED=0

# Read file and download
while IFS= read -r url || [ -n "$url" ]; do
    # Skip empty and comments
    if [[ -z "$url" || "$url" == \#* ]]; then
        continue
    fi
    
    echo "Downloading: $url"
    wget -q --show-progress -P "$DOWNLOAD_DIR" "$url"
    
    if [ $? -eq 0 ]; then
        echo "Successfully downloaded: $(basename "$url")"
        ((SUCCESS++))
    else
        echo "Error occured: $url"
        ((FAILED++))
    fi
done < "$LIST_FILE"

echo "======================================"
echo "Count of packages: $((SUCCESS + FAILED))"
echo "Success: $SUCCESS"
echo "Failed: $FAILED"
echo "Download directory: $DOWNLOAD_DIR"