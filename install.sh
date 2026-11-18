#!/bin/sh
# Installs flowgen from its GitHub release:
#
#   curl -fsSL https://connve.com/flowgen/install.sh | sh
#
# FLOWGEN_VERSION picks a release (default: the latest).
# FLOWGEN_INSTALL_DIR picks the folder (default: ~/.local/bin).
set -eu

REPOSITORY="connve/flowgen"

fail() {
    printf 'flowgen install: %s\n' "$1" >&2
    exit 1
}

command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v tar >/dev/null 2>&1 || fail "tar is required"

case "$(uname -s)" in
    Linux) os=linux ;;
    Darwin) os=darwin ;;
    *) fail "unsupported system $(uname -s)" ;;
esac

case "$(uname -m)" in
    x86_64 | amd64) arch=amd64 ;;
    arm64 | aarch64) arch=arm64 ;;
    *) fail "unsupported architecture $(uname -m)" ;;
esac

# A shell under Rosetta reports x86_64 on Apple Silicon.
if [ "$os" = darwin ] && [ "$arch" = amd64 ] && [ "$(sysctl -n sysctl.proc_translated 2>/dev/null || echo 0)" = 1 ]; then
    arch=arm64
fi

version="${FLOWGEN_VERSION:-}"
if [ -z "$version" ]; then
    latest=$(curl -fsSL -o /dev/null -w '%{url_effective}' "https://github.com/$REPOSITORY/releases/latest") ||
        fail "could not find the latest release"
    version="${latest##*/v}"
fi
version="${version#v}"

archive="flowgen-$os-$arch-$version.tar.gz"
url="https://github.com/$REPOSITORY/releases/download/v$version/$archive"
dir="${FLOWGEN_INSTALL_DIR:-$HOME/.local/bin}"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

printf 'Downloading flowgen %s for %s/%s\n' "$version" "$os" "$arch"
curl -fsSL "$url" -o "$tmp/$archive" || fail "could not download $url"
curl -fsSL "$url.sha256" -o "$tmp/$archive.sha256" || fail "could not download the checksum of $archive"

expected=$(tr -d ' \n' <"$tmp/$archive.sha256")
if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "$tmp/$archive" | cut -d ' ' -f 1)
elif command -v shasum >/dev/null 2>&1; then
    actual=$(shasum -a 256 "$tmp/$archive" | cut -d ' ' -f 1)
else
    fail "sha256sum or shasum is required"
fi
[ "$expected" = "$actual" ] || fail "checksum mismatch for $archive"

tar -xzf "$tmp/$archive" -C "$tmp"
mkdir -p "$dir"
mv "$tmp/flowgen" "$dir/flowgen"
chmod +x "$dir/flowgen"

printf 'Installed flowgen %s to %s/flowgen\n' "$version" "$dir"
case ":$PATH:" in
    *":$dir:"*) printf 'Run `flowgen` to start.\n' ;;
    *) printf 'Add %s to your PATH, then run `flowgen`:\n  export PATH="%s:$PATH"\n' "$dir" "$dir" ;;
esac
