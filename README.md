# CLF3

Wabbajack modlist installer for Linux. Installs Wabbajack modlists without needing Windows.

## Status

**Early development** - Works with Tuxborn (Skyrim SE). Other modlists untested.

## Requirements

- Rust 1.70+
- Nexus Mods API key (get from https://www.nexusmods.com/users/myaccount?tab=api) at the very bottom.

## Build

```bash
git clone https://github.com/SulfurNitride/CLF3.git
cd CLF3
cargo build --release
```

Binary will be at `target/release/clf3`

## Usage

```bash
clf3 install Tuxborn.wabbajack \
    --output ~/Games/Tuxborn \
    --downloads ~/Games/Tuxborn/downloads \
    --game ~/.steam/steam/steamapps/common/Skyrim\ Special\ Edition \
    --nexus-key "YOUR_API_KEY"
```

## Commands

- `clf3 install` - Install a modlist
- `clf3 info` - Show modlist info
- `clf3 list-bsa` - List BSA contents
- `clf3 extract-bsa` - Extract from BSA

## License

MIT
