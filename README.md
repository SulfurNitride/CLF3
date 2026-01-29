# CLF3

Modlist installer for Linux. Installs Wabbajack modlists and Nexus Collections without needing Windows.

Named after Chlorine Trifluoride - burns through modlists like ClF3 burns through concrete.

## Status

**Early development** - Testing welcome!

- Wabbajack: Works with Tuxborn (Skyrim SE). Other modlists untested.
- Collections: New feature - needs testing with various collections.

## Requirements

- Rust 1.70+
- Nexus Mods API key (get from https://www.nexusmods.com/users/myaccount?tab=api - scroll to bottom)

## Build

```bash
git clone https://github.com/SulfurNitride/CLF3.git
cd CLF3
cargo build --release
```

Binary will be at `target/release/clf3`

## Nexus Collection Installation

Install a Nexus Collection directly from URL or local JSON file:

```bash
# From Nexus URL
./clf3 collection https://www.nexusmods.com/skyrimspecialedition/mods/collections/COLLECTION_SLUG \
    --output ~/Games/MyCollection \
    --game ~/.steam/steam/steamapps/common/Skyrim\ Special\ Edition \
    --nexus-key "YOUR_API_KEY"
```

This will:
1. Download and set up MO2 portable instance
2. Create Stock Game folder (copy of game files for root mods)
3. Download all mods from Nexus
4. Extract and process mods (including FOMOD installers)
5. Generate modlist.txt and plugins.txt

## Wabbajack Modlist Installation

```bash
./clf3 install Tuxborn.wabbajack \
    --output ~/Games/Tuxborn \
    --downloads ~/Games/Tuxborn/downloads \
    --game ~/.steam/steam/steamapps/common/Skyrim\ Special\ Edition \
    --nexus-key "YOUR_API_KEY"
```

## All Commands

| Command | Description |
|---------|-------------|
| `clf3 collection` | Install a Nexus Collection |
| `clf3 install` | Install a Wabbajack modlist |
| `clf3 info` | Show Wabbajack modlist info |
| `clf3 list-bsa` | List BSA archive contents |
| `clf3 extract-bsa` | Extract file from BSA |

## Environment Variables

- `NEXUS_API_KEY` - Nexus API key (alternative to --nexus-key)
- `RUST_LOG=clf3=debug` - Enable debug logging

## License

MIT
