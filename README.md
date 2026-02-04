# CLF3

**DISCLAIMER** 

CLF3 is a tool in early stages. Do note a few things, this does not use the original tools for creating BSA files, and transforming DDS files. As a result, the modlist install will not be identical to Wabbajack or Jackify. Its primary focus is on speed, as a result some things are bound to fail. Skyrim is the only game testing and I plan on testing other games in the future and adding support. On top of this, do not report and issue to Wabbajack, or to the modlist authors. Instead, please open an issue inside the CLF3 Support thread inside [NaK Discord](https://discord.gg/9JWQzSeUWt). Plans are too eventually add Windows support and OpenMW support with Linux. The GUI is a work in progress so please do expect bugs. 

You can grab the latest version at the [release page](https://github.com/SulfurNitride/CLF3/releases). 

If you want to support the things I put out, I do have a [Ko-Fi](https://ko-fi.com/sulfurnitride) I will never charge money for any of my content.

---------------------------------------------------

Modlist installer for Linux. Installs Wabbajack modlists and Nexus Collections without needing Windows.

Named after Chlorine Trifluoride - burns through modlists like ClF3 burns through concrete.

## Status

**Early development** - Testing welcome!

Games/Modlist Tested: BG3EE, Tuxborn, LoreRim, Heartland Redux, Fallout Anomaly, Viva New Vegas, Outlander.

## Build

```bash
git clone https://github.com/SulfurNitride/CLF3.git
cd CLF3
cargo build --release
```

Binary will be at `target/release/clf3`

## Wabbajack Modlist Installation

Run the GUI and use the Browse Modlist Gallery button. Fill in the rest of the paths, and pick a proton. Proton 10+ is required, you can obtain GE or Cachy from ProtonPlus/ProtonUp. Set the API Key in the settings, https://www.nexusmods.com/settings/api-keys at the very bottom for personal API key.


## License

MIT
