# Demo Video

This directory contains the VHS tape script for generating the demo video.

## Prerequisites

- [VHS](https://github.com/charmbracelet/vhs) installed (`brew install vhs`)
- DuckDB installed and in PATH
- The h5ad file at `/Users/rj/personal/GenePT-tools/data/5a6c74b9-da1c-4cef-8fdc-07c7a90089d7.h5ad`

## Generate the Video

```bash
cd demo
vhs demo.tape
```

This will create `demo.mp4` in the current directory.

## Customization

Edit `demo.tape` to adjust:
- `Set FontSize` - text size
- `Set Width/Height` - video dimensions
- `Sleep` durations - pause between commands
- `Type@<speed>` - typing speed for specific commands
