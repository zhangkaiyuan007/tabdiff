# Recording the demo GIF

Prerequisites: `tabdiff` on PATH (`cargo install --path .`) and
[vhs](https://github.com/charmbracelet/vhs) (`sudo pacman -S vhs` on Arch).

From the repo root:

```console
$ python3 demo/gen-data.py   # once; writes demo/big_*.csv (~110 MB, gitignored)
$ vhs demo/demo.tape         # renders demo/demo.gif
```

Tweak `demo.tape` (theme, timing, shots) and re-run until it looks right, then
embed `demo/demo.gif` at the top of the main README.
