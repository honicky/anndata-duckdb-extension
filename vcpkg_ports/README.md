# vcpkg overlay ports

Ports in this directory override whatever the vcpkg baseline would otherwise
resolve. They are wired in through `vcpkg-configuration.overlay-ports` in the
repo-root `vcpkg.json`; `duckdb/scripts/merge_vcpkg_deps.py` rewrites that
relative path into an absolute one when it generates the manifest CI builds
from. See the "vcpkg: what our `vcpkg.json` actually controls" section of
`CLAUDE.md` for why this is the only lever we have — `builtin-baseline` and
`overrides` set here are discarded by that script.

Keep this directory as small as possible. Every port here is a piece of
upstream we have taken ownership of and now have to remember to refresh.

## `libaec`

**Why:** the `libaec` port at DuckDB's pinned baseline (`84bab45d`, Dec 2025)
downloads its source tarball from `gitlab.dkrz.de`, which returns HTTP 429 to
GitHub Actions runner IP ranges. That broke `linux_amd64_musl` and
`linux_arm64` every day from ~2026-07-29 — the two arches that get no hits
from DuckDB's read-only vcpkg binary cache and therefore build `libaec` from
source. We do not depend on `libaec` directly; it arrives via the `szip`
feature that vcpkg's `hdf5` port enables by default.

**What this is:** a verbatim copy of `ports/libaec` from microsoft/vcpkg at
`9e593bb18ea69cc5095e012465dcd675a822ed0d` (tag `2026.07.29`), the first
release tag where upstream had moved the source to the
`Deutsches-Klimarechenzentrum/libaec` GitHub mirror. No local modifications.

**Removing it:** once DuckDB's pinned baseline advances past that commit, this
overlay is redundant and should be deleted rather than carried forward. Check
with:

```bash
grep builtin-baseline duckdb/scripts/merge_vcpkg_deps.py
```

**Refreshing it:** re-copy from upstream rather than hand-editing:

```bash
for f in portfile.cmake vcpkg.json usage; do
  curl -sfL "https://raw.githubusercontent.com/microsoft/vcpkg/<sha>/ports/libaec/$f" \
    -o "vcpkg_ports/libaec/$f"
done
```
