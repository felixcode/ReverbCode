# Closed candidate isolated boot preparation

`test/maintenance/candidate-boot-volume.py` is a separate test-only one-shot
wrapper. It does not add an operation to the existing typed executor, publish a
database or retire its source volume. It reuses unchanged Docker control,
private-volume helper/container cleanup and isolation templates from news source
`f8f04c8a36394b75dc4243e360c08f7ffa6e6522`, file `tools/ao_copy_volume.py`, SHA256
`21884ae9d92572fba8220f53d0fa4558a2671bce27abcb2a39fd081d8b80c88e`.
It does **not** repurpose that executor's working-file-only `source_state` API.

The private request binds a completed, unpublished ROWID-rebuild receipt (or the
second receipt of a complete cleanup/rebuild chain), its private-file SHA,
immutable image ID, exact built test/daemon binaries and their SHA values, source
revision and bounded settings. Candidate path is fixed to
`/snapshot/<ao-copy-run>/candidate.sqlite3`. All running **and stopped** container
references must be absent; local-volume driver/options, run label, nonce,
private file UID/GID/mode/nlink/stat, no sidecars and closed SHA must match.
The source is mounted readonly and independently rechecked after the test.

The target is another exclusively new private Docker volume. Its root must be
empty and become 0700/uid1000. A gated uid1000 container gets only source RO,
target RW and two fixed tool RO mounts, network none, readonly root, private PID
namespace, dropped capabilities, no-new-privileges, private HOME/tmp, 1 CPU and
at most 2 GiB. No production path, host socket, provider executable, credential
or caller-supplied environment/command is mounted. After mount/reference checks,
one fixed exec writes only its new-volume approval file. The test creates a new
subdirectory and `ao.db` with O_EXCL/0600, streams the copy/hash, verifies expected
stat/SHA and a different inode, fsyncs file/directory, opens the real Store, then
uses the same daemon health/ready/own-PID and graceful-SIGTERM helper as the
accepted fixture smoke. It reads only bounded scalar metadata before startup.

The private boot **can write**: startup migration/reconciliation/janitors may
change session/turn states or other data in that disposable copy. A real candidate
with sessions is never called an empty database. The original queued input ID,
same-client-ID idempotency and changed-payload CAS contract are checked through
the application Store in synthetic nonempty fixtures; a successful private boot
does not prove every input's later runtime outcome or native/provider continuity.
Source SHA plus a complete byte copy establishes its initial identity, not an
unchanged private copy after startup. Real native replay remains U for each adapter.

The wrapper removes only its labelled helper/test containers. Its own target
volume remains for owner inspection, including after failure; source is never
placed in a retirement list. Owner cleanup later must verify exact target
run/nonce and zero references. Existing ACP 65536/128 MiB and request 5m limits
are unchanged. This package is not a production maintenance writer or scheduler.

## Future invocation (not executed against an actual candidate)

After root receives the closed volume and accepted typed result, seal the two
reviewed binaries and helper under the private `bender/files` tool directory.
Do not bind tools from another RC run's `runs/.../out`: that produces conflicting
resource ownership evidence. Fill a new 0600 request with the actual closed
receipt path/SHA, image/tool/source hashes and budgets; retain that exact request
and its SHA. In an admitted Jimmy RC job (1 CPU/2 GiB, project budget 8/20):

```sh
python3 test/maintenance/candidate-boot-volume.py \
  --helper /home/coland/bender/files/<approved-helper.py> \
  --request /home/coland/bender/files/<bound-private-boot-request.json> \
  --request-sha256 <exact-request-sha256> --apply
```

Without `--apply`, only the bound request/receipt/tool identities are checked.
No actual historical candidate exists at preparation time; real volume identity,
copy, boot, health, native compatibility and elapsed time remain U until that
separate run. A historical candidate cannot be used for production cutover.

For a future fresh stopped source, fixed `as_of` plus 47h retention does not by
itself establish a 48h limit hours later: offline work, transfer and startup must
all be included. Use the full rehearsal measurements to choose sufficient margin
through existing configurable policy, or a bounded final expiry validation;
do not enlarge the user's two-day limit. Today's historical run has no current
48h proof. Normal online 47h policy, loops and settings are unchanged here.
