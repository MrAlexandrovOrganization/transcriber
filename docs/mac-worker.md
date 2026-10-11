# Native Mac transcription worker

This opt-in installation creates two **user LaunchAgents**: an SSH tunnel to
`server` and one native Python worker. It runs while the user is logged in and
the Mac is awake. It does not change SSH configuration, sleep or power settings.
After sleep, SSH reconnects and queued work resumes; shutdown or logout stops
execution and the coordinator reclaims expired task leases.

The worker uses the project's locked `.venv` dependencies and cached Whisper
`large` (currently the `large-v3` alias), CPU `int8`, **two inference threads and
one sequential task at a time**. Model construction completes before any claim.
`WHISPER_LOCAL_FILES_ONLY=1` prohibits automatic model downloads. This uses the
M3 CPU; Apple GPU acceleration needs a separate MLX implementation.

Both agents use `ProcessType=Background`, `Nice=10`, low priority IO,
`RunAtLoad`, `KeepAlive` and `ThrottleInterval=30`. Background scheduling reduces
priority; thread limits are not a hard CPU or RAM quota. A large model still
requires several GB of RAM. Normal startup/reconnect does not wake the machine.

## Prepare and review

Use a stable checkout path. Install project dependencies using the repository's
normal dependency workflow. The installer checks `.venv`, imports, token and a
complete cached model by loading it locally; it never claims work during checks.

Place the matching coordinator token outside the repository at
`~/.config/transcriber/worker.token`, owned by the login user, mode `0600`.
Use a protected transfer from the coordinator; never paste the value into a
command, terminal output or source file. Protect the containing directory with
mode `0700`. The installer does not generate, replace or print the token.

From the `backends/transcriber` checkout:

```sh
.venv/bin/python scripts/mac_worker.py check
.venv/bin/python scripts/mac_worker.py install
```

**Install does not start the agents.** It writes:

- `~/Library/LaunchAgents/net.mralexandrov.transcriber.tunnel.plist`
- `~/Library/LaunchAgents/net.mralexandrov.transcriber.worker.plist`

An identical installation is reused. Different files, symlinks and files owned
by another user are refused; inspect and archive an old installation explicitly
before replacing it. Files in `~/Library/LaunchAgents` can also load at the next
login, so install only once the intended coordinator/token are ready.

## Activate and stop

Once the coordinator's authenticated worker API is listening on
`server:127.0.0.1:50055`, activate explicitly:

```sh
.venv/bin/python scripts/mac_worker.py start
.venv/bin/python scripts/mac_worker.py status
.venv/bin/python scripts/mac_worker.py stop
```

The SSH agent runs `ssh -N -T server` with local-only forwarding
`127.0.0.1:50055 -> server:127.0.0.1:50055`, `BatchMode`, `ExitOnForwardFailure`,
a 10-second connection timeout and keepalives every 15 seconds with three missed
responses allowed. A working noninteractive `ssh server` login and trusted host
key must already exist. No API port is published on the Mac's external addresses.
The worker polls idle queues every 60 seconds; transient coordinator failures use
bounded backoff. Tunnel and worker may start in either order.

Stop unregisters the agents from the current login session. The plists remain,
so they may load again at the next login; archive/remove these two reviewed files
after stopping if a persistent uninstall is needed. KeepAlive can reclaim a
worker if it crashes. `start` reuses already loaded labels and does not duplicate
processes. When changing a loaded agent configuration, stop it before installing
the reviewed replacement.

Setup failures identify the failed step and agent. A rejected launchctl operation
also reports its exit code and bounded, sanitized stderr; it never prints the job
environment. Some sandboxed terminals can read launchd state but cannot bootstrap
an agent. In that case repeat the reviewed `start` from an authorized local terminal;
do not run it as root or change the installation to a system LaunchDaemon.

## Logs and limits

`scripts/run_logged.py` captures child stdout/stderr in
`~/Library/Logs/transcriber/{worker,tunnel}.{stdout,stderr}.log`. Each stream rotates
at 5 MiB with three backups, approximately **80 MiB total** across both agents.
Reads are bounded to 64 KiB per log record. Files are created under umask `077`;
no token or transcript is logged by the worker. Unexpected wrapper startup errors
can be diagnosed from the agent's exit status with `launchctl print`.

The supervisor forwards termination to the child process and forces exit
after 20 seconds; the worker normally gets 10 seconds to finish shutdown. Lost
leases prevent stale results from overwriting newer work. Large model loading,
initial decoding and performance on real long recordings should be observed
before treating this CPU worker as a throughput guarantee.

See Apple's [launchd manual](https://github.com/apple-oss-distributions/launchd/blob/main/man/launchd.plist.5)
for user agent scheduling and lifecycle settings.
