# Native WSL GPU/CPU workers

Два независимых процесса используют общую durable очередь через WorkerService:

| Worker ID | Модель | Runtime | Потоки CPU | Конкурентность |
| --- | --- | --- | --- | --- |
| `wsl-gpu` | `small` | CUDA, `int8_float16` | 2 | 1 задача |
| `wsl-cpu` | `large` (`large-v3`) | CPU, `int8` | 4 | 1 задача |

Модели различаются по качеству и скорости; GPU не является ускоренным исполнением
той же `large`. MX550 имеет только 2 GiB VRAM. Нельзя автоматически подменять
`small` другой моделью после OOM: сначала проверить доступную VRAM и конкурирующие
процессы. Ollama, драйверы, Windows startup и настройки сна установщик не меняет.

Координатор учитывает availability по идентичности процесса (`instance_id`),
включая занятые workers. Primary allowlist содержит `mac-m3-pro`, `wsl-gpu`,
`wsl-cpu`; серверный worker используется как fallback после истечения availability
остальных. Workers посылают availability каждые 20 секунд, TTL — 90 секунд.
Наличие доступного primary исключает немедленное распределение очереди на сервер;
это политика приоритета, а не гарантия времени завершения задачи.

## Подготовка runtime

Используйте стабильный Git checkout, например
`~/projects/backends/transcriber`. Проверяйте Git status перед обновлением.
Получайте утверждённый commit через `git fetch` и fast-forward/отдельный checkout;
не переносите исходники вручную и не обновляйте checkout работающего процесса.

Системные prerequisites: WSL2 с systemd, Python версии из `versions.mk`, модуль
`venv`, Git, OpenSSH, CUDA 12/cuBLAS и рабочий `/dev/dxg`. Проверенная машина:
Ubuntu 22.04, i7-12650H, 16 logical CPUs, 15 GiB RAM, MX550 2 GiB, capability 7.5,
драйвер 572.42, CUDA toolkit 12.8. `nvidia-smi` доступен по
`/usr/lib/wsl/lib/nvidia-smi`. Основные Python зависимости фиксирует `poetry.lock`,
Poetry/Buf/Python/cuDNN — `versions.mk`.

Из чистого checkout создайте отдельный tooling venv для Poetry и project `.venv`.
Существующие неизвестные окружения не перезаписывайте:

```sh
python_version=$(sed -n 's/^PYTHON_VERSION := //p' versions.mk)
poetry_version=$(sed -n 's/^POETRY_VERSION := //p' versions.mk)
cudnn_version=$(sed -n 's/^CUDNN_VERSION := //p' versions.mk)
python${python_version} -m venv ~/.local/share/transcriber/poetry
~/.local/share/transcriber/poetry/bin/python -m pip install "poetry==$poetry_version"
python${python_version} -m venv .venv
POETRY_VIRTUALENVS_IN_PROJECT=true ~/.local/share/transcriber/poetry/bin/poetry sync
.venv/bin/python -m pip install --no-deps "nvidia-cudnn-cu12==$cudnn_version"
PATH="$HOME/.local/bin:$HOME/.local/share/transcriber/poetry/bin:$PATH" make proto-gen
```

`make proto-gen` требует Buf версии из `versions.mk`. Устанавливайте бинарник
соответствующей архитектуры из официального release в пользовательский PATH.
cuDNN устанавливается отдельно в project venv, с `--no-deps`, поскольку нужный
cuBLAS уже предоставляет проверенная CUDA установка. Launcher задаёт библиотечные
пути только GPU процессу; системные Python и CUDA не меняются.

Заранее загрузите `Systran/faster-whisper-small` и
`Systran/faster-whisper-large-v3` в стандартный Hugging Face cache. Workers работают
с `HF_HUB_OFFLINE=1` и `WHISPER_LOCAL_FILES_ONLY=1`; моделей они не скачивают.
Для стандартных имён моделей cache должен содержать `refs/main`, а не только
snapshot по SHA. Записывайте использованные SHA при подготовке. В проверке
2026-10-12 использованы `small` revision
`536b0662742c02347bc0e980a01041f333bce120` и `large-v3` revision
`edaa852ec7e145841d8ffdb056a99866b5f0a478`; это свидетельство проверки, а не второй
источник версий зависимостей.

## Доступ и секреты

Создайте отдельный `~/.ssh/id_ed25519_transcriber` с mode `0600`, не заменяя
существующие ключи. На сервере разрешите ему только нужный port forwarding;
запретите shell/PTY/agent/X11/user rc. Forced command `/bin/false` намеренно
запрещает `ssh ... true`; проверяйте именно `ssh -N` tunnel. Используйте отдельный
`~/.ssh/known_hosts_transcriber` с ключом сервера из уже доверенного соединения;
не принимайте неизвестный host key автоматически.

Вне Git создайте `~/.config/transcriber/wsl-ssh.json` (mode `0600`):

```json
{"host":"your-trusted-server","user":"your-login","port":22}
```

Matching bearer token хранится только в
`~/.config/transcriber/worker.token` (владелец — пользователь worker, mode `0600`,
каталог `0700`). Передавайте его защищённо, без вывода в терминал, shell arguments
или Git. Установщик не создаёт, не заменяет и не печатает token.

SSH использует этот key/known_hosts/config, `IdentitiesOnly`, strict host checking,
`BatchMode`, `ExitOnForwardFailure`, keepalives 15s×3 и reconnect через systemd.
Туннель слушает только `127.0.0.1:50055`; внешнего gRPC порта на WSL нет.

## Проверка, установка и запуск

```sh
.venv/bin/python scripts/wsl_worker.py check --audio /absolute/synthetic.wav
.venv/bin/python scripts/wsl_worker.py install
# Только после deploy совместимого coordinator и проверки fallback policy:
.venv/bin/python scripts/wsl_worker.py start
.venv/bin/python scripts/wsl_worker.py status
.venv/bin/python scripts/wsl_worker.py stop
```

`check`/`install` загружают обе cached модели локально и не подключаются к очереди.
`--audio` дополнительно запускает настоящее CPU/CUDA inference синтетического
английского аудио; без него проверяется загрузка модели. Установщик создаёт
`transcriber-wsl.target` и три unit файла (`tunnel`, `gpu`, `cpu`) в
`~/.config/systemd/user`. Install не запускает и не включает новые units.
Start включает target в user manager; stop останавливает и отключает target.
Неизвестные изменённые units, symlinks и drop-ins требуют явного просмотра,
автоматически не заменяются. При обновлении unit сначала остановите проверенную
установку и сохраните прежние файлы вне каталога systemd.

Для работы после закрытия SSH включите `loginctl enable-linger "$USER"` и
проверьте `loginctl show-user "$USER" -p Linger`. Это сохраняет user manager,
пока запущена WSL; Windows boot/autostart или сон ноутбука это не изменяет.
Выключенная WSL не исполняет задачи; coordinator восстанавливает их по lease.

## Ограничения и диагностика

GPU unit: CPUQuota 150%, MemoryHigh 2 GiB, MemoryMax 3 GiB. CPU unit:
CPUQuota 400%, MemoryHigh 4 GiB, MemoryMax 5500 MiB. Обе: Nice 10,
CPUWeight/IOWeight 20, TasksMax 96, без swap, одна задача одновременно.
Лимиты RAM относятся к памяти процесса, **не VRAM**; GPU разделяется с Windows и
другими приложениями. При OOM процесс перезапускается, job восстанавливается по
lease; это требует наблюдения на длинных записях.

Units используют `ProtectSystem=strict`, `ProtectHome=read-only`, `PrivateTmp`,
`NoNewPrivileges`; запись разрешена только в собственный log каталог и временные
файлы. На проверенной WSL sandbox успешно запущен настоящим user systemd
oneshot, а не только проверен синтаксически. Проверяйте повторно после смены WSL/
ядра/systemd. Worker shutdown получает 10 секунд, systemd — 30 секунд на всю группу.

Логи в `~/.local/state/transcriber/{gpu,cpu,tunnel}/` ротируются: 5 MiB × четыре
файла × два потока на процесс, суммарно около 120 MiB. Ошибки supervisor видны в
`journalctl --user -u transcriber-wsl-gpu.service` и аналогичных units; journal
имеет ограничение частоты сообщений. Не публикуйте transcript/credentials из
чужих диагностических команд. Проверяйте service status, restart count, cgroup
limits, GPU VRAM, доступность tunnel и coordinator availability.

В реальном локальном smoke GPU `small/int8_float16` загрузился за 1.36s,
синтетическое аудио обработано за 1.18/0.74/0.74s, RSS около 685 MiB, при начальной
занятости VRAM около 511 MiB. Это проверка runtime на коротком английском аудио;
качество русской речи, длинные записи и поведение при перегрузке требуют отдельной
оценки. Production queue/Telegram во время этого smoke не использовались.

Документация: [faster-whisper](https://github.com/SYSTRAN/faster-whisper),
[WSL systemd](https://learn.microsoft.com/en-us/windows/wsl/systemd),
[systemd resource control](https://www.freedesktop.org/software/systemd/man/latest/systemd.resource-control.html),
[systemd execution sandbox](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html).
