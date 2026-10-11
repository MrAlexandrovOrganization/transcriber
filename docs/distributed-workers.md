# Распределённые вычислительные воркеры

План постепенного подключения Mac и WSL к durable-очереди транскрибации.
Диагностика выполнена 11 октября 2026 года. Числа ниже — снимок текущего
состояния, а не замер пиковой нагрузки или гарантия требуемого размера VPS.

## Что подтверждено

| Узел | Ресурсы и состояние | Практический вывод |
|---|---|---|
| `server` | 58 контейнеров, 8 CPU, 15.6 GiB RAM; в спокойном состоянии контейнеры суммарно около 4.08 GiB и 0.7 CPU; доступно около 9.6 GiB RAM; swap занят на 7.6 GiB; диск занят на 80% | Большинство процессов невелики в момент измерения. Нужны замеры пиков и причины использования swap до выбора VPS |
| `private-test-server` | 3 сетевых контейнера, 2 CPU, 1.9 GiB RAM; свободно около 2.8 GiB диска | Текущего дискового запаса недостаточно для безопасного расширения; узел делит локацию с `server` |
| Mac | Apple M3 Pro, 12 CPU cores, 18 GPU cores, 36 GB RAM; cached Whisper `base` CPU/int8 распознал синтетическую фразу примерно за 0.73 s; отдельный CLI pilot с PostgreSQL/gRPC прошёл за 3.36 s | Проверены engine и полный локальный worker protocol; Metal и связь с production-очередью не проверены |
| `wsl` | Ubuntu 22.04.5, WSL2 x86_64; i7-12650H, 16 логических CPU, около 15 GiB RAM, 14 GiB доступно | Возможны отдельные CPU- и GPU-процессы с лимитами конкурентности |
| GPU в WSL | NVIDIA GeForce MX550, 2 GiB VRAM, compute capability 7.5 по CUDA driver API, драйвер 572.42, CUDA toolkit 12.8 | CUDA driver API успешно создаёт контекст и выделяет/освобождает 1 MiB; полноценная транскрибация ещё требует проверки конкретного runtime и модели |
| ПО в WSL | Ollama 0.32.5 активна, `qwen3.5:9b` занимает 6.6 GB на диске, загруженных моделей при проверке нет; системный Python 3.11.15 без faster-whisper/CTranslate2 | Уже работающая Ollama не доказывает готовность Whisper backend |
| Контейнеры в WSL | Linux Docker CLI отсутствует; Windows Docker CLI есть, Linux Engine недоступен | Для первого пилота подходит изолированное Python-окружение и native worker |

На `server` обнаружены 13 каталогов PostgreSQL суммарно около 3.08 GiB,
заметки около 484 MiB, модели Ollama около 11.89 GiB, данные Whisper около
3.33 GiB, Prometheus около 2.51 GiB. Размеры каталогов не определяют требования
к RAM, IOPS и времени восстановления. Не все 58 контейнеров являются
независимыми приложениями: в число входят базы и инструменты наблюдаемости.

Во время диагностики subscriptions bot был unhealthy, а Prometheus видел
10 из 12 targets как up: catalog и proxy-checker не отвечали. До переноса
следует выяснить причины этих состояний, иначе миграция перенесёт существующие
неисправности. У notes-bot webhook уже активен; переход с polling не является
необходимым первым шагом для этого бота.

Свидетельства: на обоих SSH-узлах проверены `docker ps`, `docker stats
--no-stream`, `free -h`, `df -h /`, имена/mounts контейнеров и размеры каталогов
данных без чтения содержимого. Health Prometheus/Grafana/Loki проверен локальными
запросами на соответствующем сервере; значения environment и секреты в отчёт
не включены. WSL GPU проверен через `nvidia-smi` и CUDA driver API
(`cuInit`, context, allocation/free). Это read-only аудит; бэкапы, восстановление,
пиковая нагрузка и сквозные traces в его покрытие не входили.

Кодовые свидетельства: [durable queue и leases](../whisper/storage.py),
[локальные файловые записи notes-bot](../../../telegram-bots/notes-bot/core/stores.go),
[ожидание и доставка транскрипта](../../../telegram-bots/notes-bot/frontends/telegram/tghandlers/voice.go),
[локальные mounts notes-bot](../../../telegram-bots/notes-bot/docker-compose.yml).
Работоспособность этих компонентов в одной локации не подтверждает готовность
нескольких реплик; следующий шаг — устранить перечисленные зависимости и
проверить восстановление после отказа.

## Схема первого пилота

```text
Клиент → ingress → PostgreSQL: задания, lease, результаты
             ↕
        durable audio storage
             ↕
     отдельный WorkerService
             ↑ исходящее соединение через SSH-туннель
       Mac CPU / WSL CPU / WSL CUDA
```

Ingress быстро принимает аудио и возвращает job ID. Worker сам раз в минуту
запрашивает работу: опрос доступности ноутбука со стороны бота не требуется.
При отсутствии worker задание остаётся в durable-очереди. При выключении
устройства истечение lease позволяет повторить обработку. Каждый claim
получает новый lease token; результат от прежнего владельца должен отклоняться.

Worker получает аудио через endpoint и отправляет текст обратно. Ему не нужны
PostgreSQL credentials, Telegram token или общий файловый volume. Временные
локальные файлы очищаются после задания. Queue, исходное аудио и принятый
результат остаются на постоянном узле.

Optional WorkerService включается отдельно от клиентского TranscriptionService:
`WORKER_GRPC_ADDR` задаёт адрес, `WORKER_TOKEN_FILE` — путь к файлу авторизации.
RPC: `Claim`, `Download`, `Heartbeat`, `Complete`, `Fail`. Это пять RPC;
обновления lease и прогресса совмещены в `Heartbeat`. Публичный gRPC-порт
для пилота не требуется: listener доступен через SSH port forwarding.
Токен передаётся через файл с ограниченными правами; его содержимое не должно
попадать в аргументы команд, Git и журналы.

Начальные настройки native worker:

```bash
REMOTE_WORKER_ENDPOINT=localhost:50055 \
REMOTE_WORKER_TOKEN_FILE=/path/to/worker-token \
WORKER_POLL_INTERVAL_S=60 \
WHISPER_MODEL=base \
WHISPER_DEVICE=cpu \
WHISPER_COMPUTE_TYPE=int8 \
WHISPER_CPU_THREADS=2 \
WHISPER_LOCAL_FILES_ONLY=1 \
make run-remote-worker
```

Endpoint в примере — локальный конец заранее настроенного SSH-туннеля.
Значение token file — пример пути, не секрет. Модель и зависимости необходимо
предварительно подготовить в изолированном окружении; `LOCAL_FILES_ONLY=1`
запрещает неожиданную загрузку модели во время обычного запуска. Остановка
процесса — Ctrl+C; поведение lease и повторной обработки проверяется на
синтетическом аудио вне production-очереди.

На Mac первый backend — faster-whisper/CTranslate2 на CPU. Поддержка ARM64
не означает ускорение Apple GPU. Metal/MLX или whisper.cpp можно добавить позже
за тем же WorkerService, сохранив одинаковые правила lease и завершения.
Локальный engine smoke-test использовал синтетическое аудио из `say`: фраза
«The worker is ready. This is a local transcription test.» распознана как
«The worker is ready, this as a local transcription test.». Это подтверждает
работу CPU inference; качество на русском этой проверкой не оценивалось.
Дополнительный `make pilot` проверил настоящий Submit, PostgreSQL, очередь без
worker и отдельный native процесс с claim/download/heartbeat/complete: `DONE`,
одна попытка, непустой transcript, 3.36 s вместе со стартом временной инфраструктуры.
Это локальный изолированный прогон, без сети `server` и production-задач.
При сне или выключении Mac ожидается истечение
lease, а не сохранение активного вычисления.

Для кода пилота прошли `make check` с отдельной тестовой PostgreSQL
(20 тестов выполнено, один дополнительный real-model test без явного аудио
пропущен), `make build`, проверка импортов и SQL assets в собранном образе,
Compose worker override и Buf breaking against предыдущей ревизии.
Реальная речь отдельно проверена через `make pilot`; исторические успешные
проверки не заменяют новый прогон после следующих изменений. GitHub Actions
добавлен test/build gate перед deploy, но удалённый workflow ещё не запускался.

На WSL сначала запускается такой же CPU worker. Для CUDA worker требуется
отдельная проверка `ctranslate2.get_supported_compute_types("cuda")`, загрузки
модели и реальной транскрибации. После неё можно подобрать
`WHISPER_DEVICE=cuda` и compute type; `int8_float16` — кандидат для проверки,
его поддержку и преимущество для MX550 заранее не предполагаем. Доступ к CUDA
driver API не подтверждает наличие cuDNN, совместимость wheel или достаточную
VRAM. CUDA-библиотеки должны соответствовать закреплённым версиям backend.

Начальная конкурентность — один GPU job и один CPU job. Два процесса могут
использовать разные worker IDs, но каждый ограничивает число своих заданий.
У GPU всего 2 GiB VRAM; несколько GPU-процессов дублируют модель в памяти.
CPU threads увеличиваются после измерения времени обработки, RAM и влияния
на интерактивную работу. Предпочтение GPU перед CPU потребует capability-aware
выдачи задач; одинаковый polling нескольких workers сам по себе такого
приоритета не обеспечивает.

## Ограничение notes-bot перед production

Durable-очередь inference уже сохраняет задания, но frontend notes-bot хранит
доставку в памяти. Пока это не исправлено, worker pilot не является разрешением
отключить постоянный worker и заставить production ждать ноутбук сутками.

В `frontends/telegram/tghandlers/voice.go` notes-bot целевая дата, NL/smart-режим,
статусное сообщение, порядок и backend job ID не восстанавливаются после
рестарта. Ожидание ограничено тремя часами и завершается после серии ошибок
polling. В transcriber завершённые задания удаляются по `JOB_TTL_S`, по умолчанию
через два часа: отсутствие frontend дольше этого срока способно потерять
недоставленный результат.

Следующий законченный этап должен включать:

1. Durable delivery ledger: IDs бота/чата/сообщения, idempotency key, job ID,
   цель, время приёма, режим, transcript и состояние доставки; восстановление
   после рестарта и повторная доставка с backoff.
2. Идемпотентный Submit и append заметки с operation ID. Флаг `delivered` в
   отдельной БД вокруг file append оставляет окно повторной записи при падении.
3. Сохранение порядка по чату в едином dispatcher; отменённая или окончательно
   провалившаяся задача должна освобождать следующие результаты.
4. Retention/acknowledgement, сохраняющие результат до надёжного получения
   frontend; ожидание worker и сроки хранения результата — разные интервалы.
5. Изоляцию от текущего UI-состояния: поздний результат NL/smart не должен
   перезаписывать новый draft. «Завтра» интерпретируется относительно времени
   исходного сообщения, а подтверждение относится к конкретному job.

Redis user sessions с TTL и process-local mutex не заменяют ledger доставки
или блокировки между репликами. Для ledger предпочтителен PostgreSQL;
текущий Redis требует отдельного решения о persistence и атомарности claims.
Ошибка отображения результата в Telegram не должна повторять уже выполненный
append заметки.

## Постоянная инфраструктура и три VPS

Три постоянно доступные VPS в независимых локациях подходят для контрольного
слоя K3s/etcd и небольших приложений. Три voting members выдерживают потерю
одного; потеря двух лишает кворума. `server` и `private-test-server` имеют общий
риск отказа локации. WSL и Mac не получают обязательных quorum-ролей.

Минимум K3s server — 2 CPU и 2 GB RAM на узел, без ресурсов приложений. Для
совмещения control plane, БД, ingress и observability стартовый бюджет от
4 GiB на VPS следует считать гипотезой, которую проверяют нагрузкой. Нужен
запас для переноса работы с одной VPS на две оставшиеся. Полный текущий стек
observability и все модели автоматически в этот бюджет не помещаются.
До аренды сравниваются RTT, потери, SSD/IOPS и исходящий трафик между площадками.

Между VPS нужна стабильная приватная сеть, например статический WireGuard
с конфигурацией вне зависимости от Headscale. Для подключения вычислителей
подходит Headscale, но SSH-туннель позволяет начать раньше. HA subnet routing
Headscale и HA его coordination server — разные возможности. Нельзя считать
несколько реплик coordination server за балансировщиком готовым решением без
подтверждённой поддержки и синхронизации состояния. Один активный экземпляр с
восстановлением на другой VPS — самостоятельная задача с резервированием БД,
ключей и конфигурации.

Классификация stateless требует проверки bind mounts, рабочих каталогов,
локальных файлов и контрактов. У notes-bot core пишет заметки в filesystem;
local Bot API использует общий volume; workspace-сервисы тоже сохраняют файлы.
Небольшой размер данных не делает эти компоненты stateless. Несколько webhook
реплик требуют deduplication updates и блокировок между процессами;
балансировщик этого не обеспечивает.

Live PostgreSQL/SQLite или рабочие volumes не копируются rsync на ноутбуки
как способ репликации. Для баз используются штатные backups/replication и
проверка восстановления. Асинхронные offsite backup-копии на доступное
устройство допустимы, но отключаемый ноутбук не должен быть единственным
хранилищем резервной копии. Репликация не заменяет исторические backups.

## Последовательность внедрения

1. Изолированный ingress/БД/аудио, синтетическая задача и CPU worker на Mac.
   Проверить получение результата, остановку worker, lease expiry, повторный
   claim и отклонение результата от прежнего владельца.
2. Такой же CPU worker на WSL; затем отдельная CUDA-проверка с небольшой
   моделью и измерением VRAM. Ограничить нагрузку и проверить совместную работу.
3. Durable delivery ledger notes-bot, идемпотентные эффекты и recovery-тесты;
   только затем включать длительное ожидание отсутствующих workers.
4. Исправить текущие unhealthy/down состояния, уточнить пиковые ресурсы и
   каталог данных каждого проекта, проверить backups и восстановление.
5. Пилот из трёх VPS: независимая сеть, quorum, резервирование состояния,
   внешний ingress и один маленький сервис; проверить потерю одной площадки.
6. Переносить приложения по одному с documented rollback. Внешний монитор
   проверяет доступность вне кластера; внутренние метрики показывают worker
   availability, возраст очереди, lease/retries, ошибки доставки, ресурсы и
   storage. Недоступность необязательного worker допустима; рост возраста
   критичных задач требует отдельного алерта.

## Источники

- [K3s requirements](https://docs.k3s.io/installation/requirements) и
  [HA embedded etcd](https://docs.k3s.io/datastore/ha-embedded): минимум узлов,
  quorum и ресурсы control plane.
- [etcd tuning](https://etcd.io/docs/v3.6/tuning/): влияние RTT и дисковых задержек.
- [faster-whisper](https://github.com/SYSTRAN/faster-whisper): CPU/CUDA,
  quantization, PyAV и зависимости GPU runtime.
- [CTranslate2 hardware](https://opennmt.net/CTranslate2/hardware_support.html),
  [quantization](https://opennmt.net/CTranslate2/quantization.html) и
  [parallelism](https://opennmt.net/CTranslate2/parallel.html): доступные backend,
  compute types и лимиты параллельности.
- [Telegram Bot API](https://core.telegram.org/bots/api): update IDs, повторные
  и нарушающие порядок webhook updates, срок хранения updates.
- [Headscale routing](https://headscale.net/stable/ref/routes/) и
  [обсуждение coordination HA с ответом владельца](https://github.com/juanfont/headscale/issues/100):
  различие HA routers и нескольких active control servers.
