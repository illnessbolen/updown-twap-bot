# updown-twap-bot

Бот для рынков Polymarket Up/Down (BTC/ETH, 5m и 15m): сравнивает вероятность исхода по
живому 60-секундному Chainlink TWAP с ценой на стакане и входит только при положительном
edge после комиссий. Правила и порядок работы: `CLAUDE.md`. Что известно про API и что не
проверено: `docs/API_NOTES.md`.

**Статус: веха M1** (каркас, комиссии, feed, запись JSONL). Ордера не отправляются: брокера
в коде ещё нет, режим всегда бумажный.

## Установка

```bash
python3.11 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
cp config.example.toml config.toml
```

## Ключи (только из окружения, в файлы не класть)

Нужны CLOB API credentials: PolyBolt требует их даже для чтения цен. Создайте для этого
отдельный пустой кошелёк. Приватный ключ боту не нужен.

```bash
export POLYMARKET_API_KEY=...
export POLYMARKET_API_SECRET=...
export POLYMARKET_API_PASSPHRASE=...
```

## Команды

```bash
python main.py check-config      # конфиг и наличие ключей (значения не печатаются)
python main.py smoke             # 60 с на живом PolyBolt; сохраняет сырые кадры в data/smoke
python main.py record            # запись потока: data/feed-YYYYMMDDTHH.jsonl, закрытые часы в .gz
```

`smoke` нужно запустить один раз на машине, где будет работать запись: PolyBolt в разработке
проверялся только против фейкового сервера по документации. Если он не прошёл, пришлите
`data/smoke/smoke-*.jsonl` и вывод команды.

`record` останавливается по Ctrl+C или SIGTERM. Если feed получает фатальную ошибку
(ключи отклонены, нарушение протокола, отказ в подписке), процесс завершается с кодом 1 и
не уходит в бесконечный повтор. Для автоперезапуска используйте systemd или аналог.

## Тесты

```bash
pytest -q
```

## Структура

| Файл | Назначение |
|---|---|
| `config.py` | загрузка и строгая проверка `config.toml`, ключи из окружения, условия live |
| `fees.py` | комиссия taker, rebate-оценка (формула из документации) |
| `feed.py` | PolyBolt: TWAP и спот, heartbeat, детектор зависания, реконнект |
| `recorder.py` | JSONL по часам UTC, gzip закрытых часов |
| `main.py` | CLI: `check-config`, `smoke`, `record` |
| `twap_signal.py`, `store.py` | сигнал и хранилище (не менялись) |
