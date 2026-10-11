# API_NOTES: Polymarket для updown-twap-bot

Проверено **2026-10-11** по https://docs.polymarket.com/llms.txt и страницам из него,
плюс публичные read-only запросы к Gamma и CLOB в тот же день (помечены как «наблюдение»).
Уровни достоверности:

- **[док]** написано в документации (ссылка рядом);
- **[наблюдение]** видно в живых ответах API, но в документации/OpenAPI не описано;
- **[TODO(verify)]** не удалось подтвердить, в коде будет интерфейс + `TODO(verify)`, а не догадка.

---

## 0. Главное, что меняет план

1. **30-секундного TWAP больше нет.** С 2026-08-14 все 5m крипторынки рассчитываются по
   **60-секундному** Chainlink TWAP, как и 15m. В PolyBolt окно только `window_seconds: 60`,
   «других окон нет». [док: [changelog 2026-08-14](https://docs.polymarket.com/changelog/predictions),
   [PolyBolt overview](https://docs.polymarket.com/api-reference/live-data/overview#supported-symbols),
   [миграция](https://docs.polymarket.com/migrate/rtds-to-polybolt#topic-mapping)]
   Живые рынки это подтверждают: `cryptoMarketConfig.twapLookbackSeconds = 60` и для `btc-5m`, и для `btc-15m`. [наблюдение]
   → в `config.example.toml` комментарий «5m рынки (TWAP 30 с)» устарел; `window = 60` для обоих.
2. **TWAP и спот теперь в PolyBolt, и он требует CLOB API-ключи** (`apiKey`, `secret`, `passphrase`)
   даже для чтения цен. RTDS для цен объявлен legacy. [док: [PolyBolt overview](https://docs.polymarket.com/api-reference/live-data/overview)]
3. **Символы:** `btcusd`, `ethusd` (нижний регистр, без `/`, суффикс `usd`). `btc/usd` и `btcusdt` больше не принимаются.
   [док: [миграция](https://docs.polymarket.com/migrate/rtds-to-polybolt)]
4. **Комиссия taker на крипто: `fee = C × 0.07 × p × (1 − p)`**, то есть 3.5% от суммы при p = 0.50
   (а не «около 3%»), 1.75 USD на 100 shares. Maker комиссию не платит; rebate 20% от taker-комиссий
   рынка, делится между мейкерами. [док: [Fees](https://docs.polymarket.com/trading/fees)]
5. **Price to beat в реальном времени из Gamma не получить.** `eventMetadata.priceToBeat` появляется
   только около конца окна. Strike надо брать из TWAP-потока в момент `eventStartTime`. [наблюдение, см. §3.3]

---

## 1. TWAP и спот-цены (PolyBolt)

### 1.1 Подключение [док]

Источники: [PolyBolt overview](https://docs.polymarket.com/api-reference/live-data/overview),
[Live Data Channel (схемы)](https://docs.polymarket.com/api-reference/wss/polybolt),
[Real-Time Data → Reference Prices](https://docs.polymarket.com/market-data/realtime-data#reference-prices),
[миграция RTDS → PolyBolt](https://docs.polymarket.com/migrate/rtds-to-polybolt).

- URL: `wss://ws-live-v2.polymarket.com/ws`
- Сначала аутентификация (до подписки):
  ```json
  {"op":"auth","rid":"a1","auth":{"apiKey":"…","secret":"…","passphrase":"…"}}
  ```
  Ответ: `{"op":"authed","rid":"a1"}`. Ошибки без закрытия сокета: `auth_required`, `auth_invalid`, `auth_unavailable`.
- Подписка (батчем, один кадр считается одним запросом):
  ```json
  {"op":"subscribe","rid":"s1","subscriptions":[
    {"channel":"price.crypto.twap","filter":{"symbol":"btcusd","window_seconds":60}},
    {"channel":"price.crypto.twap","filter":{"symbol":"ethusd","window_seconds":60}},
    {"channel":"price.crypto","filter":{"symbol":"btcusd"}},
    {"channel":"price.crypto","filter":{"symbol":"ethusd"}}
  ]}
  ```
  Подтверждение: `{"op":"subscribed","channel":"…","rid":"s1"}`; в нём указан фактический `provider`.
- Каналы:
  | Канал | Что | Фильтр |
  |---|---|---|
  | `price.crypto.twap` | 60-с Chainlink TWAP, по нему идёт расчёт up/down | `{"symbol":"btcusd","window_seconds":60}` |
  | `price.crypto` | спот; по умолчанию Chainlink, можно `provider: "pyth"` для btc/eth/sol/xrp/doge/bnb | `{"symbol":"btcusd"}` |
- Символы `price.crypto.twap`: `btcusd, ethusd, solusd, xrpusd, dogeusd, hypeusd, bnbusd, zecusd`.

### 1.2 Формат сообщений [док]

Общий конверт: `{"v":1,"channel","seq","ts","snapshot"?,"dropped"?,"payload"}`.

- Live TWAP:
  ```json
  {"v":1,"channel":"price.crypto.twap","seq":3,"ts":1788973002000,
   "payload":{"symbol":"btcusd","value":64120.7,"full_accuracy_value":"64120.70000000",
              "timestamp":1788973002000,"window_seconds":60,"source":"chainlink"}}
  ```
- Снимок после каждой новой подписки: `snapshot: true`, `payload.data[]` за **предыдущие 2 минуты**
  (`timestamp`, `value`, `full_accuracy_value`), может быть пустым.
- Брать `full_accuracy_value` (строка, `Decimal`), а не `value` (float). E18-масштабирование из старого RTDS **не применять**.
- `timestamp` в payload: время события у источника, мс. Свежесть проверять по нему.
  `ts` в конверте: время события у продюсера, мс.
- `seq` плотный в пределах (соединение, канал), сбрасывается при переподключении; снимок тоже занимает `seq`.
  `dropped` > 0 значит, что клиент не успевал и кадры выброшены.
- `source` у `price.crypto` может смениться между соединениями: перечитывать после каждого reconnect.
- В примерах документации точки идут раз в 1 с. Реальная частота не гарантирована документацией. [TODO(verify)]

### 1.3 Heartbeat, лимиты, закрытия [док]

- Сервер шлёт WebSocket-ping каждые 25 с; стандартный клиент отвечает сам; 2 пропущенных pong → закрытие `4002`.
  Можно слать `{"op":"ping"}` → `{"op":"pong"}`.
- Лимиты на соединение: 64 активных подписки, 20 кадров subscribe/unsubscribe в секунду, кадр до 64 KB, 8 auth-кадров.
  Нарушение → ошибка и закрытие `4008`.
- Коды закрытия: `4001` auth (не переподключаться, пока не исправлено), `4002` медленный потребитель/pong
  (экспоненциальный backoff с jitter), `4003` drain (переподключиться через случайные 0–10 с),
  `4008` нарушение политики (это баг), `1006` (backoff с full jitter, от 1 с до 30 с).
  HTTP `429`/`503` при handshake: ждать не меньше `Retry-After`.
- После reconnect: снова `auth`, снова все подписки, инициализация из новых снимков.

**Вывод для `feed.py`.** Ping/pong протокольного уровня не ловит ситуацию «сокет жив, данных нет».
Поэтому нужен собственный `last_data_ts` по каждому (канал, символ) и сторожевой таймер
(CLAUDE.md, правило 5): нет новых точек дольше `stale_after_sec` → переподключение и запрет новых входов.

### 1.4 Ключи для PolyBolt [док]

[Getting started → API → Authentication](https://docs.polymarket.com/getting-started/api#authentication).
L1: кошелёк подписывает EIP-712 `ClobAuth`, затем `POST /auth/api-key` (создать) или `GET /auth/derive-api-key`
(получить существующие) на `https://clob.polymarket.com` → `{apiKey, secret, passphrase}`.
Для PolyBolt нужны только эти три значения; приватный ключ боту на M1–M4 не нужен.

- Можно ли получить ключи для кошелька без средств и без истории на Polymarket? [TODO(verify)]
- Python SDK `AsyncSecureClient.create(private_key=…)` сам выводит ключи, но требует приватный ключ в процессе.
  Можно ли создать его только из L2-ключей, в документации не нашёл. [TODO(verify)]

### 1.5 RTDS (legacy) [док]

`wss://ws-live-data.polymarket.com`: для цен legacy. `crypto_prices_twap_thirty` замены не имеет.
В JS SDK удаление legacy-топиков цен запланировано через месяц после `@polymarket/client 0.11.0`;
Python `polymarket-client 0.11.0` вышел 2026-09-23 ([PyPI](https://pypi.org/project/polymarket-client/)).
На RTDS остаётся только `activity`. Строить на RTDS нельзя.
Страница [Getting started → API](https://docs.polymarket.com/getting-started/api) всё ещё называет RTDS
источником «public reference prices»: она устарела относительно changelog и страницы миграции.

### 1.6 Спот для sigma

- **PolyBolt `price.crypto`** [док]: Chainlink по умолчанию, Pyth по запросу. Тот же сокет и те же ключи.
- Совпадает ли поток `price.crypto` (Chainlink) с тем спотом, из которого Chainlink считает TWAP? [TODO(verify)]
  Проверка на записанных данных: среднее `price.crypto` за [t−60, t] против `price.crypto.twap` в момент t.
- Binance/Coinbase: документацию этих бирж в этой задаче не читал, их формат здесь не описан. [TODO(verify)]
  Из этого контейнера `api.binance.com` отвечает `451` (гео-блок), `api.exchange.coinbase.com` доступен.

---

## 2. Как рассчитываются up/down рынки [док]

[Resolution → Chainlink TWAP–Resolved Up-Down Markets](https://docs.polymarket.com/concepts/resolution),
[changelog 2026-08-07 и 2026-08-14](https://docs.polymarket.com/changelog/predictions).

- У рынка есть пара, время старта и длительность. **TWAP в момент старта = price to beat;
  TWAP в момент конца = final price.** Оба из одного Chainlink TWAP-потока.
- **Up, если final ≥ price to beat**, иначе Down. Без UMA.
  (Совпадает с `fair_prob_up` в `twap_signal.py`: `P(final >= strike)`.)
- Окно усреднения: 60 с и для 5m, и для 15m (с 2026-08-14). Значит, в `effective_var_time(secs_left, window)` `window = 60`.
- Текст правил на живом рынке [наблюдение]: «Up, если TWAP Chainlink … ≥ цены в начале диапазона»,
  источник `https://data.chain.link/streams/btc-usd-twap-60s-streams` (для ETH: `eth-usd-twap-60s-streams`).
- [наблюдение] `finalPrice` рынка 19:15–19:20 UTC (83637.57257415207) совпал с `priceToBeat` следующего
  рынка 19:20–19:25. Конец одного окна = старт следующего.

---

## 3. Поиск активных 5m/15m рынков (Gamma API)

[Discover Markets](https://docs.polymarket.com/market-data/discover-markets),
[Market Details](https://docs.polymarket.com/market-data/market-details),
[List events (keyset)](https://docs.polymarket.com/api-reference/events/list-events-keyset-pagination).
Базовый URL `https://gamma-api.polymarket.com`, публичный, без ключей.

### 3.1 Серии [наблюдение, ID с живых ответов]

| Серия (slug) | id | recurrence | slug события |
|---|---|---|---|
| `btc-up-or-down-5m` | 10684 | `5m` | `btc-updown-5m-<start_unix>` |
| `eth-up-or-down-5m` | 10683 | `5m` | `eth-updown-5m-<start_unix>` |
| `btc-up-or-down-15m` | 10192 | `15m` | `btc-updown-15m-<start_unix>` |
| `eth-up-or-down-15m` | 10191 | `15m` | `eth-updown-15m-<start_unix>` |

`<start_unix>` = начало окна в секундах UTC, кратное 300 или 900. Схема slug в документации не описана.
Это быстрый путь, но основным его делать нельзя.

### 3.2 Запросы

- [док] `GET /events/keyset?series_id=10684&closed=false&start_time_min=<ISO>&limit=…`
  Курсор: `next_cursor` → `after_cursor`. Ответ `{"events":[…],"next_cursor":…}`. Проверено вживую: отдаёт ближайшие будущие окна.
  `offset` запрещён. Старые `GET /events` и `GET /markets` «будут deprecated».
- [док] `GET /events/slug/{slug}`, `GET /markets/slug/{slug}`.
- Лимиты: `/events` 500 req/10 s, общий Gamma 4000 req/10 s. [док: [Rate Limits](https://docs.polymarket.com/api-reference/rate-limits)]

### 3.3 Нужные поля рынка (`event.markets[0]`)

| Поле | Значение | Статус |
|---|---|---|
| `conditionId` | id рынка для CLOB | док |
| `version` | `"v1"` (CTF) или `"v2"` | док |
| `clobTokenIds` | JSON-строка `[up, down]` для `v1`; для `v2` брать `positionIds` | док |
| `outcomes` | JSON-строка `["Up","Down"]`, индекс совпадает с токенами | док + наблюдение |
| `eventStartTime` | начало окна (ISO) = момент strike | в OpenAPI есть, смысл из наблюдения |
| `endDate` | конец окна = время расчёта | док |
| `active`, `closed`, `acceptingOrders` | готовность к торговле | док |
| `orderPriceMinTickSize`, `orderMinSize` | tick и мин. размер ордера (5) | док |
| `feesEnabled`, `feeSchedule{rate,exponent,takerOnly,rebateRate}` | на 5m/15m сейчас `0.07, 1, true, 0.2` | док + наблюдение |
| `cryptoMarketConfig{asset,duration,twapEnabled,twapLookbackSeconds}` | `btc`, `5m`, `true`, `60` | наблюдение, нет в OpenAPI |
| `restricted` | `true`: есть гео-ограничения | док |
| `bestBid`, `bestAsk`, `outcomePrices` | **кэш, отстаёт**; для решений не использовать | наблюдение |

**Price to beat** [наблюдение]: на уровне события `eventMetadata: {"priceToBeat": …, "finalPrice": …}`.
В 19:28 UTC у идущего рынка 19:25–19:30 `eventMetadata` был `null`; у закончившегося 19:20–19:25 уже был
`priceToBeat`, но ещё не `finalPrice`. У полностью закрытого были оба. В OpenAPI Gamma поля нет.
→ Strike для торговли = точка `price.crypto.twap` с `timestamp == eventStartTime` (в мс).
  `eventMetadata.priceToBeat` использовать только для сверки и разметки исходов.
  Точно ли значение TWAP-потока в момент старта совпадает с `priceToBeat` до последнего знака,
  проверим на записи. [TODO(verify)]
  Снимок PolyBolt покрывает только 2 минуты: если бот подключился позже, чем через 2 минуты после старта
  окна, strike неизвестен, и рынок пропускаем.

---

## 4. Стакан CLOB

### 4.1 REST [док]

`https://clob.polymarket.com`, публично.
- `GET /book?token_id=…` → `{market, asset_id, timestamp(ms, строка), hash, bids[{price,size}], asks[…], min_order_size, tick_size, neg_risk, last_trade_price}`.
  [Get order book](https://docs.polymarket.com/api-reference/market-data/get-order-book).
  [наблюдение] `asks` отсортированы по убыванию цены, лучший ask последний. Код не должен на это полагаться: брать min/max.
- `POST /books` (пакетно), `GET /price`, `/midpoint`, `/spread`. Для исполнения в paper брать только уровни стакана, не mid.
- `GET /clob-markets/{condition_id}` → `t` (токены с метками `Up/Down`), `mts` (tick), `mos` (мин. размер),
  `fd{r,e,to}` (комиссия: rate, exponent, taker-only), `itode` (включена задержка taker).
  [Get CLOB market info](https://docs.polymarket.com/api-reference/markets/get-clob-market-info).
  Живой пример: `"fd":{"r":0.07,"e":1,"to":true}`, `"itode":true`.

### 4.2 WebSocket market channel [док]

[Market Channel](https://docs.polymarket.com/api-reference/wss/market), [Real-Time Data → Market Stream](https://docs.polymarket.com/market-data/realtime-data#market-stream).
- `wss://ws-subscriptions-clob.polymarket.com/ws/market`, публичный.
- Подписка: `{"assets_ids":[…],"type":"market","initial_dump":true,"custom_feature_enabled":true}`;
  менять подписки можно без переподключения.
- Heartbeat: **клиент шлёт текст `PING` каждые 10 с**, сервер отвечает `PONG`.
- События: `book` (полный снимок, с `hash`), `price_change` (дельты уровней; size 0 = уровень удалён),
  `last_trade_price`, `tick_size_change`, а при `custom_feature_enabled` ещё `best_bid_ask`, `new_market`, `market_resolved`.
- Детали для `book.py` (M2) разберу на вехе M2.

### 4.3 Ограничения торговли [док]

- **Tick size меняется на ходу** (событие `tick_size_change`). [наблюдение] у одного рынка Gamma показывала 0.01,
  `/clob-markets` 0.001, `/book` то 0.01, то 0.001. Брать tick из последнего `book`/`tick_size_change`.
- Мин. размер ордера: `orderMinSize = 5` (в документации написано «USDC notional»). [TODO(verify)] в shares или в USD.
- **Задержка taker** на крипто up/down: в changelog **150 мс** с 2026-09-04; в описании `itode` и в
  [Order Lifecycle](https://docs.polymarket.com/concepts/order-lifecycle) всё ещё 250 мс. Документация противоречит
  сама себе; для paper брать консервативно 250 мс поверх сетевой задержки.
- Типы ордеров: GTC, GTD (истекает за 1 мин до указанного времени), FOK, FAK; есть **post-only** (для maker-входа).
  [Place Orders](https://docs.polymarket.com/trading/place-orders).
- Перезапуски движка: после рестарта 2 минуты только post-only.
  [Matching Engine Restarts](https://docs.polymarket.com/trading/matching-engine).
- Гео-блок: `GET https://polymarket.com/api/geoblock`. Рынки up/down помечены `restricted: true`.
  Для live (M5) обязательно проверять. [Geographic Restrictions](https://docs.polymarket.com/api-reference/geoblock)

---

## 5. Комиссии и rebate

### 5.1 Taker-комиссия [док]

[Fees](https://docs.polymarket.com/trading/fees), [Market Details → Trading Fees](https://docs.polymarket.com/market-data/market-details#trading-fees),
[changelog 2026-03-31: считать по `feeSchedule`](https://docs.polymarket.com/changelog/predictions).

```
fee = C × feeRate × p × (1 − p)        C = число shares, p = цена share
```
- Crypto: `feeRate = 0.07`, maker fee 0, maker rebate 20%. Берётся при матчинге; в ордере не указывается.
- Комиссия в pUSD, симметрична вокруг 0.50; округление до 5 знаков, минимум 0.00001, меньше → 0.
- Market BUY: сумма указывается до комиссии, комиссия «сверху» (`max_spend` ограничивает итог).
  [Place Orders → Cap Market Buy Spending](https://docs.polymarket.com/trading/place-orders)
- Таблица из документации (100 shares, Crypto), это тест-кейсы для `fees.py`:

  | p | 0.01 | 0.05 | 0.10 | 0.20 | 0.30 | 0.40 | 0.50 | 0.60 | 0.70 | 0.80 | 0.90 | 0.95 | 0.99 |
  |---|---|---|---|---|---|---|---|---|---|---|---|---|---|
  | fee, USD | 0.07 | 0.33 | 0.63 | 1.12 | 1.47 | 1.68 | 1.75 | 1.68 | 1.47 | 1.12 | 0.63 | 0.33 | 0.07 |

  (В таблице округлено до центов; точные значения по формуле: 0.0693, 0.3325, 0.63, …)
- В единицах вероятности на 1 share: `0.07 × p(1−p)` = **0.0175 при p = 0.5**. Как доля суммы: `0.07 × (1−p)`,
  то есть 3.5% при 0.5, 4.9% при 0.3, 2.1% при 0.7. Дешёвые контракты дороже в процентах.
- **`exponent`.** В `feeSchedule` есть `exponent` («степень ценовой компоненты кривой»), сейчас он `1`.
  Формула на странице Fees записана без него. Как именно он входит при `exponent ≠ 1`, документация не говорит.
  [TODO(verify)] Решение: `fees.py` поддерживает только `exponent == 1`, на другом значении бросает ошибку,
  и рынок помечается как неподдерживаемый.
  (Моя интерпретация, не из документации: «пик 1.56%» из changelog февраля 2026 соответствует старой формуле
  `C × p × 0.25 × (p(1−p))^2`. Отсюда и устаревшие цифры в статьях.)
- Режим округления до 5 знаков (half-up/вниз/вверх) не указан. [TODO(verify)] Для бумажной торговли беру
  округление вверх, это консервативно.
- SELL: как списывается комиссия при продаже (из выручки в pUSD), явно не сказано. Сумма та же по формуле. [TODO(verify)]

### 5.2 Maker rebate [док]

[Maker Rebates Program](https://docs.polymarket.com/programs/maker-rebates).
- Пул = 20% taker-комиссий **этого рынка** (Crypto). Делится между мейкерами рынка пропорционально
  `fee_equivalent = C × feeRate × p × (1 − p)` их исполненных maker-ордеров. Выплата раз в день в pUSD, минимум 1 pUSD.
- Rebate для конкретной сделки заранее не известен. В PnL по умолчанию **не учитываем** (консервативно).
  В отчёте можно показать оценку сверху `≈ rebateRate × fee_equivalent`. Это мой вывод: сумма fee_equivalent
  всех мейкеров рынка примерно равна сумме taker-комиссий. В документации этого нет.
- [наблюдение] поле `makerRebatesFeeShareBps: 10000` у рынка; смысл не описан. [TODO(verify)]

### 5.3 Taker Rebate Program [док]

[Taker Rebate Program](https://docs.polymarket.com/programs/taker-rebates). Уровни по 30-дневному
weighted volume `wV = size × (1 − price) × 2.3` для крипто. Первый уровень (3%) с 2000 USD wV.
Сделка 5 USD при p = 0.5 даёт ≈ 5.75 wV. Для наших размеров это 0, в расчёт не берём.

---

## 6. Клиент и версии

| Пакет | Версия (PyPI, 2026-10-11) | Назначение |
|---|---|---|
| [`polymarket-client`](https://pypi.org/project/polymarket-client/) | **0.13.0** (2026-10-09), Python ≥ 3.11 | официальный «унифицированный» SDK: Gamma, CLOB, стримы (PolyBolt с 0.11.0), торговля. Зависимости: `httpx`, `websockets<16`, `pydantic 2`, `eth-account` и др. |
| [`py-clob-client-v2`](https://pypi.org/project/py-clob-client-v2/) | 1.2.0 (2026-09-25) | CLOB V2 клиент (после перехода на CLOB V2 и pUSD 2026-04-28) |
| `py-clob-client` | 0.34.6 | **устарел**, после CLOB V2 не работает. Не использовать. |

[Python SDK](https://docs.polymarket.com/getting-started/python), [SDK changelog](https://docs.polymarket.com/changelog/sdks),
[миграция на unified SDK](https://docs.polymarket.com/migrate/clob-sdk-to-unified-sdk).
В SDK `CryptoTwapPriceSpec(symbols=["btcusd"])` и `CryptoPriceSpec(...)` из `polymarket.streams`, только через `AsyncSecureClient`.

Предложение по M1–M4: **SDK не тянуть**. `feed.py` работает напрямую по WebSocket (`websockets`),
Gamma/CLOB через `httpx`. Причины: нужны только L2-ключи, а не приватный ключ; свой контроль
`last_data_ts` и переподключения (SDK переподключается сам и может скрыть «тихий» сокет); меньше зависимостей.
Для `ClobBroker` (M5) выбирать между `polymarket-client` и `py-clob-client-v2` отдельно.

---

## 7. Что не подтверждено (сводка TODO(verify))

1. Можно ли получить CLOB API-ключи для пустого кошелька без истории на Polymarket.
2. Можно ли поднять `AsyncSecureClient` только с L2-ключами, без приватного ключа.
3. Реальная частота точек `price.crypto.twap` и `price.crypto` (в примерах 1 с).
4. Совпадает ли точка TWAP-потока с `timestamp == eventStartTime` с `eventMetadata.priceToBeat` из Gamma.
5. Считается ли TWAP из того же Chainlink-спота, что отдаёт `price.crypto`.
6. Как входит `feeSchedule.exponent` при значении ≠ 1; режим округления комиссии до 5 знаков; списание комиссии при SELL.
7. Смысл `makerRebatesFeeShareBps`.
8. `orderMinSize = 5`: shares или USD.
9. Задержка taker на крипто: 150 мс (changelog) или 250 мс (справочник).
10. Схема slug `…-updown-5m-<start>` и поля `cryptoMarketConfig`, `eventMetadata` не описаны в документации
    (видны только в ответах). Основной путь поиска через `series_id` документирован.
11. Форматы Binance/Coinbase не проверялись.

## 8. Ограничения среды, где проверялось

Из облачного контейнера, где писались эти заметки: `gamma-api`, `clob`, `data-api` доступны;
`ws-live-v2.polymarket.com` закрыт сетевой политикой окружения (403 от прокси); `api.binance.com` отвечает 451.
Живое подключение к PolyBolt отсюда не проверено.
