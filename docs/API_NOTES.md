# API notes: Polymarket для updown-twap-bot

Проверено **2026-10-03** (около 08:50–09:05 UTC). Источник: https://docs.polymarket.com/llms.txt
и страницы по ссылкам ниже, плюс живые запросы к Gamma, CLOB REST и CLOB WebSocket.

Метки:
- **[DOC]** написано в документации (ссылка рядом);
- **[LIVE]** увидел в живом ответе API, в документации этого нет или там написано иначе;
- **TODO(verify)** не подтверждено. В коде под это делаем интерфейс, а не догадку.

---

## 0. Что изменилось по сравнению с `CLAUDE.md` и черновиком

| Было в черновике | Сейчас | Источник |
|---|---|---|
| 5m рынки рассчитываются по TWAP 30 с, 15m по TWAP 60 с | **Оба (5m и 15m) по TWAP 60 с.** 5m перевели с 30 с на 60 с 14.08.2026 | [DOC] [Changelog, 14.08.2026](https://docs.polymarket.com/changelog/predictions.md), [LIVE] `cryptoMarketConfig.twapLookbackSeconds = 60` у всех четырёх серий |
| RTDS `wss://ws-live-data.polymarket.com`, топики `crypto_prices_chainlink`, `crypto_prices_twap_thirty/sixty`, символ `btc/usd` | **PolyBolt** `wss://ws-live-v2.polymarket.com/ws`, каналы `price.crypto` и `price.crypto.twap`, символ `btcusd`. RTDS для цен помечен legacy | [DOC] [Migrate RTDS → PolyBolt](https://docs.polymarket.com/migrate/rtds-to-polybolt.md) |
| Цены публичные | **Для PolyBolt нужна авторизация CLOB API-ключом** (apiKey/secret/passphrase), даже чтобы просто читать цены | [DOC] [PolyBolt overview](https://docs.polymarket.com/api-reference/live-data/overview.md) |
| TWAP 30 с | **TWAP 30 с больше нет.** «Other windows do not provide data» | [DOC] там же, раздел Supported Symbols |
| Taker-комиссия «около 3%» | `fee = C × 0.07 × p × (1 − p)`, то есть 3.5% от суммы при p = 0.50 | [DOC] [Fees](https://docs.polymarket.com/trading/fees.md) |
| Клиент `py-clob-client` | Единый Python SDK `polymarket-client` (на PyPI 0.12.0). `py-clob-client` и `py-clob-client-v2` считаются старыми | [DOC] [Python SDK](https://docs.polymarket.com/getting-started/python.md), [SDK migration](https://docs.polymarket.com/migrate/clob-sdk-to-unified-sdk.md) |
| Залог USDC | **pUSD** (с 28.04.2026, CLOB V2) | [DOC] [Changelog, 17.04 и 28.04.2026](https://docs.polymarket.com/changelog/predictions.md) |

Следствия для кода:
- `config.example.toml`: `windows = [5, 15]` остаётся, но TWAP-окно у обоих 60 с. Комментарий «5m (TWAP 30 с)» устарел.
  В `store.py` колонка `twap_window` фактически всегда 60.
- `twap_signal.evaluate(..., window=60)` для обоих типов рынков. Для 5m это значит, что в последние 60 с из 300
  итоговая цена уже наполовину «зафиксирована» усреднением.
- Символы: `btcusd`, `ethusd`, а не `btc/usd`.

---

## 1. Цены: Chainlink TWAP и спот (PolyBolt)

Ссылки: [PolyBolt overview](https://docs.polymarket.com/api-reference/live-data/overview.md),
[Live Data Channel (message reference)](https://docs.polymarket.com/api-reference/wss/polybolt.md),
[Real-Time Data → Reference Prices](https://docs.polymarket.com/market-data/realtime-data.md),
[Migrate RTDS → PolyBolt](https://docs.polymarket.com/migrate/rtds-to-polybolt.md),
машиночитаемый контракт `https://ws-live-v2.polymarket.com/asyncapi.json`.

### 1.1 Подключение и авторизация [DOC]

1. Подключиться к `wss://ws-live-v2.polymarket.com/ws`.
2. Отправить `{"op":"auth","rid":"a1","auth":{"apiKey":"…","secret":"…","passphrase":"…"}}`.
3. Дождаться `{"op":"authed","rid":"a1"}`.
4. Подписаться:
   ```json
   {"op":"subscribe","rid":"s1","subscriptions":[
     {"channel":"price.crypto.twap","filter":{"symbol":"btcusd","window_seconds":60}},
     {"channel":"price.crypto.twap","filter":{"symbol":"ethusd","window_seconds":60}},
     {"channel":"price.crypto","filter":{"symbol":"btcusd"}},
     {"channel":"price.crypto","filter":{"symbol":"ethusd"}}
   ]}
   ```
   Пакет подписок считается за один запрос.

Ошибки авторизации (соединение остаётся открытым): `auth_required`, `auth_invalid`, `auth_unavailable`.

Ключи CLOB API получаются через L1-подпись EIP-712 приватным ключом кошелька:
`POST https://clob.polymarket.com/auth/api-key` (создать) или `GET /auth/derive-api-key` (получить существующие).
Ответ: `{apiKey, secret, passphrase}` ([DOC] [API → Authentication](https://docs.polymarket.com/getting-started/api.md)).
Нужны ли на кошельке средства, чтобы создать ключ, в документации не сказано: **TODO(verify)**.

[LIVE] Формат L1-подписи проверен скриптом `scripts/get_clob_api_key.py` (подпись байт в байт совпадает с SDK 0.12.0):
для нового адреса с верной подписью `GET /auth/derive-api-key` отвечает `400 {"error":"Could not derive api key!"}`
(ключа ещё нет), с испорченной подписью `401 {"error":"Invalid L1 Request headers"}`. Сам `POST /auth/api-key` не вызывался.
Cloudflare перед CLOB отвечает `403 error code: 1010` на User-Agent по умолчанию `Python-urllib`, поэтому нужен свой User-Agent.

### 1.2 Каналы и символы [DOC]

| Канал | Что | Фильтр | Провайдер |
|---|---|---|---|
| `price.crypto` | спот | `{"symbol":"btcusd"}` (+ опц. `"provider":"chainlink"\|"pyth"`) | по умолчанию Chainlink (с 02.10.2026) |
| `price.crypto.twap` | TWAP Chainlink, окно 60 с | `{"symbol":"btcusd","window_seconds":60}` | только Chainlink |

Символы для обоих каналов: `btcusd, ethusd, solusd, xrpusd, dogeusd, hypeusd, bnbusd, zecusd`.
Только нижний регистр и суффикс `usd`. `btc/usd`, `btcusdt`, `BTCUSD` отклоняются.

### 1.3 Формат сообщений [DOC]

```json
{"v":1,"channel":"price.crypto.twap","seq":3,"ts":1788886177000,
 "payload":{"symbol":"btcusd","value":…,"full_accuracy_value":"78803.715261094101516288",
            "timestamp":1788886177000,"window_seconds":60,"source":"chainlink"}}
```
- Сразу после подписки приходит снапшот `"snapshot": true`, в `payload.data[]` цены за предыдущие 2 минуты
  (может быть пустой). В примерах точки идут раз в секунду.
- `seq` идёт подряд в рамках соединения и канала и сбрасывается при переподключении. `dropped` показывает, сколько кадров
  пропущено, потому что клиент не успевал читать.
- Использовать `full_accuracy_value` (строка с точной десятичной записью), а не float `value`. E18-конвертация, как в старом
  RTDS, не нужна.
- `source` нужно перечитывать после каждого переподключения: у `price.crypto` провайдер может смениться.
- Свежесть проверять по `payload.timestamp` (время цены), а не по времени получения.

### 1.4 Живость и переподключение [DOC]

- Сервер шлёт WebSocket ping каждые 25 с. Стандартный клиент отвечает сам. Два пропущенных pong дают закрытие `4002`.
- Можно отправить `{"op":"ping"}` и получить `pong`.
- Коды закрытия: `4001` ошибка авторизации (сначала чинить, потом переподключаться); `4002` медленный клиент или нет pong
  (backoff с jitter); `4003` сервер выводится из работы (переподключиться через случайные 0–10 с); `4008` нарушение политики
  (это баг у нас); `1006` (backoff с full jitter от 1 с до 30 с). HTTP 429/503 при подключении: ждать не меньше `Retry-After`.
- После переподключения заново пройти авторизацию, заново подписаться и инициализироваться из нового снапшота.
- Лимиты: 64 подписки на соединение, 20 кадров subscribe/unsubscribe в секунду, кадр до 64 KB, 8 auth на соединение.

Это не отменяет нашего сторожа `last_data_ts` (`CLAUDE.md`, правило 5): протокольные ping/pong не гарантируют, что
данные идут.

### 1.5 Старый RTDS [DOC]+[LIVE]

`wss://ws-live-data.polymarket.com` (публичный, `PING` каждые 5 с) для цен помечен legacy. В SDK старые price-топики
deprecated, их удаление планировалось через месяц после выхода `@polymarket/client` 0.11.0.
[LIVE] 2026-10-03 RTDS ещё отдавал `crypto_prices_chainlink` для `btc/usd` без авторизации. Строить на этом нельзя.

### 1.6 Спот Binance/Coinbase для sigma: не проверено

Документацию Binance и Coinbase из этой среды прочитать не удалось: прокси блокирует `developers.binance.com` и
`docs.cdp.coinbase.com`. **TODO(verify)** эндпоинты, формат и гео-ограничения (Binance недоступен из США).
Проверенная альтернатива: `price.crypto` в том же PolyBolt-соединении (Chainlink по умолчанию, для btc/eth можно
выбрать `pyth`). См. вопрос 2 в плане.

---

## 2. Рынки Up/Down 5m и 15m (Gamma API)

Ссылки: [Discover Markets](https://docs.polymarket.com/market-data/discover-markets.md),
[Market Details](https://docs.polymarket.com/market-data/market-details.md),
[Gamma OpenAPI](https://docs.polymarket.com/api-spec/gamma-openapi.yaml), база `https://gamma-api.polymarket.com`.

### 2.1 Поиск активных рынков

Документированный способ [DOC]+[LIVE]:
```
GET /events?tag_slug=up-or-down&closed=false&end_date_min=<now ISO>&end_date_max=<now+30m ISO>&limit=100
```
Затем фильтр по `event.seriesSlug`:

| Серия | `seriesSlug` | series id [LIVE] |
|---|---|---|
| BTC 5m | `btc-up-or-down-5m` | 10684 |
| BTC 15m | `btc-up-or-down-15m` | 10192 |
| ETH 5m | `eth-up-or-down-5m` | 10683 |
| ETH 15m | `eth-up-or-down-15m` | 10191 |

Быстрый путь [LIVE], но формат slug нигде не описан (**TODO(verify)**, использовать только как запасной вариант):
`GET /events/slug/{asset}-updown-{5m|15m}-{unix_start}`, где `unix_start` кратен 300 или 900.
Например, `btc-updown-5m-1791017700` это окно 08:55–09:00 UTC. Рынки заводятся примерно за сутки вперёд.

Параметр `series_id` у `/events` в OpenAPI не описан. При `closed=false` он вернул старые незакрытые события 2025 года,
поэтому не используем.

### 2.2 Поля события и рынка (у Up/Down в событии один рынок)

| Что | Поле | Метка |
|---|---|---|
| Начало окна (момент strike) | `event.startTime` = `market.eventStartTime` | [LIVE]; `eventStartTime` есть в OpenAPI |
| Конец окна = время расчёта | `event.endDate` = `market.endDate` (ISO с временем) | [LIVE]. **Не** `endDateIso`: там только дата |
| Токены | `market.clobTokenIds` (JSON-строка), порядок соответствует `market.outcomes` = `["Up","Down"]` | [DOC] порядок по индексу, [LIVE] метки Up/Down |
| condition id | `market.conditionId` | [DOC] |
| Можно торговать | `active && !closed && acceptingOrders` | [DOC] |
| Тик и мин. размер | `orderPriceMinTickSize` (0.01), `orderMinSize` (5) | [DOC]+[LIVE] |
| Комиссия | `feesEnabled`, `feeSchedule{rate, exponent, takerOnly, rebateRate}` | [DOC]+[LIVE]: `0.07, 1, true, 0.2`, `feeType = "crypto_fees_v2"` |
| Параметры TWAP | `cryptoMarketConfig{id:"btc-5m-twap-60", asset, duration, twapEnabled, twapLookbackSeconds:60}` | [LIVE], в доках нет, **TODO(verify)** стабильность поля |
| Источник расчёта | `resolutionSource = https://data.chain.link/streams/btc-usd-twap-60s-streams` | [LIVE] |
| Гео-ограничение | `restricted: true` | [DOC] значение поля, [LIVE] true у этих рынков |

`makerBaseFee`/`takerBaseFee` (1000) это старые поля, для расчёта комиссии не используем. Комиссия считается по
`feeSchedule` (см. §4).

### 2.3 Правило расчёта и strike (price to beat)

[LIVE] Текст рынка: Up, если «TWAP Chainlink за указанный в заголовке интервал ≥ цены в начале интервала».
Источник: Chainlink BTC/USD TWAP 60s stream.
[DOC] [Changelog, 07.08.2026](https://docs.polymarket.com/changelog/predictions.md): «Both the price to beat and the
final settlement price come from the applicable TWAP feed». Окна усреднения сейчас 60 с (см. §0).

Отсюда strike = значение потока TWAP-60 в `eventStartTime`, итог = значение TWAP-60 в `endDate`.
Ровно какой принт берётся (точное совпадение секунды, последний до момента или первый после), не описано: **TODO(verify)**.

[LIVE] Поле `event.eventMetadata`:
- во время окна его **нет** (проверено на текущем 5m рынке);
- примерно через минуту после конца появляется `{"priceToBeat": 84608.904251…}`;
- позже добавляется `finalPrice` (`{"finalPrice": 84568.14069316, "priceToBeat": 84561.342236…}`).

В документации поля нет. **TODO(verify)**. Вывод: strike во время окна берём из своего TWAP-потока, а после закрытия
сверяем с `eventMetadata.priceToBeat`. Сверку записываем в JSONL, это готовый тест на правильность strike.

---

## 3. Стакан CLOB

Ссылки: [Prices and Order Books](https://docs.polymarket.com/market-data/prices-order-books.md),
[Market Channel](https://docs.polymarket.com/api-reference/wss/market.md),
[Real-Time Data → Market Stream](https://docs.polymarket.com/market-data/realtime-data.md),
[Get order book](https://docs.polymarket.com/api-reference/market-data/get-order-book.md).

### 3.1 WebSocket (публичный, без авторизации) [DOC]+[LIVE]

- `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- Подписка: `{"assets_ids":["<token_up>","<token_down>"],"type":"market","custom_feature_enabled":true}`.
  Опционально `initial_dump` (по умолчанию true) и `level` (1/2/3, по умолчанию 2; что означают уровни, не описано,
  **TODO(verify)**).
- Добавлять и убирать токены без переподключения: `{"assets_ids":[…],"operation":"subscribe"|"unsubscribe"}`.
- Heartbeat: отправлять текстовый кадр `PING` каждые 10 с, ответ `PONG`.
- События (`event_type`): `book` (полный агрегированный стакан), `price_change` (дельта уровня, size = новый суммарный
  объём, 0 означает, что уровень удалён; в элементах есть `best_bid`/`best_ask`), `last_trade_price` (с `fee_rate_bps`),
  `tick_size_change`, а с `custom_feature_enabled` ещё `best_bid_ask`, `new_market`, `market_resolved`.
- Сообщение может быть JSON-массивом событий [LIVE].

**Порядок уровней [LIVE]:** и в WS `book`, и в REST `/book` bids шли по возрастанию цены, asks по убыванию, то есть
лучшая цена в **конце** массива. В справочнике REST написано наоборот («bids descending, asks ascending»).
Вывод: лучшие цены всегда считаем как `max(bids)` и `min(asks)` и не полагаемся на порядок.

**Поток очень плотный [LIVE]:** 4 токена (BTC 5m + BTC 15m) в начале окна дали **470–680 кадров/с и 290–430 KB/с**.
В основном это `price_change`. Сырой поток в JSONL писать нельзя: выйдет десятки GB в сутки. Нужна прореженная запись
(см. план).

### 3.2 REST [DOC]+[LIVE]

`GET https://clob.polymarket.com/book?token_id=…`: `market, asset_id, timestamp (ms), hash, bids[], asks[],
min_order_size, tick_size, neg_risk, last_trade_price`. Есть пакетный `POST /books`. Нужен для начальной загрузки и сверки.

`GET /clob-markets/{condition_id}` (Get CLOB market info) отдаёт `fd{r, e, to}` (fee rate, exponent, taker-only), `mts`,
`mos`, `itode` (включена ли taker delay).

Лимиты (Cloudflare, по IP): CLOB в целом 9000 запросов за 10 с, Gamma `/events` 500 за 10 с
([Rate Limits](https://docs.polymarket.com/api-reference/rate-limits.md)).

---

## 4. Комиссии

Ссылки: [Fees](https://docs.polymarket.com/trading/fees.md),
[Maker Rebates Program](https://docs.polymarket.com/programs/maker-rebates.md),
[Market Details → Trading Fees](https://docs.polymarket.com/market-data/market-details.md),
[Taker Rebate Program](https://docs.polymarket.com/programs/taker-rebates.md).

### 4.1 Taker-комиссия [DOC]

```
fee (USD) = C × feeRate × p × (1 − p)
```
`C` число акций, `p` цена акции. Для Crypto `feeRate = 0.07`. Мейкеры комиссию не платят («Makers are never charged fees»).
Комиссия симметрична относительно 0.50. Округляется до 5 знаков, минимум 0.00001; всё меньше округляется в 0.

Пример из документации, Crypto, 100 акций (в таблице значения округлены до центов):

| p | Сумма | Комиссия |
|---|---|---|
| 0.01 | $1 | $0.07 |
| 0.05 | $5 | $0.33 |
| 0.10 | $10 | $0.63 |
| 0.20 | $20 | $1.12 |
| 0.30 | $30 | $1.47 |
| 0.40 | $40 | $1.68 |
| 0.50 | $50 | $1.75 |
| 0.60 | $60 | $1.68 |
| 0.70 | $70 | $1.47 |
| 0.80 | $80 | $1.12 |
| 0.90 | $90 | $0.63 |
| 0.95 | $95 | $0.33 |
| 0.99 | $99 | $0.07 |

В долях: на одну акцию `0.07 × p × (1−p)` (1.75¢ при p = 0.5); от суммы сделки `0.07 × (1−p)` (3.5% при p = 0.5,
0.7% при p = 0.9). Для edge это значит: покупка по ask = a стоит `a + 0.07·a·(1−a)` за акцию.

- У рыночного BUY сумма указывается до комиссии, комиссия берётся сверху ([DOC] Place Orders, «Cap Market Buy Spending»).
- Комиссия определяется протоколом в момент матчинга, в ордере её не передают ([DOC] Fees; CLOB V2).
- **`exponent`** в `feeSchedule`: в формуле на странице Fees его нет. Для крипторынков он сейчас равен 1 [LIVE], и таблица
  совпадает с формулой выше. Как он входит в формулу при значении ≠ 1, не описано: **TODO(verify)**. В `fees.py`
  поддерживаем только `exponent == 1`, на другом значении бросаем исключение.
- Режим округления до 5 знаков не указан: **TODO(verify)**. В бумажной торговле округляем вверх (консервативно).
- С продаж комиссия, вероятно, вычитается из выручки. Явно это не сказано: **TODO(verify)**. На PnL это не влияет:
  считаем комиссию отдельной суммой в USD.
- Комиссия за redeem после расчёта не упоминается: **TODO(verify)**. Пока считаем 0.

### 4.2 Maker rebate [DOC]

- Пул = `rebateRate` × собранные taker-комиссии рынка. Для Crypto это 20% (`feeSchedule.rebateRate = 0.2`).
- Пул делится между мейкерами рынка пропорционально `fee_equivalent = C × feeRate × p × (1−p)` по их исполненным мейкерским
  ордерам: `rebate = my_fee_equivalent / total_fee_equivalent × pool`. Выплата раз в сутки в pUSD, минимум $1.
- Отсюда следует, что заранее посчитать ребейт по сделке нельзя: он зависит от чужого объёма. Верхняя граница (если мы
  единственный мейкер в рынке) равна `0.2 × fee_equivalent`.

### 4.3 Taker Rebate Program [DOC]

Тиры по 30-дневному взвешенному объёму. При наших размерах ($5–20 на сделку) это тир 0, ребейт 0%. Не учитываем.

---

## 5. Клиент и версии

- [DOC] Официальный Python SDK: **`polymarket-client`**, импорт `polymarket`, Python ≥ 3.11. Классы `AsyncPublicClient`,
  `AsyncSecureClient`. Realtime-подписки есть только у async-клиентов (`client.subscribe(...)`; спеки
  `CryptoTwapPriceSpec`, `CryptoPriceSpec`, `MarketSpec` из `polymarket.streams`). PolyBolt поддерживается с 0.11.0.
- [LIVE] На PyPI последняя версия **0.12.0** (requires_python ≥ 3.11; зависимости httpx, websockets < 16, pydantic 2,
  eth-account и др.). В SDK changelog на сайте последняя описанная версия 0.11.0, что поменялось в 0.12.0, не проверено:
  **TODO(verify)**.
- SDK сам делает авторизацию, heartbeat и переподключение для PolyBolt. Но в документированных Python-типах событий нет
  `source`, и SDK не отдаёт сырые кадры.
- Старые `py-clob-client`, `py-clob-client-v2`, `py-builder-*` по
  [гайду миграции](https://docs.polymarket.com/migrate/clob-sdk-to-unified-sdk.md) нужно удалить и перейти на единый SDK.
- Python в этой среде: 3.11.15.

---

## 6. На будущее (M5, ClobBroker): факты, которые влияют на PaperBroker уже сейчас

- **Taker delay на крипторынках: 150 мс** (с 04.09.2026) ([DOC] Changelog). В OpenAPI у поля `itode` всё ещё написано
  250 мс (устарело). Задержка в PaperBroker должна быть не меньше этого значения.
- Типы ордеров: GTC, GTD (истекает за 1 минуту до указанного времени), рыночные FAK и FOK, флаг `post_only` для мейкерских
  ([DOC] [Place Orders](https://docs.polymarket.com/trading/place-orders.md)).
- Рестарты матчинга: после рестарта 2 минуты принимаются только post-only ордера ([DOC]
  [Matching Engine](https://docs.polymarket.com/trading/matching-engine.md)).
- Есть эндпоинт heartbeat, который отменяет все ордера, если heartbeat не приходит
  ([DOC] [Send heartbeat](https://docs.polymarket.com/api-reference/trade/send-heartbeat.md)).
- Гео-ограничения: рынки помечены `restricted: true`. Перед live нужно проверить
  [Geographic Restrictions](https://docs.polymarket.com/api-reference/geoblock.md).
- `orderMinSize = 5`: в Gamma написано «Minimum order size in USDC», в SDK «minimum USDC notional». Исторически это были
  акции. Что именно, **TODO(verify)**.

---

## 7. Сводка «не подтверждено»

| # | Что | Как проверить | Что делаем до проверки |
|---|---|---|---|
| V1 | Какой принт TWAP-60 становится strike и итогом (граница секунды) | Сравнить свои записи TWAP в `eventStartTime`/`endDate` с `eventMetadata.priceToBeat`/`finalPrice` | Strike = последний принт с `timestamp ≤ eventStartTime`, параметр настраивается |
| V2 | `eventMetadata`, `cryptoMarketConfig` (недокументированные поля Gamma) | Наблюдать; спросить в Discord/Telegram API | Только для сверки и проверки окна, не для торговых решений |
| V3 | Формат slug `{asset}-updown-{dur}-{ts}` | Не документирован | Основной путь `tag_slug` + `seriesSlug`, slug как запасной |
| V4 | Роль `feeSchedule.exponent` ≠ 1 | Документация или поддержка | `fees.py` бросает исключение, рынок не торгуем |
| V5 | Режим округления комиссии до 1e-5 | Сверить с реальными fills на M5 | В бумажной торговле округляем вверх |
| V6 | Комиссия при продаже и при redeem | M5, реальные сделки | Комиссия отдельной суммой; redeem 0 |
| V7 | Нужны ли средства на кошельке, чтобы создать CLOB API-ключ | Попробовать с пустым кошельком | Вопрос человеку |
| V8 | Binance/Coinbase WS: эндпоинты, формат, гео | Доки заблокированы в этой среде | Предлагаю PolyBolt `price.crypto` |
| V9 | `level` в подписке market WS | Не описан | Не передаём (по умолчанию 2) |
| V10 | Порядок уровней в `book` (наблюдение противоречит REST-справочнику) | Видно в данных | Лучшие цены всегда через max/min |
| V11 | Что нового в `polymarket-client` 0.12.0 | Release notes на GitHub | Фиксировать версию |
| V12 | `orderMinSize`: акции или USD | M5 | Проверять оба условия |
| V13 | PolyBolt не проверен вживую: этот sandbox блокирует `ws-live-v2.polymarket.com` (403 от egress-прокси), и ключей нет | Запуск на машине человека | Протокол строго по доке, тесты на фейковом сервере |

## 8. Что проверено вживую из этой среды

- Gamma `/events/slug/...` и `/events?tag_slug=up-or-down…`: ответы и поля, как описано выше.
- CLOB WS `ws/market`: подписка, события `book`/`price_change`/`last_trade_price`/`best_bid_ask`/`new_market`,
  `PING`/`PONG`, замер плотности потока.
- CLOB REST `/book`: формат и порядок уровней.
- RTDS legacy: ещё отдаёт Chainlink-спот без авторизации.
- CLOB L1-авторизация: формат подписи принят сервером (400 для адреса без ключа, 401 для плохой подписи).
- PolyBolt: **не проверен**, хост заблокирован прокси этой среды.
