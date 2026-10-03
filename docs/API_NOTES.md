# API_NOTES — Polymarket Up/Down 5m/15m (BTC/ETH)

Дата сверки: **2026-10-03**. Источник истины: <https://docs.polymarket.com/llms.txt> и страницы, на которые он ведёт.
Где написано «проверено вживую» — я сам сделал запрос к публичному API в этот день из облачной песочницы.
Всё остальное — со слов документации. Что не подтверждено, собрано в разделе 9 и помечено `TODO(verify)`.

## 0. Главное: что отличается от CLAUDE.md и `config.example.toml`

| # | Было в черновике | Сейчас по документации | Влияние |
|---|---|---|---|
| 1 | 5m-рынки: TWAP 30 с; 15m: TWAP 60 с | **Все 5m и 15m рынки считаются по 60-секундному Chainlink TWAP.** 30 с действовал 7–14 авг 2026, затем заменён на 60 с ([changelog](https://docs.polymarket.com/changelog/predictions.md)). Проверено вживую: `cryptoMarketConfig.twapLookbackSeconds = 60` у `btc-5m-twap-60`, `btc-15m-twap-60`, `eth-15m-twap-60` | `window` в `twap_signal.evaluate` = 60 для всех рынков. Топика на 30 с в новом сокете нет |
| 2 | Символы `btc/usd` | PolyBolt: `btcusd`, `ethusd` (строчные, без слэша, `usd`, не `usdt`) | `symbols` в конфиге и в `positions.symbol` |
| 3 | RTDS `wss://ws-live-data...` | **RTDS для цен — legacy.** Новый сокет PolyBolt `wss://ws-live-v2.polymarket.com/ws`, **требует CLOB API credentials даже для чтения цен** | Нужны ключи уже на M1 (см. вопросы). Старый RTDS в моей проверке живых обновлений не присылал (раздел 2.4) |
| 4 | Комиссия «около 3% у 0.50» | `fee = C × 0.07 × p × (1−p)`; в % от суммы сделки **3.5% при p = 0.50**, 5.6% при 0.20, 0.7% при 0.90 (в долларах на акцию пик 1.75 цента при 0.50) | Плоский `cost = 0.02` в `twap_signal.evaluate` почти целиком уходит на комиссию на 0.50 (1.75 цента/акцию) и ничего не оставляет на спред и проскальзывание. В M3 `cost` = комиссия(p) + проскальзывание |
| 5 | Strike (price to beat) «берём с рынка» | **Gamma не отдаёт числовой strike.** Strike = значение TWAP на момент начала окна (раздел 3.3) | Его нужно записывать самим из потока TWAP; при старте бота посреди окна strike текущего рынка недоступен |
| 6 | maker-rebate как скидка к сделке | Rebate — **ежедневный пул-выплата**, пропорциональная доле maker-объёма, минимум $1/день | Нельзя посчитать на сделку. Предложение: в основных метриках rebate = 0, отдельной строкой — оценка (вопрос 7) |

## 1. Клиент и версии

- Официальный Python SDK: пакет **`polymarket-client`**, импорт `polymarket` ([docs](https://docs.polymarket.com/getting-started/python.md), [PyPI](https://pypi.org/project/polymarket-client/), [GitHub](https://github.com/Polymarket/py-sdk/)).
- Проверено вживую (PyPI): последняя версия **0.12.0**, `requires_python >= 3.11`. Документация в changelog упоминает 0.11.0 как первую версию с PolyBolt ([SDK changelog](https://docs.polymarket.com/changelog/sdks.md)). SDK до 1.0, API может меняться — пинить точную версию.
- Клиенты: `AsyncPublicClient` (публичные данные), `AsyncSecureClient` (торговля и приватные потоки). Подписки (`subscribe`) есть **только в async-клиентах**.
- Старые клиенты (`py-clob-client` и т.п.) заменены на унифицированный SDK: [миграция](https://docs.polymarket.com/migrate/clob-sdk-to-unified-sdk.md).
- Окружение здесь: Python 3.11.15.
- Прямые эндпоинты: Gamma `https://gamma-api.polymarket.com`, CLOB `https://clob.polymarket.com`, Data API `https://data-api.polymarket.com/v2` ([API overview](https://docs.polymarket.com/getting-started/api.md)).

## 2. Цены: TWAP и спот

### 2.1 PolyBolt WebSocket (актуальный)

Источники: [overview](https://docs.polymarket.com/api-reference/live-data/overview.md), [migration RTDS→PolyBolt](https://docs.polymarket.com/migrate/rtds-to-polybolt.md), [Reference Prices](https://docs.polymarket.com/market-data/realtime-data.md#reference-prices), [machine-readable contract](https://ws-live-v2.polymarket.com/asyncapi.json).

- URL: `wss://ws-live-v2.polymarket.com/ws`
- Каналы:
  - `price.crypto` — спот-цена, фильтр `{"symbol":"btcusd"}`. С 2 окт 2026 по умолчанию **Chainlink** (раньше было Pyth/Binance). Можно закрепить `"provider":"pyth"` (для btc/eth/sol/xrp/doge/bnb).
  - `price.crypto.twap` — 60-секундный Chainlink TWAP, фильтр `{"symbol":"btcusd","window_seconds":60}`. Другие окна «не дают данных».
- Символы TWAP: `btcusd, ethusd, solusd, xrpusd, dogeusd, hypeusd, bnbusd, zecusd`.
- Аутентификация (сначала `auth`, дождаться `authed`, потом `subscribe`):
  ```json
  {"op":"auth","rid":"a1","auth":{"apiKey":"…","secret":"…","passphrase":"…"}}
  {"op":"subscribe","rid":"s1","subscriptions":[
    {"channel":"price.crypto.twap","filter":{"symbol":"btcusd","window_seconds":60}},
    {"channel":"price.crypto","filter":{"symbol":"btcusd"}}]}
  ```
  Ошибки (соединение остаётся открытым): `auth_required`, `auth_invalid`, `auth_unavailable`.
- Конверт: `{"v":1,"channel","seq","ts"(мс),"snapshot"?,"dropped"?,"payload"}`.
  - живой апдейт: `payload = {symbol, value(float), full_accuracy_value(строка-decimal), timestamp(мс), source}`;
  - снапшот: `payload = {symbol, source, data:[{timestamp,value,full_accuracy_value}…]}` — последние **две минуты**, на каждую подписку один снапшот (возможно с пустым `data`).
  - `seq` последовательный на (соединение, канал), **сбрасывается при реконнекте**; `dropped` — сколько кадров потеряно из-за медленного клиента.
  - Брать `full_accuracy_value` (строка decimal), не float. Старый RTDS отдавал E18-целое — **для PolyBolt масштабировать не нужно**.
  - `source` перечитывать после каждого реконнекта (поставщик может смениться).
- Частота: спот до 5 обновлений/с на feed (из раздела Crypto Prices). Частота TWAP в документации не указана; в примерах метки времени идут с шагом 1 с.
- Лимиты: 64 подписки на соединение; 20 subscribe/unsubscribe-кадров в секунду; кадр ≤ 64 КБ; 8 auth-кадров на соединение. Нарушение → ошибка и закрытие `4008`.
- Heartbeat: сервер шлёт WebSocket-ping каждые 25 с, два пропущенных pong → закрытие `4002`. Есть и прикладной `{"op":"ping"}` → `pong`.
- Коды закрытия: `4001` auth (чинить ключи, не реконнектиться вслепую); `4002` медленный потребитель/pong (backoff+jitter); `4003` сервер дренируется (реконнект через случайные 0–10 с); `4008` нарушение политики (баг клиента); `1006` обрыв (backoff с full jitter, 1→30 с). При HTTP 429/503 — ждать не меньше `Retry-After`.
- После реконнекта: заново `auth`, заново все подписки, заново инициализироваться из снапшотов.
- **Важно для правила 5 (зависший сокет):** протокол-уровневый ping/pong не доказывает, что приходят *данные*. `last_data_ts` надо обновлять только по кадрам с `payload`, а не по `pong`/`subscribed`.

### 2.2 Как получить CLOB API credentials

[Authentication](https://docs.polymarket.com/getting-started/api.md#authentication): L1 — кошелёк подписывает EIP-712 `ClobAuth`, получается/выводится набор `apiKey / secret / passphrase`; L2 — запросы подписываются HMAC этими ключами. В SDK: `AsyncSecureClient.create(private_key=…)` «выводит или получает» ключи.
Для PolyBolt нужны только эти три значения. `TODO(verify)`: можно ли получить их для нового кошелька без пополнения и онбординга.

### 2.3 SDK-вариант подписки (для справки)

```python
from polymarket import AsyncSecureClient
from polymarket.streams import CryptoTwapPriceSpec, CryptoPriceSpec
client = await AsyncSecureClient.create(private_key=os.environ["POLYMARKET_PRIVATE_KEY"])
async with await client.subscribe([CryptoTwapPriceSpec(symbols=["btcusd"]),
                                   CryptoPriceSpec(symbols=["btcusd"])]) as stream:
    async for event in stream: ...   # event.type == "subscribe" (снапшот) | "update"
```
SDK сам делает auth, heartbeat и reconnect — это удобно, но прячет логику реконнекта от кода детекции зависания (правило 5). Не запускалось: PolyBolt из песочницы недоступен (2.5).

### 2.4 Старый RTDS (legacy)

`wss://ws-live-data.polymarket.com`, публичный, без ключей. Топики: `crypto_prices_twap_sixty`, `crypto_prices_twap_thirty`, `crypto_prices_chainlink`, `crypto_prices` (Binance, `btcusdt`) ([карта топиков](https://docs.polymarket.com/migrate/rtds-to-polybolt.md#topic-mapping)). Для SDK удаление старых топиков запланировано «через месяц после релиза 0.11.0».

Проверено вживую 2026-10-03: соединение принимается, на подписку приходят **снапшоты** (≈59 точек за минуту; у TWAP `window_s` 30 и 60, значения в E18-строке `full_accuracy_value` и в float `value`). После этого за 15 с и (отдельным прогоном, с текстовым `PING` каждые 5 с) за 25 с **ни одного живого `update`** не пришло, соединение при этом не рвалось. Также на подписку `crypto_prices_chainlink` пришёл кадр с топиком `crypto_prices`. Почему так — не выяснено. Вывод: **как источник для бота не использовать**; это ровно тот «тихий зависший сокет», от которого защищает правило 5.

### 2.5 Ограничение песочницы

Хост `ws-live-v2.polymarket.com` блокируется сетевой политикой этой облачной среды (прокси отвечает 403 на CONNECT; в `/__agentproxy/status` — `connect_rejected`). Поэтому **PolyBolt я здесь живьём не проверял**; всё в 2.1 — по документации. Остальные хосты Polymarket (gamma, clob, ws-live-data, ws-subscriptions-clob) доступны.

## 3. Рынки Up/Down

### 3.1 Поиск (проверено вживую)

- Слаг события и рынка: **`{asset}-updown-{5m|15m}-{unix_начала_окна}`**, например `btc-updown-5m-1791014400`, `eth-updown-15m-1791014400`. Начало окна кратно 300 с (5m) / 900 с (15m).
- Запрос: `GET https://gamma-api.polymarket.com/events/slug/{slug}` ([Get event by slug](https://docs.polymarket.com/api-reference/events/get-event-by-slug.md)). Для текущего и следующего окна все 8 комбинаций (btc/eth × 5m/15m × 2 окна) вернули 200. Рынки создаются заранее: `startDate` события ≈ за сутки до начала окна.
- Альтернатива — перечисление: серии `btc-up-or-down-5m` (`recurrence: "5m"`), тег `up-or-down`; [Series](https://docs.polymarket.com/api-reference/series/list-series.md), [List events](https://docs.polymarket.com/api-reference/events/list-events-keyset-pagination.md). Не проверялось, только слаг.
- Поля рынка (`markets[0]`):
  - `conditionId`; `clobTokenIds` — **JSON-строка** со списком из двух token_id; `outcomes` — JSON-строка `["Up","Down"]`, порядок соответствует `clobTokenIds` (проверять по `outcomes`, не по позиции вслепую);
  - `eventStartTime` (ISO, начало окна) и `endDate` (ISO, конец окна = время расчёта);
  - `acceptingOrders`, `orderPriceMinTickSize` = 0.01, `orderMinSize` = 5 акций;
  - `feesEnabled`, `feeSchedule = {rate:0.07, exponent:1, takerOnly:true, rebateRate:0.2}`, `feeType: "crypto_fees_v2"`;
  - `cryptoMarketConfig = {id:"btc-5m-twap-60", asset, duration, twapEnabled, twapLookbackSeconds:60}`;
  - `resolutionSource`: `https://data.chain.link/streams/btc-usd-twap-60s-streams` (для ETH — `eth-usd-…`).
  - `makerBaseFee`/`takerBaseFee` = 1000 в ответе Gamma — это **не** текущая формула; комиссию считать только по `feeSchedule` ([changelog 31 мар 2026](https://docs.polymarket.com/changelog/predictions.md)).
- `restricted: true` стоит на событиях; это не то же самое, что гео-блок (см. раздел 7).

### 3.2 Правило расчёта

Из описания рынка и [changelog от 7 авг 2026](https://docs.polymarket.com/changelog/predictions.md): «Up», если TWAP Chainlink за указанный в заголовке интервал ≥ цене в начале интервала, иначе «Down». «Цена для сравнения (price to beat) и итоговая цена расчёта берутся из соответствующего TWAP-потока». То есть и strike, и финал — значения **60-секундного TWAP**, а не спота. Это согласуется с `effective_var_time(secs_left, window=60)` в `twap_signal.py`.

### 3.3 Strike

- В Gamma и CLOB числового strike **нет** (просмотрены все поля события и рынка).
- Рабочая гипотеза: `strike` = значение потока `price.crypto.twap` с меткой времени `eventStartTime`. Снапшот PolyBolt хранит только последние 2 минуты, поэтому strike надо **записывать самим** (все принты TWAP — в JSONL) и искать по метке времени.
- Следствие: если бот стартовал позже чем через ~2 минуты после начала окна, strike этого рынка неизвестен — рынок пропускаем до следующего окна. Для 15m это до 13 минут простоя при старте.
- `TODO(verify)`: совпадает ли это значение с «Price to beat» на сайте; какую именно метку брать (ровно `T0`, ближайшую до или после); как Chainlink округляет/сэмплирует. Проверка: сравнить записанные принты с цифрой на сайте для 3–5 рынков.

## 4. Стакан CLOB

### 4.1 REST

[`GET https://clob.polymarket.com/book?token_id=…`](https://docs.polymarket.com/api-reference/market-data/get-order-book.md): `{market, asset_id, timestamp, hash, bids:[{price,size}], asks:[…], min_order_size, tick_size, neg_risk, last_trade_price}` (числа — строки). Также `/price`, `/midpoint`, `/spread`, `/tick-size`, `/fee-rate`, `/clob-markets/{condition_id}` (параметры комиссии `fd:{r,e,to}`, `itode` — признак задержки тейкера). Публично, без ключей.

### 4.2 WebSocket (проверено вживую)

- `wss://ws-subscriptions-clob.polymarket.com/ws/market` — публичный ([market channel](https://docs.polymarket.com/api-reference/wss/market.md), [Real-Time Data → Market Stream](https://docs.polymarket.com/market-data/realtime-data.md#market-stream)).
- Подписка: `{"assets_ids":[tokenUp,tokenDown],"type":"market","custom_feature_enabled":true}`. Динамически: `{"operation":"subscribe"|"unsubscribe","assets_ids":[…]}`. `custom_feature_enabled` добавляет `best_bid_ask`, `new_market`, `market_resolved`.
- Heartbeat: клиент шлёт **текстовый `PING` каждые 10 с**, сервер отвечает `PONG`.
- Наблюдалось: первым пришёл JSON-**массив** кадров `book` (по одному на токен: `market, asset_id, timestamp(мс, строка), hash, bids, asks, …`), затем кадры `price_change` с `price_changes:[{asset_id, price, size, side:"BUY"|"SELL", hash, best_bid, best_ask}]`. Ещё есть `last_trade_price`, `tick_size_change`.
- **Порядок уровней.** Руководство ([Order Book](https://docs.polymarket.com/market-data/prices-order-books.md#order-book)) и живой кадр: bids по возрастанию, asks по убыванию, лучший — **последний** элемент. OpenAPI-описание `/book` утверждает обратное (bids по убыванию). Поэтому в `book.py` лучший уровень брать как `max(bids)` и `min(asks)`, порядок не предполагать.
- Свежесть: у кадров есть `timestamp` (мс) и `hash`. Для стакана хранить время последнего кадра и считать устаревшим после порога.
- `TODO(verify)`: `size` в `price_change` — это новый суммарный объём на уровне (как в `book`) или дельта; `price_change` с `size="0"` — удаление уровня; лимиты подписки по числу токенов.

## 5. Комиссии и rebate

Источники: [Fees](https://docs.polymarket.com/trading/fees.md), [Maker Rebates](https://docs.polymarket.com/programs/maker-rebates.md), [Market Details → Trading Fees](https://docs.polymarket.com/market-data/market-details.md#trading-fees), [Taker Rebates](https://docs.polymarket.com/programs/taker-rebates.md).

```
fee = C × feeRate × p × (1 − p)      # C — число акций, p — цена; в USDC/pUSD
```

- Крипто: `feeRate = 0.07` (проверено вживую в `feeSchedule` всех четырёх типов рынков), maker fee = 0, `takerOnly = true`.
- Пример из документации (100 акций, крипто): p=0.50 → $1.75; 0.30 → $1.47; 0.10 → $0.63; 0.01 → $0.07; 0.95 → $0.33 (полная таблица на странице Fees; она станет тестами `fees.py`).
- В % от суммы сделки (`fee / (C·p) = 0.07·(1−p)`): 3.5% при p=0.50, 5.6% при 0.20, 0.7% при 0.90. В долларах на акцию (`0.07·p·(1−p)`) пик 1.75 цента при 0.50 и спад к краям — в этом смысле «меньше у краёв» верно, в процентах от вложенного — наоборот.
- Округление: до 5 знаков, минимум 0.00001; меньше — ноль. Режим округления (вверх/вниз/к ближайшему) в документации не указан.
- Симметрия: комиссия при p и 1−p одинакова.
- Maker rebate: 20% собранных taker-комиссий крипто-рынков, делится **по рынку** между мейкерами пропорционально `C × feeRate × p(1−p)` их исполненных мейкерских ордеров; выплата раз в день, минимум $1 накопленного. Пока Polymarket может менять процент «по своему усмотрению».
- Taker rebate (tier-программа с 28 мая 2026): зависит от 30-дневного weighted volume и категории; для бота не моделируем.
- Задержка тейкера на крипто-рынках: **150 мс** с 4 сен 2026 (было 50 мс с 17 авг, до этого 250 мс), [changelog](https://docs.polymarket.com/changelog/predictions.md). Значение `latency_ms = 300` в `[paper]` её покрывает, но это надо держать ≥ 150.
- В Gamma у рынка есть `exponent` в `feeSchedule` (сейчас 1). Для других значений формула в документации не дана (раздел 9).

## 6. Ордера (нужно позже, M5; здесь только справка)

[Place Orders](https://docs.polymarket.com/trading/place-orders.md): лимитные GTC/GTD, рыночные FAK/FOK, `postOnly` (ордер, который пересёк бы спред, отклоняется — подходит для maker-входа). Минимальный размер 5 акций, шаг цены 0.01. GTD истекает за минуту до указанного срока. Ответ на размещение содержит статус, в том числе `delayed` (маркетабельный ордер на задержке тейкера). Приватный поток ордеров/сделок: [Real-Time Order Updates](https://docs.polymarket.com/trading/realtime-order-updates.md). Лимиты запросов: [Rate Limits](https://docs.polymarket.com/api-reference/rate-limits.md), [CLOB Trading Rate Limits](https://docs.polymarket.com/api-reference/trading-rate-limits.md). Heartbeat-эндпоинт `POST /heartbeats` ([Send heartbeat](https://docs.polymarket.com/api-reference/trade/send-heartbeat.md)): если пинги не приходят регулярно, все открытые ордера пользователя отменяются автоматически — учесть в `ClobBroker` (периодичность в описании не найдена, `TODO(verify)`).
В M1–M4 ничего из этого раздела не вызывается; M5 — только после приёмки M1–M4.

## 7. Гео-ограничения

[Geographic Restrictions](https://docs.polymarket.com/api-reference/geoblock.md): ордера из ряда юрисдикций отклоняются; часть — полный блок (в т.ч. закрытие позиций), часть — только закрытие. Проверка: `GET https://polymarket.com/api/geoblock`. Чтение публичных данных ограничением не затрагивается, но для live это надо проверить с того IP, где будет работать бот. Полный список страны/регионы я не выписывал.

## 8. Резолюция и погашение

[Resolution](https://docs.polymarket.com/concepts/resolution.md). Общий порядок: победившая акция гасится по $1, проигравшая — 0; итог определяется через UMA Optimistic Oracle (предложение → 2-часовое окно оспаривания, при споре дольше). Распространяется ли именно этот путь на крипто-рынки с Chainlink-TWAP, я не выяснил: Gamma у них показывает `umaResolutionStatuses: []`. Для бумажной торговли расчёт считаем сами: победившая акция = 1, проигравшая = 0, по финальному 60-секундному TWAP на `endDate` (раздел 3.2). Для калибровки **реальный исход** нужно брать с рынка (событие `market_resolved` в WS при `custom_feature_enabled`, либо Gamma/Data API `resolutions`), а не из собственных вычислений. Для live: деньги по проигравшим и выигравшим акциям освобождаются только после погашения, это может занимать часы (`TODO(verify)` для этих рынков). `TODO(verify)`: в какой момент после `endDate` рынок получает итог и в каком поле.

## 9. Не подтверждено (`TODO(verify)`)

1. **PolyBolt живьём** (авторизация, формат кадров, частота TWAP, поведение при зависании) — хост заблокирован в песочнице; всё по документации.
2. Можно ли получить CLOB API credentials новым кошельком без пополнения/онбординга.
3. Определение strike: метка времени, округление, совпадение с «Price to beat» на сайте (раздел 3.3).
4. Формула комиссии при `exponent ≠ 1` и режим округления до 5 знаков. До выяснения `fees.py` принимает только `exponent == 1` и иначе падает с ошибкой, а не догадывается. Старые публикации («пик 1.56%») относились к другой формуле (в Gamma тогда был `exponent=2`, `rate=0.25`), к текущему `exponent=1` они неприменимы.
5. Почему старый RTDS не присылает живые обновления (раздел 2.4) и когда его выключат.
6. Семантика `price_change.size`; лимиты подписок market-канала (раздел 4.2).
7. Порядок уровней в REST `/book` (OpenAPI и руководство противоречат друг другу).
8. Момент и поле итога рынка после расчёта (раздел 8).
9. Список «обычных» гео-ограничений для страны пользователя.
10. Перечисление рынков через серии/теги (используется только прямой слаг).

## 10. Нужные правки в существующих файлах (не делались, ждут ответа)

- `config.example.toml`: `windows = [5, 15]` — комментарий «5m (TWAP 30 с), 15m (TWAP 60 с)» устарел, оба 60 с; `symbols = ["btc/usd","eth/usd"]` → `["btcusd","ethusd"]`; `[signal] cost` — перейти на «комиссия(p) + проскальзывание»: плоские 0.02 почти целиком съедаются комиссией на 0.50.
- `CLAUDE.md` (пункт про RTDS и `feed.py`): указать PolyBolt и 60 с.
- `store.py`: схема не меняется; в комментариях `'btc/usd'` и «30 или 60» устарели. Переписывать не надо.
- `twap_signal.py`: логика остаётся; вызывать с `window=60`. Оговорка про sigma: PolyBolt `price.crypto` даёт Chainlink-спот (до 5 обновлений/с) — того же поставщика, что и расчётный поток, без Binance/Coinbase.
