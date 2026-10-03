"""
Сигнал для Up/Down рынков Polymarket на основе TWAP.

Отличия от варианта из статьи:
  1. z-score считается ДО добавления текущей точки в историю.
  2. z-score считается по приращениям (лог-доходностям), а не по уровням цены.
  3. Решение о входе принимает не пробой, а edge: оценка вероятности исхода
     минус цена, по которой реально можно купить.
  4. Учитываются обе стороны (Up и Down).

Зависимостей нет, только стандартная библиотека.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from statistics import NormalDist

_N = NormalDist()


@dataclass
class Print:
    ts: float      # unix time, секунды
    value: float   # значение TWAP


@dataclass
class Signal:
    side: str          # "up" или "down"
    p_fair: float      # оценка вероятности выбранной стороны
    ask: float         # цена, по которой покупаем
    edge: float        # p_fair - ask - издержки
    z_incr: float      # z-score последнего приращения (диагностика)
    sigma: float       # волатильность, доля цены на sqrt(сек)
    secs_left: float


class TwapState:
    """Скользящая история принтов TWAP по одному символу."""

    def __init__(self, maxlen: int = 120, min_prints: int = 20):
        self.prints: deque[Print] = deque(maxlen=maxlen)
        self.incr: deque[float] = deque(maxlen=maxlen)       # лог-приращения
        self.dts: deque[float] = deque(maxlen=maxlen)        # интервалы между принтами
        self.min_prints = min_prints

    def update(self, ts: float, value: float) -> float | None:
        """
        Добавляет принт и возвращает z-score нового приращения
        относительно ПРЕДЫДУЩИХ приращений (или None, если данных мало).
        """
        z = None
        if self.prints:
            prev = self.prints[-1]
            dt = ts - prev.ts
            if dt <= 0 or prev.value <= 0 or value <= 0:
                return None  # пропускаем дубликаты и мусор
            r = math.log(value / prev.value)

            # z-score СНАЧАЛА, потом добавление в историю
            if len(self.incr) >= self.min_prints:
                mean = sum(self.incr) / len(self.incr)
                var = sum((x - mean) ** 2 for x in self.incr) / len(self.incr)
                sd = math.sqrt(var)
                if sd > 0:
                    z = (r - mean) / sd

            self.incr.append(r)
            self.dts.append(dt)
        self.prints.append(Print(ts, value))
        return z

    def sigma_per_sqrt_sec(self) -> float | None:
        """
        Реализованная волатильность: sqrt(сумма r^2 / сумма dt).
        ВАЖНО: TWAP сглажен, поэтому это занижает волатильность спота.
        Если есть спот-фид (Binance/Coinbase), считай sigma по нему.
        """
        if len(self.incr) < self.min_prints:
            return None
        total_dt = sum(self.dts)
        if total_dt <= 0:
            return None
        return math.sqrt(sum(r * r for r in self.incr) / total_dt)

    @property
    def last(self) -> float | None:
        return self.prints[-1].value if self.prints else None


def effective_var_time(secs_left: float, window: float) -> float:
    """
    Расчёт идёт по среднему за последние `window` секунд, а не по споту.
    Дисперсия итогового TWAP при известной текущей цене:
      до начала окна усреднения: (s - W) + W/3
      внутри окна:               s^3 / (3 W^2)
    Приближение: считаем, что текущий TWAP ~ текущая цена.
    """
    s, w = secs_left, float(window)
    if s <= 0:
        return 0.0
    if s >= w:
        return (s - w) + w / 3.0
    return s ** 3 / (3.0 * w * w)


def fair_prob_up(twap_now: float, strike: float, sigma: float,
                 secs_left: float, window: float) -> float:
    """P(итоговый TWAP >= strike). strike = цена в начале интервала рынка."""
    vt = effective_var_time(secs_left, window)
    if vt <= 0 or sigma <= 0:
        return 1.0 if twap_now >= strike else 0.0
    # лог-пространство: ln(final/strike) ~ N(ln(now/strike), sigma^2 * vt)
    z = math.log(twap_now / strike) / (sigma * math.sqrt(vt))
    return _N.cdf(z)


def evaluate(state: TwapState, strike: float, secs_left: float, window: int,
             ask_up: float, ask_down: float,
             cost: float = 0.02, min_edge: float = 0.03,
             min_secs_left: float | None = None) -> Signal | None:
    """
    Возвращает Signal, если у какой-то стороны edge после издержек > min_edge.

    cost     - спред/комиссия/проскальзывание в долях цены контракта (0.02 = 2 цента)
    min_edge - дополнительный запас на ошибку модели
    min_secs_left - не входить, если до расчёта осталось меньше (по умолчанию 2*window)
    """
    if min_secs_left is None:
        min_secs_left = 2 * window
    if secs_left < min_secs_left:
        return None

    sigma = state.sigma_per_sqrt_sec()
    now = state.last
    if sigma is None or now is None:
        return None

    p_up = fair_prob_up(now, strike, sigma, secs_left, window)
    z_last = None  # z последнего приращения нужен только для логов
    if len(state.incr) >= state.min_prints:
        inc = list(state.incr)
        prev, last = inc[:-1], inc[-1]
        if len(prev) > 1:
            m = sum(prev) / len(prev)
            sd = math.sqrt(sum((x - m) ** 2 for x in prev) / len(prev))
            z_last = (last - m) / sd if sd > 0 else 0.0

    candidates = []
    for side, p, ask in (("up", p_up, ask_up), ("down", 1.0 - p_up, ask_down)):
        if not (0.0 < ask < 1.0):
            continue
        edge = p - ask - cost
        candidates.append((edge, side, p, ask))
    if not candidates:
        return None

    edge, side, p, ask = max(candidates)
    if edge < min_edge:
        return None
    return Signal(side=side, p_fair=p, ask=ask, edge=edge,
                  z_incr=z_last if z_last is not None else 0.0,
                  sigma=sigma, secs_left=secs_left)
