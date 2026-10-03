"""
Комиссии Polymarket на крипто-рынках Up/Down (5m, 15m).

Формула (https://docs.polymarket.com/trading/fees.md, сверено 2026-10-03,
см. docs/API_NOTES.md, раздел 5):

    fee = C * rate * p * (1 - p)      # в USDC, платит только taker

C - число акций, p - цена акции (0..1). Для крипто rate = 0.07, maker не платит.
Округление: до 5 знаков, меньше 0.00001 превращается в 0.

Maker-rebate - не скидка на сделку, а дневная выплата из пула (rebate_rate от
собранных taker-комиссий, делится по рынку пропорционально fee-equivalent
исполненных мейкерских ордеров, минимум $1 в день). На одну сделку его точно
не посчитать, поэтому в основных метриках он равен 0, а maker_rebate_estimate()
даёт только отдельную оценку для отчёта.

Зависимостей нет, только стандартная библиотека.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Mapping

_QUANT = Decimal("0.00001")


class UnsupportedFeeSchedule(ValueError):
    """Параметры комиссии, для которых формула в документации не приведена."""


@dataclass(frozen=True)
class FeeSchedule:
    rate: float
    exponent: float = 1
    taker_only: bool = True
    rebate_rate: float = 0.0

    def __post_init__(self) -> None:
        if self.rate < 0:
            raise ValueError(f"rate < 0: {self.rate}")
        if not 0.0 <= self.rebate_rate <= 1.0:
            raise ValueError(f"rebate_rate вне [0, 1]: {self.rebate_rate}")
        # TODO(verify): формула для exponent != 1 в документации не приведена.
        # Старая схема (rate=0.25, exponent=2, пик 1.56%) считалась иначе, чем
        # текущая C*rate*p*(1-p), поэтому не угадываем, а падаем.
        if self.exponent != 1:
            raise UnsupportedFeeSchedule(
                f"exponent={self.exponent}: формула неизвестна (см. API_NOTES.md, раздел 9)")
        if not self.taker_only:
            raise UnsupportedFeeSchedule(
                "taker_only=False: комиссия мейкера в документации не описана")


# feeSchedule крипто-рынков, проверено вживую в Gamma: rate 0.07, exponent 1,
# takerOnly true, rebateRate 0.2
CRYPTO = FeeSchedule(rate=0.07, exponent=1, taker_only=True, rebate_rate=0.20)


def from_gamma(fee_schedule: Mapping[str, object]) -> FeeSchedule:
    """feeSchedule рынка из Gamma (camelCase) или из SDK (snake_case)."""
    def pick(camel: str, snake: str, default=None):
        if camel in fee_schedule:
            return fee_schedule[camel]
        return fee_schedule.get(snake, default)

    rate = pick("rate", "rate")
    if rate is None:
        raise ValueError("в feeSchedule нет поля rate")
    return FeeSchedule(
        rate=float(rate),
        exponent=pick("exponent", "exponent", 1),
        taker_only=bool(pick("takerOnly", "taker_only", True)),
        rebate_rate=float(pick("rebateRate", "rebate_rate", 0.0)),
    )


def _check(shares: float, price: float) -> None:
    if shares < 0:
        raise ValueError(f"shares < 0: {shares}")
    if not 0.0 <= price <= 1.0:
        raise ValueError(f"цена вне [0, 1]: {price}")


def _fee_exact(shares: float, price: float, schedule: FeeSchedule) -> Decimal:
    _check(shares, price)
    c, p, r = Decimal(str(shares)), Decimal(str(price)), Decimal(str(schedule.rate))
    return c * r * p * (1 - p)


def taker_fee(shares: float, price: float, schedule: FeeSchedule = CRYPTO) -> float:
    """Комиссия taker в USDC, округлённая до 5 знаков.

    TODO(verify): режим округления в документации не указан, берём half-up.
    """
    return float(_fee_exact(shares, price, schedule).quantize(_QUANT, ROUND_HALF_UP))


def maker_fee(shares: float, price: float, schedule: FeeSchedule = CRYPTO) -> float:
    """Мейкер комиссию не платит (takerOnly)."""
    _check(shares, price)
    return 0.0


def fee(role: str, shares: float, price: float, schedule: FeeSchedule = CRYPTO) -> float:
    if role == "taker":
        return taker_fee(shares, price, schedule)
    if role == "maker":
        return maker_fee(shares, price, schedule)
    raise ValueError(f"role должен быть 'taker' или 'maker', получено {role!r}")


def fee_per_share(price: float, schedule: FeeSchedule = CRYPTO) -> float:
    """Комиссия taker на одну акцию без округления (долей цены контракта).

    Это то, что вычитается из edge: например, 0.0175 при p = 0.50.
    """
    _check(0.0, price)
    return schedule.rate * price * (1.0 - price)


def fee_pct_of_notional(price: float, schedule: FeeSchedule = CRYPTO) -> float:
    """Комиссия taker в долях суммы сделки: rate * (1 - p). 0.035 при p = 0.50."""
    _check(0.0, price)
    return schedule.rate * (1.0 - price)


def maker_rebate_estimate(shares: float, price: float,
                          schedule: FeeSchedule = CRYPTO) -> float:
    """
    Оценка дневного rebate за исполненный мейкерский ордер (USDC), без округления.

    Пул рынка = rebate_rate * сумма taker-комиссий, а доля мейкера = его
    fee-equivalent / сумма fee-equivalent по рынку. В среднем это даёт
    rebate_rate * fee-equivalent. Реальная выплата дневная, минимум $1 и зависит
    от конкурентов, так что цифра только для отдельной строки отчёта.
    """
    return float(_fee_exact(shares, price, schedule)) * schedule.rebate_rate


def buy_cost(role: str, shares: float, price: float,
             schedule: FeeSchedule = CRYPTO) -> float:
    """Полная стоимость покупки: акции * цена + комиссия."""
    _check(shares, price)
    return shares * price + fee(role, shares, price, schedule)


def sell_proceeds(role: str, shares: float, price: float,
                  schedule: FeeSchedule = CRYPTO) -> float:
    """Выручка от продажи за вычетом комиссии."""
    _check(shares, price)
    return shares * price - fee(role, shares, price, schedule)
