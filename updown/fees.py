"""
Комиссии Polymarket (docs/API_NOTES.md, §4).

Taker:  fee (USD) = C × rate × p × (1 − p)
        C - число акций, p - цена акции. Для Crypto rate = 0.07 (feeSchedule рынка).
        Округление до 5 знаков; меньше 0.00001 - ноль.
        https://docs.polymarket.com/trading/fees.md
Maker:  комиссии нет ("Makers are never charged fees").
Rebate: пул = rebateRate × taker-комиссии рынка, делится между мейкерами пропорционально
        fee_equivalent = C × rate × p × (1 − p). Заранее по сделке не вычисляется,
        в PnL считаем 0; maker_rebate_upper_bound() - только оценка сверху.
        https://docs.polymarket.com/programs/maker-rebates.md

Все суммы в Decimal: float даёт ошибки на границах округления.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Mapping, Union

Number = Union[Decimal, float, int, str]

FEE_QUANT = Decimal("0.00001")
_ONE = Decimal(1)


class UnsupportedFeeSchedule(ValueError):
    """Параметры комиссии, формула для которых не подтверждена документацией."""


def _d(x: Number) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


@dataclass(frozen=True)
class FeeSchedule:
    rate: Decimal
    exponent: Decimal = _ONE
    taker_only: bool = True
    rebate_rate: Decimal = Decimal(0)
    enabled: bool = True

    @classmethod
    def from_gamma_market(cls, market: Mapping[str, Any]) -> "FeeSchedule":
        """Из рынка Gamma API: feesEnabled + feeSchedule{rate, exponent, takerOnly, rebateRate}."""
        if not market.get("feesEnabled"):
            return NO_FEES
        fs = market.get("feeSchedule")
        if not isinstance(fs, Mapping) or "rate" not in fs:
            raise UnsupportedFeeSchedule("feesEnabled = true, но нет feeSchedule.rate")
        return cls(
            rate=_d(fs["rate"]),
            exponent=_d(fs.get("exponent", 1)),
            taker_only=bool(fs.get("takerOnly", True)),
            rebate_rate=_d(fs.get("rebateRate", 0)),
            enabled=True,
        )

    def _check(self) -> None:
        # TODO(verify): как exponent входит в формулу при значении != 1, в документации нет.
        if self.exponent != _ONE:
            raise UnsupportedFeeSchedule(f"feeSchedule.exponent = {self.exponent}, поддерживается только 1")
        if not self.taker_only:
            raise UnsupportedFeeSchedule("feeSchedule.takerOnly = false: формула для мейкеров не описана")


NO_FEES = FeeSchedule(rate=Decimal(0), enabled=False)
# Значения для 5m/15m крипторынков на 2026-10-03 (Gamma: feeType "crypto_fees_v2").
CRYPTO = FeeSchedule(rate=Decimal("0.07"), exponent=_ONE, taker_only=True, rebate_rate=Decimal("0.2"))


def _check_price(p: Decimal) -> None:
    if not Decimal(0) < p < _ONE:
        raise ValueError(f"цена должна быть в (0, 1), получено {p}")


def fee_per_share(price: Number, schedule: FeeSchedule = CRYPTO) -> Decimal:
    """Taker-комиссия на одну акцию без округления (для расчёта edge)."""
    p = _d(price)
    _check_price(p)
    if not schedule.enabled or schedule.rate == 0:
        return Decimal(0)
    schedule._check()
    return schedule.rate * p * (_ONE - p)


def taker_fee(shares: Number, price: Number, schedule: FeeSchedule = CRYPTO) -> Decimal:
    """
    Taker-комиссия сделки в USD, округлённая до 0.00001.
    TODO(verify): режим округления в документации не указан; ROUND_HALF_UP согласуется
    с "Anything smaller rounds to zero" и не занижает комиссию на половине шага.
    """
    c = _d(shares)
    if c < 0:
        raise ValueError(f"число акций должно быть >= 0, получено {c}")
    raw = c * fee_per_share(price, schedule)
    return raw.quantize(FEE_QUANT, rounding=ROUND_HALF_UP)


def maker_fee(shares: Number, price: Number, schedule: FeeSchedule = CRYPTO) -> Decimal:
    """Мейкер комиссию не платит (при takerOnly = true)."""
    _check_price(_d(price))
    if schedule.enabled and schedule.rate != 0:
        schedule._check()
    return Decimal(0)


def taker_buy_cost_per_share(price: Number, schedule: FeeSchedule = CRYPTO) -> Decimal:
    """Полная цена покупки одной акции по ask с комиссией: p + rate·p·(1−p)."""
    return _d(price) + fee_per_share(price, schedule)


def taker_sell_proceeds_per_share(price: Number, schedule: FeeSchedule = CRYPTO) -> Decimal:
    """Выручка с продажи одной акции по bid за вычетом комиссии: p − rate·p·(1−p)."""
    return _d(price) - fee_per_share(price, schedule)


def maker_rebate_upper_bound(shares: Number, price: Number, schedule: FeeSchedule = CRYPTO) -> Decimal:
    """
    Верхняя граница ребейта за исполненный мейкерский ордер: rebateRate × fee_equivalent.
    Достигается, только если мы единственный мейкер рынка за день. В PnL не включается.
    """
    if not schedule.enabled:
        return Decimal(0)
    return schedule.rebate_rate * _d(shares) * fee_per_share(price, schedule)
