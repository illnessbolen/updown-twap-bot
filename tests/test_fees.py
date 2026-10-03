from decimal import Decimal

import pytest

from updown.fees import (
    CRYPTO, NO_FEES, FeeSchedule, UnsupportedFeeSchedule, fee_per_share, maker_fee,
    maker_rebate_upper_bound, taker_buy_cost_per_share, taker_fee, taker_sell_proceeds_per_share,
)

# https://docs.polymarket.com/trading/fees.md, вкладка Crypto, 100 акций.
# В таблице суммы округлены до центов.
DOC_TABLE_CRYPTO_100 = [
    ("0.01", "0.07"), ("0.05", "0.33"), ("0.10", "0.63"), ("0.15", "0.89"), ("0.20", "1.12"),
    ("0.25", "1.31"), ("0.30", "1.47"), ("0.35", "1.59"), ("0.40", "1.68"), ("0.45", "1.73"),
    ("0.50", "1.75"), ("0.55", "1.73"), ("0.60", "1.68"), ("0.65", "1.59"), ("0.70", "1.47"),
    ("0.75", "1.31"), ("0.80", "1.12"), ("0.85", "0.89"), ("0.90", "0.63"), ("0.95", "0.33"),
    ("0.99", "0.07"),
]


@pytest.mark.parametrize("price,expected", DOC_TABLE_CRYPTO_100)
def test_crypto_table_from_docs(price, expected):
    fee = taker_fee(100, price, CRYPTO)
    assert abs(fee - Decimal(expected)) <= Decimal("0.005"), (price, fee)


@pytest.mark.parametrize("price,expected", [("0.50", "1.75"), ("0.15", "0.8925"), ("0.05", "0.3325")])
def test_exact_values(price, expected):
    # 100 × 0.07 × p × (1 − p) без округления до центов
    assert taker_fee(100, price) == Decimal(expected)


def test_peak_at_half_is_3_5_percent_of_notional():
    assert fee_per_share("0.5") == Decimal("0.0175")
    assert taker_fee(100, "0.5") / Decimal(50) == Decimal("0.035")


@pytest.mark.parametrize("p", ["0.01", "0.13", "0.3", "0.42", "0.5"])
def test_symmetric_around_half(p):
    q = Decimal(1) - Decimal(p)
    assert taker_fee(37, p) == taker_fee(37, q)


def test_rounded_to_five_decimals():
    fee = taker_fee("3.3", "0.37")          # 3.3 × 0.07 × 0.37 × 0.63 = 0.05384...
    assert fee == Decimal("0.05385")
    assert fee.as_tuple().exponent == -5


def test_tiny_fee_rounds_to_zero():
    # "Anything smaller rounds to zero": 0.0001 × 0.07 × 0.01 × 0.99 ≈ 6.9e-8
    assert taker_fee("0.0001", "0.01") == Decimal(0)


def test_smallest_charged_fee():
    # 0.0145 × 0.07 × 0.01 × 0.99 = 0.0000100485 -> 0.00001
    assert taker_fee("0.0145", "0.01") == Decimal("0.00001")


def test_maker_pays_nothing():
    assert maker_fee(1000, "0.5") == 0


def test_fees_disabled():
    assert taker_fee(100, "0.5", NO_FEES) == 0
    assert fee_per_share("0.5", NO_FEES) == 0
    assert maker_rebate_upper_bound(100, "0.5", NO_FEES) == 0


def test_unsupported_exponent_raises():
    sched = FeeSchedule(rate=Decimal("0.07"), exponent=Decimal(2))
    with pytest.raises(UnsupportedFeeSchedule):
        taker_fee(100, "0.5", sched)


def test_maker_fees_schedule_raises():
    sched = FeeSchedule(rate=Decimal("0.07"), taker_only=False)
    with pytest.raises(UnsupportedFeeSchedule):
        taker_fee(100, "0.5", sched)


@pytest.mark.parametrize("p", ["0", "1", "-0.1", "1.2"])
def test_price_out_of_range(p):
    with pytest.raises(ValueError):
        taker_fee(10, p)


def test_negative_shares():
    with pytest.raises(ValueError):
        taker_fee(-1, "0.5")


def test_buy_cost_and_sell_proceeds():
    assert taker_buy_cost_per_share("0.5") == Decimal("0.5175")
    assert taker_sell_proceeds_per_share("0.5") == Decimal("0.4825")
    assert taker_buy_cost_per_share("0.9") == Decimal("0.9063")


def test_rebate_upper_bound_is_20_percent_of_fee_equivalent():
    assert maker_rebate_upper_bound(100, "0.5") == Decimal("0.35")


def test_from_gamma_market_live_sample():
    # Фрагмент живого ответа Gamma для btc-updown-5m-1791017700 (2026-10-03)
    market = {"feesEnabled": True, "feeType": "crypto_fees_v2",
              "feeSchedule": {"exponent": 1, "rate": 0.07, "takerOnly": True, "rebateRate": 0.2}}
    s = FeeSchedule.from_gamma_market(market)
    assert s == CRYPTO
    assert taker_fee(100, "0.5", s) == Decimal("1.75")


def test_from_gamma_market_fees_disabled():
    assert FeeSchedule.from_gamma_market({"feesEnabled": False}) == NO_FEES


def test_from_gamma_market_missing_schedule():
    with pytest.raises(UnsupportedFeeSchedule):
        FeeSchedule.from_gamma_market({"feesEnabled": True})
