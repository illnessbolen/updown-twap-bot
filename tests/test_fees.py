"""Тесты fees.py. Таблица 100 акций скопирована из https://docs.polymarket.com/trading/fees.md (Crypto)."""
import math

import pytest

import fees
from fees import CRYPTO, FeeSchedule, UnsupportedFeeSchedule

# цена -> комиссия taker в USDC за 100 акций (как в документации, 2 знака)
DOC_TABLE_100_SHARES = {
    0.01: 0.07, 0.05: 0.33, 0.10: 0.63, 0.15: 0.89, 0.20: 1.12, 0.25: 1.31,
    0.30: 1.47, 0.35: 1.59, 0.40: 1.68, 0.45: 1.73, 0.50: 1.75, 0.55: 1.73,
    0.60: 1.68, 0.65: 1.59, 0.70: 1.47, 0.75: 1.31, 0.80: 1.12, 0.85: 0.89,
    0.90: 0.63, 0.95: 0.33, 0.99: 0.07,
}


@pytest.mark.parametrize("price,expected", sorted(DOC_TABLE_100_SHARES.items()))
def test_matches_doc_table(price, expected):
    assert round(fees.taker_fee(100, price), 2) == expected


def test_peak_at_half_is_1_75_usdc_per_100_shares():
    assert fees.taker_fee(100, 0.5) == pytest.approx(1.75)


def test_symmetric_around_half():
    for p in (0.05, 0.2, 0.3, 0.45):
        assert fees.taker_fee(100, p) == pytest.approx(fees.taker_fee(100, 1 - p))


def test_pct_of_notional():
    # 1.75 / 50 = 3.5% при p=0.50; 0.7% при p=0.90; 5.6% при p=0.20
    assert fees.fee_pct_of_notional(0.5) == pytest.approx(0.035)
    assert fees.fee_pct_of_notional(0.9) == pytest.approx(0.007)
    assert fees.fee_pct_of_notional(0.2) == pytest.approx(0.056)
    # согласованность с абсолютной комиссией
    assert fees.taker_fee(100, 0.2) == pytest.approx(0.056 * 100 * 0.2)


def test_fee_per_share_is_unrounded_and_consistent():
    assert fees.fee_per_share(0.5) == pytest.approx(0.0175)
    assert fees.fee_per_share(0.0) == 0.0 and fees.fee_per_share(1.0) == 0.0
    assert fees.taker_fee(1000, 0.37) == pytest.approx(1000 * fees.fee_per_share(0.37), abs=1e-5)


def test_rounding_to_five_decimals():
    # 7 акций по 0.33: 7 * 0.07 * 0.33 * 0.67 = 0.1083390 -> 0.10834
    assert fees.taker_fee(7, 0.33) == 0.10834


def test_tiny_trade_rounds_to_zero_or_min_fee():
    # 0.1 * 0.07 * 0.01 * 0.99 = 0.0000693 -> 0.00007
    assert fees.taker_fee(0.1, 0.01) == 0.00007
    # 0.01 акции по 0.01: 0.00000693 -> 0.00001 (минимальная комиссия)
    assert fees.taker_fee(0.01, 0.01) == 0.00001
    # меньше половины минимального шага -> 0
    assert fees.taker_fee(0.001, 0.01) == 0.0


def test_zero_at_extremes_and_zero_shares():
    assert fees.taker_fee(100, 0.0) == 0.0
    assert fees.taker_fee(100, 1.0) == 0.0
    assert fees.taker_fee(0, 0.5) == 0.0


def test_maker_pays_nothing():
    assert fees.maker_fee(100, 0.5) == 0.0
    assert fees.fee("maker", 100, 0.5) == 0.0
    assert fees.fee("taker", 100, 0.5) == fees.taker_fee(100, 0.5)


def test_buy_cost_and_sell_proceeds_include_fee():
    assert fees.buy_cost("taker", 100, 0.5) == pytest.approx(51.75)
    assert fees.buy_cost("maker", 100, 0.5) == pytest.approx(50.0)
    assert fees.sell_proceeds("taker", 100, 0.5) == pytest.approx(48.25)
    assert fees.sell_proceeds("maker", 100, 0.5) == pytest.approx(50.0)


def test_maker_rebate_estimate_is_20_percent_of_fee_equivalent():
    assert fees.maker_rebate_estimate(100, 0.5) == pytest.approx(0.2 * 1.75)
    assert fees.maker_rebate_estimate(100, 0.5, FeeSchedule(0.07, rebate_rate=0.0)) == 0.0


@pytest.mark.parametrize("shares,price", [(-1, 0.5), (10, -0.01), (10, 1.01)])
def test_invalid_inputs_raise(shares, price):
    with pytest.raises(ValueError):
        fees.taker_fee(shares, price)


def test_unknown_role_raises():
    with pytest.raises(ValueError):
        fees.fee("market_maker", 1, 0.5)


def test_unsupported_exponent_is_refused_not_guessed():
    with pytest.raises(UnsupportedFeeSchedule):
        FeeSchedule(rate=0.25, exponent=2)
    with pytest.raises(UnsupportedFeeSchedule):
        FeeSchedule(rate=0.07, taker_only=False)


def test_from_gamma_camel_case_as_returned_by_live_api():
    # feeSchedule из живого ответа Gamma для btc-updown-5m-* (2026-10-03)
    s = fees.from_gamma({"exponent": 1, "rate": 0.07, "takerOnly": True, "rebateRate": 0.2})
    assert s == CRYPTO


def test_from_gamma_snake_case_as_in_sdk():
    s = fees.from_gamma({"rate": 0.04, "exponent": 1, "taker_only": True, "rebate_rate": 0.25})
    assert s == FeeSchedule(0.04, 1, True, 0.25)
    # категория Finance/Politics: 100 акций по 0.5 -> $1.00 (таблица в документации)
    assert fees.taker_fee(100, 0.5, s) == pytest.approx(1.00)


def test_from_gamma_requires_rate():
    with pytest.raises(ValueError):
        fees.from_gamma({"exponent": 1})


def test_from_gamma_with_old_exponent_is_refused():
    with pytest.raises(UnsupportedFeeSchedule):
        fees.from_gamma({"rate": 0.25, "exponent": 2, "takerOnly": True, "rebateRate": 0.2})


def test_no_nan_inf():
    assert math.isfinite(fees.taker_fee(1e6, 0.5))
