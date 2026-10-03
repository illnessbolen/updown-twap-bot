import math

import pytest

from updown.twap_signal import TwapState, effective_var_time, evaluate, fair_prob_up


def filled_state(n=40, start=100.0, step=0.0001, alternate=True) -> TwapState:
    st = TwapState(maxlen=120, min_prints=20)
    v = start
    for i in range(n):
        st.update(float(i), v)
        v *= math.exp(step if (i % 2 == 0 or not alternate) else -step)
    return st


def test_zscore_uses_only_previous_increments():
    st = filled_state(n=30)
    before = list(st.incr)
    last = st.prints[-1]
    z = st.update(last.ts + 1, last.value * math.exp(0.01))   # большой скачок
    # z считается по истории ДО добавления: скачок далеко за пределами ±0.0001
    m = sum(before) / len(before)
    sd = math.sqrt(sum((x - m) ** 2 for x in before) / len(before))
    assert z == pytest.approx((0.01 - m) / sd)
    assert z > 50


def test_duplicates_and_garbage_are_skipped():
    st = TwapState(min_prints=2)
    st.update(1.0, 100.0)
    assert st.update(1.0, 101.0) is None      # dt = 0
    assert st.update(0.5, 101.0) is None      # время назад
    assert st.update(2.0, -1.0) is None       # мусор
    assert len(st.prints) == 1


def test_sigma_needs_min_prints():
    st = filled_state(n=10)
    assert st.sigma_per_sqrt_sec() is None
    st = filled_state(n=40, step=0.0002)
    assert st.sigma_per_sqrt_sec() == pytest.approx(0.0002, rel=1e-6)   # |r| = 0.0002 за 1 с


def test_effective_var_time_is_continuous_at_window():
    w = 60.0
    assert effective_var_time(w, w) == pytest.approx(w / 3)
    assert effective_var_time(w - 1e-9, w) == pytest.approx(w / 3)
    assert effective_var_time(300, w) == pytest.approx(240 + 20)
    assert effective_var_time(0, w) == 0.0


def test_fair_prob_at_strike_and_symmetry():
    assert fair_prob_up(100.0, 100.0, 0.0002, 200, 60) == pytest.approx(0.5)
    up = fair_prob_up(100.1, 100.0, 0.0002, 200, 60)
    down = fair_prob_up(100.0 / 1.001, 100.0, 0.0002, 200, 60)
    assert up > 0.5 and up + down == pytest.approx(1.0)
    assert fair_prob_up(101.0, 100.0, 0.0002, 0, 60) == 1.0   # расчёт уже наступил


def test_evaluate_picks_down_when_below_strike():
    st = filled_state(n=40, step=0.0002)
    strike = st.last * 1.002
    sig = evaluate(st, strike, secs_left=180, window=60, ask_up=0.30, ask_down=0.55,
                   cost=0.0, min_edge=0.01)
    assert sig is not None and sig.side == "down"
    assert sig.edge == pytest.approx(sig.p_fair - 0.55)


def test_evaluate_respects_min_secs_left_and_min_edge():
    st = filled_state(n=40, step=0.0002)
    assert evaluate(st, st.last, secs_left=119, window=60, ask_up=0.1, ask_down=0.1) is None
    # на страйке p = 0.5, ask 0.49 + cost 0.02 => edge < 0
    assert evaluate(st, st.last, secs_left=200, window=60, ask_up=0.49, ask_down=0.49) is None
