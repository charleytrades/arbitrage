"""Streamlit dashboard for the crypto price predictor.

Displays live predictions, model confidence, backtest results,
and historical accuracy. Runs on port 8502 to avoid conflict
with the arb bot dashboard on 8501.

Usage:
    streamlit run predictor_dashboard.py --server.port 8502
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd
import streamlit as st

# Must be first Streamlit call
st.set_page_config(
    page_title="Crypto Predictor",
    page_icon="🔮",
    layout="wide",
)


def load_predictor():
    """Load the predictor (cached)."""
    from predictor.model.predictor import LivePredictor
    return LivePredictor()


def load_data(symbol: str, interval: str):
    """Load kline data from disk."""
    from predictor.data.fetcher import load_klines
    return load_klines(symbol, interval)


def main():
    st.title("Crypto Price Predictor")
    st.caption("XGBoost walk-forward ensemble | BTC / ETH / SOL")

    from predictor.config import settings

    # Sidebar
    st.sidebar.header("Settings")
    selected_symbols = st.sidebar.multiselect(
        "Symbols",
        settings.symbol_list,
        default=settings.symbol_list,
    )
    auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=True)

    # Check if models exist
    model_dir = Path(settings.model_dir)
    if not model_dir.exists() or not list(model_dir.glob("*_meta.json")):
        st.error(
            "No trained models found. Run these commands first:\n\n"
            "```bash\n"
            "python -m predictor fetch\n"
            "python -m predictor train\n"
            "```"
        )
        return

    predictor = load_predictor()

    if not predictor.available_models:
        st.warning("No models loaded.")
        return

    # ── Header metrics ───────────────────────────────────────
    st.sidebar.markdown("---")
    st.sidebar.metric("Models loaded", len(predictor.available_models))
    st.sidebar.metric("Symbols", len(selected_symbols))
    st.sidebar.metric("Horizons", len(settings.horizon_list))

    # Show ensemble weights
    st.sidebar.markdown("**Ensemble weights:**")
    for h, w in settings.ensemble_weights.items():
        st.sidebar.text(f"  {h}: {w:.0%}")

    # ── Main predictions ─────────────────────────────────────
    for symbol in selected_symbols:
        st.markdown(f"### {symbol}")

        # Load data
        df_dict: dict[str, pd.DataFrame] = {}
        for tf in settings.timeframe_list:
            data = load_data(symbol, tf)
            if data is not None and len(data) > 0:
                df_dict[tf] = data.tail(500)

        if not df_dict:
            st.warning(f"No data for {symbol}. Run `python -m predictor fetch`.")
            continue

        base = settings.timeframe_list[0]
        ensemble = predictor.predict_ensemble(symbol, df_dict, base_interval=base)

        if ensemble is None:
            st.info(f"No prediction available for {symbol}")
            continue

        # Ensemble result
        col1, col2, col3 = st.columns(3)

        direction_color = "green" if ensemble.direction == "UP" else "red"
        col1.metric(
            "Direction",
            ensemble.direction,
            delta=f"{ensemble.weighted_prob_up:.1%} P(up)",
            delta_color="normal" if ensemble.direction == "UP" else "inverse",
        )
        col2.metric("Confidence", f"{ensemble.confidence:.1%}")
        col3.metric("Weighted P(up)", f"{ensemble.weighted_prob_up:.1%}")

        # Individual horizons table
        horizon_data = []
        for horizon in settings.horizon_list:
            pred = ensemble.predictions.get(horizon)
            if pred:
                weight = settings.ensemble_weights.get(horizon, 0)
                horizon_data.append({
                    "Horizon": horizon,
                    "P(up)": f"{pred.prob_up:.1%}",
                    "P(down)": f"{pred.prob_down:.1%}",
                    "Confidence": f"{pred.confidence:.1%}",
                    "Weight": f"{weight:.0%}",
                    "Model Age": f"{pred.model_age_hours:.0f}h",
                })

        if horizon_data:
            st.dataframe(
                pd.DataFrame(horizon_data),
                use_container_width=True,
                hide_index=True,
            )

        # Confidence bar
        conf = ensemble.confidence
        bar_html = f"""
        <div style="background: #333; border-radius: 4px; height: 24px; width: 100%; margin: 8px 0;">
            <div style="background: {'#22c55e' if ensemble.direction == 'UP' else '#ef4444'};
                        height: 100%; width: {conf*100:.0f}%; border-radius: 4px;
                        text-align: center; color: white; font-size: 12px; line-height: 24px;">
                {conf:.0%}
            </div>
        </div>
        """
        st.markdown(bar_html, unsafe_allow_html=True)

        # Recent price chart
        if base in df_dict:
            recent = df_dict[base].tail(100)
            st.line_chart(recent["close"], height=200)

        st.markdown("---")

    # ── Backtest results (if available) ──────────────────────
    report_dir = Path(settings.report_dir)
    if report_dir.exists():
        plots = list(report_dir.glob("backtest_*.png"))
        if plots:
            st.markdown("## Backtest Results")
            for plot_path in sorted(plots):
                st.image(str(plot_path), use_container_width=True)

    # ── Model info ───────────────────────────────────────────
    with st.expander("Model Details"):
        for (sym, horizon) in sorted(predictor.available_models):
            meta_path = model_dir / f"{sym}_{settings.timeframe_list[0]}_{horizon}_meta.json"
            if meta_path.exists():
                with open(meta_path) as f:
                    meta = json.load(f)
                st.text(f"{sym} {horizon}: {len(meta.get('features', []))} features, "
                        f"trained {meta.get('trained_at', 'unknown')}")

    # ── Data freshness ───────────────────────────────────────
    with st.expander("Data Freshness"):
        from predictor.data.fetcher import list_available
        available = list_available()
        for sym, intervals in sorted(available.items()):
            data = load_data(sym, intervals[0])
            if data is not None and len(data) > 0:
                last_ts = data.index[-1]
                age_min = (pd.Timestamp.now(tz="UTC") - last_ts).total_seconds() / 60
                freshness = "FRESH" if age_min < 10 else f"{age_min:.0f}m old"
                st.text(f"{sym}: {', '.join(intervals)} — last candle: {last_ts} ({freshness})")

    # Auto-refresh
    if auto_refresh:
        time.sleep(30)
        st.rerun()


if __name__ == "__main__":
    main()
