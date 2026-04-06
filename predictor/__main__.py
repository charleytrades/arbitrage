"""CLI dispatcher: python -m predictor {fetch|train|predict|backtest}."""

import sys


def main() -> None:
    commands = {
        "fetch": "predictor.fetch",
        "train": "predictor.train",
        "predict": "predictor.predict",
        "backtest": "predictor.backtest_cli",
    }

    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print("Usage: python -m predictor {fetch|train|predict|backtest}")
        print()
        print("Commands:")
        print("  fetch     Download historical klines from Binance")
        print("  train     Train XGBoost models (walk-forward)")
        print("  predict   Run live predictions in a loop")
        print("  backtest  Evaluate model accuracy on historical data")
        sys.exit(1)

    cmd = sys.argv[1]
    # Remove the subcommand so argparse in submodules works
    sys.argv = [sys.argv[0]] + sys.argv[2:]

    if cmd == "fetch":
        from predictor.fetch import main as run
    elif cmd == "train":
        from predictor.train import main as run
    elif cmd == "predict":
        from predictor.predict import main as run
    elif cmd == "backtest":
        from predictor.backtest_cli import main as run

    run()


if __name__ == "__main__":
    main()
