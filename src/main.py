import argparse, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import Config


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("role", choices=["broker", "publisher", "subscriber"])
    ap.add_argument("--id", required=True)
    ap.add_argument("--config", required=True)
    a = ap.parse_args()
    cfg = Config(a.config)
    if a.role == "broker":
        from broker import Broker
        Broker(cfg, a.id).run()
    elif a.role == "publisher":
        from publisher import run_publisher
        run_publisher(cfg, a.id)
    else:
        from subscriber import run_subscriber
        run_subscriber(cfg, a.id)


if __name__ == "__main__":
    main()
