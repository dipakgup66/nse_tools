"""
NSE Trading Engine — Main Entry Point
========================================
Starts the unified Orchestrator API server.

Usage:
    python main.py                    # Start API server on port 7778
    python main.py --port 8080        # Custom port
    python main.py --demo             # Force demo mode
"""

import argparse
from agents.orchestrator import Orchestrator
from core.config import cfg


def main():
    ap = argparse.ArgumentParser(description="NSE Trading Engine — Unified API")
    ap.add_argument("--port", type=int, default=cfg.engine_port,
                    help=f"HTTP server port (default {cfg.engine_port})")
    args = ap.parse_args()

    orch = Orchestrator()
    orch.serve(port=args.port)


if __name__ == "__main__":
    main()
