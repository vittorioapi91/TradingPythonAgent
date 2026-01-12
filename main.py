"""
Main entry point for TradingPythonAgent
"""

# Load environment configuration early
import src.trading_agent.config  # noqa: F401

from src.trading_agent.agent import TradingAgent


def main():
    """Main function to run the trading agent"""
    agent = TradingAgent()
    agent.run()


if __name__ == "__main__":
    main()
