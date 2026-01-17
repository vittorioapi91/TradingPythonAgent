"""
Main entry point for TradingPythonAgent
"""

# Load environment configuration early
import trading_agent.config  # noqa: F401

from trading_agent.agent import TradingAgent


def main():
    """Main function to run the trading agent"""
    agent = TradingAgent()
    agent.run()


if __name__ == "__main__":
    main()
