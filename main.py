"""
Main entry point for TradingPythonAgent
"""

from src.trading_agent.agent import TradingAgent


def main():
    """Main function to run the trading agent"""
    agent = TradingAgent()
    agent.run()


if __name__ == "__main__":
    main()
