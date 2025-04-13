# National Stock Exchange of India (NSEI) MCP Server

A Model Context Protocol (MCP) server that provides access to NSEI trade data through a standardized interface. This server allows AI assistants like Claude to access financial data programmatically.

## Features

- **Get top Movers**: Access the top movers — gainers and losers for any given time period (presently limiting upto any 3 months interval)
- **Get latest trades**: Retrieve latest trades for further analysis of the day's movements

## Installation

### Prerequisites

- Python 3.8 or higher
- UV package manager (recommended) or pip
- Financial Modeling Prep API key

### Setup

1. Clone this repository

2. Install dependencies using UV (recommended):
   ```bash
   uv venv
   uv pip install -r requirements.txt
   ```

   Or using pip:
   ```bash
   pip install -r requirements.txt
   ```

## Running the Server

### Using UV (Recommended)

UV provides faster dependency resolution and installation. To run the server with UV:

```bash
# Activate the virtual environment
uv venv activate

# Run the server
python nsei_mcp_server.py
```

The server will start and listen for connections on the default MCP port.

### Using pip

```bash
# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Run the server
python fmp_mcp_server.py
```

## Connecting with Claude Desktop

Claude Desktop can connect to MCP servers to access financial data. Here's how to set it up:

1. Download Claude Desktop
2. If you're running the server from previous steps, you may shut it down since Claude Desktop will spawn a local server by itself.
2. Edit claude_desktop_config.json:
```
    	"fmp_mcp_server": {
            "command": "uv",
            "args": [
                "--directory",
                "REPLACE ME WITH ABSOLUTE DIRECTORY TO REPO",
                "run",
                "fmp_mcp_server.py"]
             }
 ```


Now Claude can use the FMP data through the MCP interface. You can ask Claude to:
- Get top movers for the day from NSE
- Retrieve trades for a particular stock — "how did RIL perform today?"
- And more!

## Example Queries for Claude

Once connected, you can ask Claude questions like:

- "How did SBI's stock perform today?"
- "Was RIL among the top movers today?"
- "Find me the biggest losers by percentage points in today's equity market trading"

## Caching

Will be implemented in future versions

## Logging

Logs are written to the `logs` directory with rotation enabled:
- Maximum log file size: 10MB
- Number of backup files: 5

## License

[MIT License](LICENSE)

## Acknowledgements

- [Shadi Copty's FMP MCP Server](https://github.com/shadi-fsai/fmp_mcp_server) for providing the API
- [MCP Server QuickStart guide](https://modelcontextprotocol.io/quickstart/server) for the Model Context Protocol quick start guides implementation
