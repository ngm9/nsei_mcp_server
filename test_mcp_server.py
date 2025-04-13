from mcp.server.fastmcp import FastMCP
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger("test_mcp")

# Create MCP server
mcp = FastMCP("test")

@mcp.tool()
async def hello(name: str) -> str:
    """Say hello to someone."""
    logger.info(f"Saying hello to {name}")
    return f"Hello, {name}!"

if __name__ == "__main__":
    logger.info("Starting test MCP server")
    # Don't redirect stdout to stderr here
    mcp.run(transport='stdio')
