import asyncio
from src.mcp_server import processor

if __name__ == "__main__":
    start = "2025-12-10"
    end = "2025-12-30"

    print(f"Processing emails from {start} to {end}...")
    result = asyncio.run(
        processor.process_emails(start, end, [], None)
    )
    print(result)
