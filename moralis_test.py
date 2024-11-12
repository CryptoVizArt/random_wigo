import aiohttp
import asyncio
from rich.console import Console

console = Console()


async def test_moralis():
    WIGO_ADDRESS = "0xE992bEAb6659BFF447893641A378FbbF031C5bD6"
    API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6ImE0MmZkMDhmLWIyMTMtNDYzZi1iNWI3LWU3NTVmMmVmMmU2MiIsIm9yZ0lkIjoiNDE1NTM5IiwidXNlcklkIjoiNDI3MDU4IiwidHlwZUlkIjoiZTIxODM5ZWItZjYzOC00Nzc4LWE0ZDItMDljYmQ4YjQ0YWE2IiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3MzExOTQ2OTYsImV4cCI6NDg4Njk1NDY5Nn0.mOSRISYtKPFoVnlABmI8v97L4I5tas8T7stNQBpyMX0"

    # Test different endpoint structures
    endpoints = [
        f"https://deep-index.moralis.io/api/v2/token/{WIGO_ADDRESS}",
        f"https://deep-index.moralis.io/api/v2/erc20/{WIGO_ADDRESS}",
        f"https://deep-index.moralis.io/api/v2/{WIGO_ADDRESS}"
    ]

    headers = {
        "Accept": "application/json",
        "X-API-Key": API_KEY
    }

    async with aiohttp.ClientSession() as session:
        for endpoint in endpoints:
            console.print(f"\n[yellow]Testing endpoint:[/yellow] {endpoint}")
            try:
                async with session.get(endpoint, headers=headers, params={"chain": "fantom"}) as response:
                    status = response.status
                    text = await response.text()
                    console.print(f"Status: {status}")
                    console.print(f"Response: {text[:200]}...")
            except Exception as e:
                console.print(f"[red]Error: {str(e)}[/red]")


if __name__ == "__main__":
    asyncio.run(test_moralis())