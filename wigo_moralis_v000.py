import asyncio
import aiohttp
from rich.console import Console
from rich.progress import Progress
import pandas as pd
import os
from datetime import datetime, timedelta
from decimal import Decimal

console = Console()


class WigoAnalyzer:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://deep-index.moralis.io/api/v2"
        self.headers = {
            "Accept": "application/json",
            "X-API-Key": api_key
        }

    def convert_to_token_amount(self, wei_value: str) -> float:
        """Convert wei string to token amount (divide by 10^18)"""
        try:
            # Convert string to Decimal for precise calculation
            wei = Decimal(wei_value)
            # Convert wei to token amount (divide by 10^18)
            token_amount = float(wei / Decimal('1000000000000000000'))
            return token_amount
        except (ValueError, TypeError):
            return 0.0

    async def get_token_data(self, token_address: str, days: int = 30):
        """Fetch token transfers and transactions"""
        async with aiohttp.ClientSession() as session:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            params = {
                "chain": "fantom",
                "from_date": start_date.strftime("%Y-%m-%d"),
                "to_date": end_date.strftime("%Y-%m-%d")
            }

            url = f"{self.base_url}/{token_address}"
            transfers = []

            with Progress() as progress:
                task = progress.add_task("[cyan]Fetching token data...", total=None)

                try:
                    while True:
                        async with session.get(url, headers=self.headers, params=params) as response:
                            if response.status != 200:
                                console.print(f"[red]Error: {response.status}[/red]")
                                console.print(await response.text())
                                return None

                            data = await response.json()
                            transfers.extend(data.get('result', []))

                            progress.update(task, advance=len(data.get('result', [])))

                            if not data.get('cursor'):
                                break

                            params['cursor'] = data['cursor']

                    return transfers

                except Exception as e:
                    console.print(f"[red]Error fetching data: {str(e)}[/red]")
                    return None

    def analyze_transfers(self, transfers: list):
        """Analyze the transfer data"""
        if not transfers:
            return None

        # Convert to DataFrame
        df = pd.DataFrame(transfers)

        # Convert value to numeric (token amount)
        console.print("[cyan]Converting token values...[/cyan]")
        df['value'] = df['value'].apply(self.convert_to_token_amount)

        # Convert timestamp to datetime
        df['datetime'] = pd.to_datetime(df['block_timestamp'])

        # Daily metrics
        console.print("[cyan]Calculating daily metrics...[/cyan]")
        daily_metrics = df.groupby(df['datetime'].dt.date).agg({
            'value': ['count', 'sum', 'mean', 'median'],
            'from_address': 'nunique',
            'to_address': 'nunique'
        }).round(4)

        daily_metrics.columns = [
            'num_transfers', 'total_volume', 'mean_transfer',
            'median_transfer', 'unique_senders', 'unique_receivers'
        ]

        # Top addresses analysis
        console.print("[cyan]Analyzing top addresses...[/cyan]")
        top_senders = df.groupby('from_address').agg({
            'value': ['count', 'sum'],
            'to_address': 'nunique'
        }).round(4)

        top_receivers = df.groupby('to_address').agg({
            'value': ['count', 'sum'],
            'from_address': 'nunique'
        }).round(4)

        return {
            'daily_metrics': daily_metrics,
            'top_senders': top_senders.sort_values(('value', 'sum'), ascending=False).head(10),
            'top_receivers': top_receivers.sort_values(('value', 'sum'), ascending=False).head(10),
            'raw_data': df
        }

    def save_results(self, results: dict, output_dir: str = "wigo_analysis"):
        """Save analysis results"""
        os.makedirs(output_dir, exist_ok=True)

        # Save daily metrics
        results['daily_metrics'].to_csv(f"{output_dir}/daily_metrics.csv")
        results['top_senders'].to_csv(f"{output_dir}/top_senders.csv")
        results['top_receivers'].to_csv(f"{output_dir}/top_receivers.csv")
        results['raw_data'].to_csv(f"{output_dir}/raw_data.csv", index=False)

        console.print(f"\n[green]Results saved to {output_dir} directory[/green]")

    def display_summary(self, results: dict):
        """Display analysis summary"""
        console.print("\n[bold blue]Analysis Summary[/bold blue]")
        console.print("=" * 50)

        daily = results['daily_metrics']
        console.print("\n[bold cyan]Daily Metrics (Average)[/bold cyan]")
        console.print(f"Daily Transfers: {daily['num_transfers'].mean():,.2f}")
        console.print(f"Daily Volume: {daily['total_volume'].mean():,.2f} WIGO")
        console.print(f"Mean Transfer Size: {daily['mean_transfer'].mean():,.2f} WIGO")
        console.print(f"Daily Active Senders: {daily['unique_senders'].mean():,.2f}")

        console.print("\n[bold cyan]Top Addresses by Volume[/bold cyan]")
        for addr in results['top_senders'].head().index:
            volume = results['top_senders'].loc[addr][('value', 'sum')]
            count = results['top_senders'].loc[addr][('value', 'count')]
            console.print(f"{addr[:10]}... : {volume:,.2f} WIGO ({count} transfers)")


async def main():
    analyzer = WigoAnalyzer(
        api_key="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6ImE0MmZkMDhmLWIyMTMtNDYzZi1iNWI3LWU3NTVmMmVmMmU2MiIsIm9yZ0lkIjoiNDE1NTM5IiwidXNlcklkIjoiNDI3MDU4IiwidHlwZUlkIjoiZTIxODM5ZWItZjYzOC00Nzc4LWE0ZDItMDljYmQ4YjQ0YWE2IiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3MzExOTQ2OTYsImV4cCI6NDg4Njk1NDY5Nn0.mOSRISYtKPFoVnlABmI8v97L4I5tas8T7stNQBpyMX0")

    console.print("[bold blue]Starting WIGO token analysis...[/bold blue]")

    # Fetch data
    transfers = await analyzer.get_token_data(
        token_address="0xE992bEAb6659BFF447893641A378FbbF031C5bD6",
        days=30
    )

    if not transfers:
        console.print("[red]No data retrieved. Exiting.[/red]")
        return

    # Analyze data
    console.print("\n[bold blue]Analyzing transfer data...[/bold blue]")
    results = analyzer.analyze_transfers(transfers)

    if not results:
        console.print("[red]Analysis failed. Exiting.[/red]")
        return

    # Save results
    analyzer.save_results(results)

    # Display summary
    analyzer.display_summary(results)

    return results


if __name__ == "__main__":
    try:
        results = asyncio.run(main())
    except Exception as e:
        console.print(f"[red]Error running analysis: {str(e)}[/red]")