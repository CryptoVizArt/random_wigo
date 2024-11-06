"""
wigo_rpc_v003_getblock.py
Version: 0.0.3
Description: WIGO token metrics collector using GetBlock RPC
Author: Claude & User
Date: 2024-11-06
"""

from web3 import Web3
from datetime import datetime, timedelta
import pandas as pd
import time
from collections import defaultdict
import os
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import json

class WigoMetricsCollector:
    VERSION = "003-getblock"
    TOKEN_ADDRESS = Web3.to_checksum_address('0xE992bEAb6659BFF447893641A378FbbF031C5bD6')
    TRANSFER_EVENT_SIGNATURE = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

    def __init__(self):
        """Initialize with GetBlock RPC"""
        self.rpc_url = "https://go.getblock.io/20590083eb8d48e392c976d463fa208f"

        # Initialize Web3 with proper headers for GetBlock
        self.w3 = Web3(Web3.HTTPProvider(
            self.rpc_url,
            request_kwargs={
                'headers': {
                    'x-api-key': '20590083eb8d48e392c976d463fa208f',
                    'Content-Type': 'application/json',
                }
            }
        ))

        self.validate_connection()
        self.setup_directories()
        self.setup_plotting()

    def setup_directories(self):
        """Setup directory structure for outputs"""
        self.output_dir = 'wigo_metrics'
        self.session_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.session_dir = os.path.join(self.output_dir, f'session_{self.session_timestamp}')
        os.makedirs(self.session_dir, exist_ok=True)
        self.csv_file = os.path.join(self.session_dir, f'wigo_metrics_v{self.VERSION}_live.csv')

    def setup_plotting(self):
        """Setup real-time plotting"""
        plt.ion()
        self.fig, self.axes = plt.subplots(2, 2, figsize=(15, 10))
        self.fig.suptitle('WIGO Token Metrics - Live Update (GetBlock RPC)', fontsize=16)
        plt.subplots_adjust(hspace=0.3)

    def validate_connection(self):
        """Validate connection to GetBlock RPC"""
        if not self.w3.is_connected():
            raise ConnectionError("Failed to connect to GetBlock RPC")
        print(f"Connected to Fantom network via GetBlock. Current block: {self.w3.eth.block_number:,}")

    def get_block_timestamp(self, block_number):
        """Get block timestamp with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                block = self.w3.eth.get_block(block_number)
                return datetime.fromtimestamp(block['timestamp'])
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                time.sleep(1)

    def find_block_by_timestamp(self, target_timestamp):
        """Binary search to find the closest block to a timestamp"""
        left = 1
        right = self.w3.eth.block_number

        while left <= right:
            mid = (left + right) // 2
            try:
                block = self.w3.eth.get_block(mid)
                if block['timestamp'] == target_timestamp:
                    return mid
                if block['timestamp'] < target_timestamp:
                    left = mid + 1
                else:
                    right = mid - 1
            except Exception as e:
                print(f"Error getting block {mid}: {e}")
                return None

        return left if left <= self.w3.eth.block_number else right

    def decode_transfer_event(self, event_log):
        """Decode a transfer event log"""
        try:
            from_address = Web3.to_checksum_address('0x' + event_log['topics'][1].hex()[-40:])
            to_address = Web3.to_checksum_address('0x' + event_log['topics'][2].hex()[-40:])
            value = int(event_log['data'].hex(), 16)

            return {
                'from_address': from_address,
                'to_address': to_address,
                'value': value / 1e18  # Convert from wei to token units
            }
        except Exception as e:
            print(f"Error decoding event: {e}")
            return None

    def update_plots(self):
        """Update all plots with current data"""
        try:
            if not os.path.exists(self.csv_file):
                return

            df = pd.read_csv(self.csv_file)
            if df.empty:
                return

            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')

            # Clear all axes
            for ax in self.axes.flat:
                ax.clear()

            # Plot 1: Transactions
            ax = self.axes[0, 0]
            ax.plot(df['date'], df['transactions'], 'b-')
            ax.set_title('Daily Transactions')
            ax.set_xlabel('Date')
            ax.grid(True)
            ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

            # Plot 2: Active Addresses
            ax = self.axes[0, 1]
            ax.plot(df['date'], df['active_addresses'], 'g-')
            ax.set_title('Daily Active Addresses')
            ax.set_xlabel('Date')
            ax.grid(True)
            ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

            # Plot 3: Volume
            ax = self.axes[1, 0]
            ax.plot(df['date'], df['volume'], 'r-')
            ax.set_title('Daily Volume (WIGO)')
            ax.set_xlabel('Date')
            ax.grid(True)
            ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

            # Plot 4: Unique Senders/Receivers
            ax = self.axes[1, 1]
            ax.plot(df['date'], df['unique_senders'], 'c-', label='Senders')
            ax.plot(df['date'], df['unique_receivers'], 'm-', label='Receivers')
            ax.set_title('Daily Unique Addresses')
            ax.set_xlabel('Date')
            ax.legend()
            ax.grid(True)
            ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

            plt.draw()
            plt.pause(0.1)

        except Exception as e:
            print(f"Error updating plots: {e}")

    def append_chunk_data(self, chunk_metrics):
        """Append chunk data to CSV file"""
        if not chunk_metrics:
            return

        results = []
        for date, metrics in chunk_metrics.items():
            results.append({
                'date': date,
                'transactions': metrics['transactions'],
                'active_addresses': len(metrics['active_addresses']),
                'volume': metrics['volume'],
                'unique_senders': len(metrics['senders']),
                'unique_receivers': len(metrics['receivers'])
            })

        df = pd.DataFrame(results)
        df.to_csv(self.csv_file, mode='a', header=not os.path.exists(self.csv_file), index=False)
        self.update_plots()

    def get_token_metrics(self, start_date, end_date, chunk_size=1000):  # Reduced chunk size for GetBlock
        """Collect token metrics using GetBlock RPC"""
        print(f"Finding blocks for date range: {start_date.date()} to {end_date.date()}")

        current_block = self.find_block_by_timestamp(int(start_date.timestamp()))
        end_block = self.find_block_by_timestamp(int(end_date.timestamp()))

        if not current_block or not end_block:
            print("Error finding blocks for date range")
            return None

        print(f"Analyzing blocks from {current_block:,} to {end_block:,}")

        # Save progress file
        with open(os.path.join(self.session_dir, 'progress.txt'), 'w') as f:
            f.write(f"start_block={current_block}\nend_block={end_block}\nlast_processed={current_block}")

        while current_block < end_block:
            chunk_end = min(current_block + chunk_size, end_block)

            try:
                print(f"Fetching events for blocks {current_block:,} to {chunk_end:,}...")

                chunk_metrics = defaultdict(lambda: {
                    'transactions': 0,
                    'active_addresses': set(),
                    'volume': 0,
                    'senders': set(),
                    'receivers': set()
                })

                event_filter = {
                    'address': self.TOKEN_ADDRESS,
                    'fromBlock': current_block,
                    'toBlock': chunk_end,
                    'topics': [self.TRANSFER_EVENT_SIGNATURE]
                }

                events = self.w3.eth.get_logs(event_filter)

                for event in events:
                    block_timestamp = self.get_block_timestamp(event['blockNumber'])
                    date_key = block_timestamp.date()

                    transfer_data = self.decode_transfer_event(event)
                    if transfer_data:
                        daily_metrics = chunk_metrics[date_key]
                        daily_metrics['transactions'] += 1
                        daily_metrics['active_addresses'].add(transfer_data['from_address'])
                        daily_metrics['active_addresses'].add(transfer_data['to_address'])
                        daily_metrics['volume'] += transfer_data['value']
                        daily_metrics['senders'].add(transfer_data['from_address'])
                        daily_metrics['receivers'].add(transfer_data['to_address'])

                print(f"Processed {len(events)} events")
                self.append_chunk_data(chunk_metrics)

                # Update progress
                with open(os.path.join(self.session_dir, 'progress.txt'), 'w') as f:
                    f.write(f"start_block={current_block}\nend_block={end_block}\nlast_processed={chunk_end}")

                current_block = chunk_end + 1
                time.sleep(0.5)  # Increased delay for GetBlock rate limits

            except Exception as e:
                print(f"Error fetching events: {e}")
                chunk_size = chunk_size // 2
                if chunk_size < 100:
                    print("Chunk size too small, stopping")
                    break
                time.sleep(2)  # Increased error delay
                continue

        return pd.read_csv(self.csv_file) if os.path.exists(self.csv_file) else None

def main():
    # Initialize collector
    collector = WigoMetricsCollector()

    # Set date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)

    # Collect metrics
    df = collector.get_token_metrics(start_date, end_date)

    if df is not None and not df.empty:
        print("\nSummary Statistics:")
        print(f"Total Transactions: {df['transactions'].sum():,}")
        print(f"Average Daily Active Addresses: {df['active_addresses'].mean():.2f}")
        print(f"Total Volume: {df['volume'].sum():,.2f} WIGO")
        print(f"Average Daily Senders: {df['unique_senders'].mean():.2f}")
        print(f"Average Daily Receivers: {df['unique_receivers'].mean():.2f}")

        plt.ioff()
        plt.show()
    else:
        print("No data was collected. Please check your GetBlock API key and try again.")

if __name__ == "__main__":
    main()