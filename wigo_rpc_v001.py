from web3 import Web3
from datetime import datetime, timedelta
import pandas as pd
import time
from collections import defaultdict
import json

# Connect to Fantom network
w3 = Web3(Web3.HTTPProvider('https://rpc.ftm.tools'))

# WIGO Token Contract - ensure proper checksum address
TOKEN_ADDRESS = Web3.to_checksum_address('0xE992bEAb6659BFF447893641A378FbbF031C5bD6')

# Transfer event signature
TRANSFER_EVENT_SIGNATURE = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'


def get_block_timestamp(block_number):
    block = w3.eth.get_block(block_number)
    return datetime.fromtimestamp(block['timestamp'])


def find_block_by_timestamp(target_timestamp):
    """Binary search to find the closest block to a timestamp"""
    left = 1
    right = w3.eth.block_number

    while left <= right:
        mid = (left + right) // 2
        try:
            block = w3.eth.get_block(mid)
            if block['timestamp'] == target_timestamp:
                return mid
            if block['timestamp'] < target_timestamp:
                left = mid + 1
            else:
                right = mid - 1
        except Exception as e:
            print(f"Error getting block {mid}: {e}")
            return None

    return left if left <= w3.eth.block_number else right


def decode_transfer_event(event_log):
    """Decode a transfer event log"""
    try:
        # Extract from and to addresses from indexed parameters (topics)
        from_address = Web3.to_checksum_address('0x' + event_log['topics'][1].hex()[-40:])
        to_address = Web3.to_checksum_address('0x' + event_log['topics'][2].hex()[-40:])

        # Decode the value from the data field
        value = int(event_log['data'].hex(), 16)

        return {
            'from_address': from_address,
            'to_address': to_address,
            'value': value / 1e18  # Convert from wei to token units
        }
    except Exception as e:
        print(f"Error decoding event: {e}")
        return None


def get_token_metrics(start_date, end_date):
    print("Finding start and end blocks...")

    # Convert dates to timestamps
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())

    # Find corresponding blocks
    start_block = find_block_by_timestamp(start_timestamp)
    end_block = find_block_by_timestamp(end_timestamp)

    if not start_block or not end_block:
        print("Error finding blocks for date range")
        return None

    print(f"Analyzing blocks from {start_block} to {end_block}")

    metrics_by_date = defaultdict(lambda: {
        'transactions': 0,
        'active_addresses': set(),
        'volume': 0,
        'senders': set(),
        'receivers': set()
    })

    # Fetch events in chunks to avoid timeout
    CHUNK_SIZE = 2000
    current_block = start_block

    while current_block < end_block:
        chunk_end = min(current_block + CHUNK_SIZE, end_block)

        try:
            print(f"Fetching events for blocks {current_block} to {chunk_end}...")

            # Get transfer events using eth_getLogs
            event_filter = {
                'address': TOKEN_ADDRESS,
                'fromBlock': current_block,
                'toBlock': chunk_end,
                'topics': [TRANSFER_EVENT_SIGNATURE]
            }

            events = w3.eth.get_logs(event_filter)

            for event in events:
                block_timestamp = get_block_timestamp(event['blockNumber'])
                date_key = block_timestamp.date()

                # Decode the event
                transfer_data = decode_transfer_event(event)
                if transfer_data:
                    # Update metrics
                    daily_metrics = metrics_by_date[date_key]
                    daily_metrics['transactions'] += 1
                    daily_metrics['active_addresses'].add(transfer_data['from_address'])
                    daily_metrics['active_addresses'].add(transfer_data['to_address'])
                    daily_metrics['volume'] += transfer_data['value']
                    daily_metrics['senders'].add(transfer_data['from_address'])
                    daily_metrics['receivers'].add(transfer_data['to_address'])

            print(f"Processed {len(events)} events")
            current_block = chunk_end + 1

            # Add delay to avoid rate limiting
            time.sleep(0.2)

        except Exception as e:
            print(f"Error fetching events: {e}")
            # Reduce chunk size on error
            CHUNK_SIZE = CHUNK_SIZE // 2
            if CHUNK_SIZE < 100:
                print("Chunk size too small, stopping")
                break
            time.sleep(1)
            continue

    # Convert to DataFrame
    results = []
    for date, metrics in metrics_by_date.items():
        results.append({
            'date': date,
            'transactions': metrics['transactions'],
            'active_addresses': len(metrics['active_addresses']),
            'volume': metrics['volume'],
            'unique_senders': len(metrics['senders']),
            'unique_receivers': len(metrics['receivers'])
        })

    if not results:
        print("No data collected")
        return None

    return pd.DataFrame(results)


def main():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)

    print(f"Fetching metrics from {start_date.date()} to {end_date.date()}")

    df = get_token_metrics(start_date, end_date)

    if df is not None and not df.empty:
        # Sort by date
        df = df.sort_values('date')

        # Save to CSV
        output_file = f'wigo_metrics_{start_date.date()}_{end_date.date()}.csv'
        df.to_csv(output_file, index=False)
        print(f"\nMetrics saved to {output_file}")

        # Print summary statistics
        print("\nSummary Statistics:")
        print(f"Total Transactions: {df['transactions'].sum():,}")
        print(f"Average Daily Active Addresses: {df['active_addresses'].mean():.2f}")
        print(f"Total Volume: {df['volume'].sum():,.2f} WIGO")
        print(f"Average Daily Senders: {df['unique_senders'].mean():.2f}")
        print(f"Average Daily Receivers: {df['unique_receivers'].mean():.2f}")
    else:
        print("No data was collected. Please try again with different parameters or check the RPC endpoint.")


if __name__ == "__main__":
    main()