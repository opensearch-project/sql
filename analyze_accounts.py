#!/usr/bin/env python3
"""
Simple script to analyze the accounts.json test data structure
and understand the data for bin command testing.
"""

import json
import sys
from collections import defaultdict

def analyze_accounts_data():
    try:
        with open('/Users/ahkcs/IdeaProjects/sql/integ-test/src/test/resources/accounts.json', 'r') as f:
            accounts = []
            for line in f:
                line = line.strip()
                if line:
                    # Parse each line as JSON (NDJSON format)
                    if line.startswith('{"index"'):
                        continue  # Skip index metadata lines
                    accounts.append(json.loads(line))
        
        print("=== ACCOUNTS DATA ANALYSIS ===")
        print(f"Total records: {len(accounts)}")
        
        if not accounts:
            print("No account records found!")
            return
        
        # Sample records
        print(f"\nSample record structure:")
        print(json.dumps(accounts[0], indent=2))
        
        # Analyze balance field
        balances = [acc.get('balance', 0) for acc in accounts]
        print(f"\nBalance analysis:")
        print(f"  Min balance: {min(balances)}")
        print(f"  Max balance: {max(balances)}")
        print(f"  Range: {max(balances) - min(balances)}")
        
        # Analyze age field  
        ages = [acc.get('age', 0) for acc in accounts]
        print(f"\nAge analysis:")
        print(f"  Min age: {min(ages)}")
        print(f"  Max age: {max(ages)}")
        print(f"  Range: {max(ages) - min(ages)}")
        
        # Balance distribution for binning
        print(f"\nBalance distribution for bin testing:")
        balance_ranges = defaultdict(int)
        for balance in balances:
            # Test different span values
            bin_5000 = (balance // 5000) * 5000
            bin_10000 = (balance // 10000) * 10000
            balance_ranges[f"span_5000_{bin_5000}"] += 1
            balance_ranges[f"span_10000_{bin_10000}"] += 1
        
        print("  Top balance bins (span=5000):")
        span_5000_counts = [(k.split('_')[2], v) for k, v in balance_ranges.items() if k.startswith('span_5000')]
        span_5000_counts.sort(key=lambda x: int(x[0]))
        for bin_val, count in span_5000_counts[:10]:
            print(f"    {bin_val}: {count} records")
        
        print("  Top balance bins (span=10000):")
        span_10000_counts = [(k.split('_')[2], v) for k, v in balance_ranges.items() if k.startswith('span_10000')]
        span_10000_counts.sort(key=lambda x: int(x[0]))
        for bin_val, count in span_10000_counts[:10]:
            print(f"    {bin_val}: {count} records")
        
        # Age distribution
        print(f"\nAge distribution for bin testing:")
        age_ranges = defaultdict(int)
        for age in ages:
            bin_5 = (age // 5) * 5
            age_ranges[bin_5] += 1
        
        print("  Age bins (span=5):")
        for bin_val in sorted(age_ranges.keys()):
            print(f"    {bin_val}: {age_ranges[bin_val]} records")
        
        # Test manual bin calculations for verification
        print(f"\nManual bin calculation examples:")
        test_balances = balances[:5]
        for balance in test_balances:
            bin_5000 = (balance // 5000) * 5000
            bin_10000 = (balance // 10000) * 10000
            print(f"  Balance {balance} -> span=5000: {bin_5000}, span=10000: {bin_10000}")
        
    except FileNotFoundError:
        print("Error: accounts.json file not found!")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    analyze_accounts_data()