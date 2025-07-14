#!/usr/bin/env python3
"""
Manual test script to understand bin command behavior
by analyzing the expected vs actual results.
"""

import json

def main():
    print("=== BIN COMMAND BEHAVIOR ANALYSIS ===")
    
    # Based on our data analysis, let's understand what the bin command should produce
    
    print("\n1. SAMPLE DATA FROM ANALYSIS:")
    sample_data = [
        {"account_number": 1, "balance": 39225, "age": 32},
        {"account_number": 6, "balance": 5686, "age": 36}, 
        {"account_number": 13, "balance": 32838, "age": 28},
        {"account_number": 18, "balance": 4180, "age": 33},
        {"account_number": 20, "balance": 16418, "age": 36}
    ]
    
    for record in sample_data:
        print(f"  Account {record['account_number']}: balance={record['balance']}, age={record['age']}")
    
    print("\n2. EXPECTED BIN RESULTS FOR 'bin balance span=5000':")
    for record in sample_data:
        balance = record['balance']
        bin_result = (balance // 5000) * 5000
        print(f"  Account {record['account_number']}: {balance} -> {bin_result}")
    
    print("\n3. EXPECTED BIN RESULTS FOR 'bin age span=5':")
    for record in sample_data:
        age = record['age']
        bin_result = (age // 5) * 5
        print(f"  Account {record['account_number']}: {age} -> {bin_result}")
    
    print("\n4. BINS PARAMETER ANALYSIS:")
    print("For 'bin age bins=5' with age range 20-40:")
    min_age, max_age = 20, 40
    age_range = max_age - min_age  # 20
    bin_width = age_range / 5  # 4
    print(f"  Age range: {min_age}-{max_age} ({age_range})")
    print(f"  Bin width: {bin_width}")
    print("  Expected bin boundaries:")
    for i in range(5):
        bin_start = min_age + (i * bin_width)
        bin_end = min_age + ((i + 1) * bin_width)
        print(f"    Bin {i}: {bin_start}-{bin_end}")
    
    print("\n5. HISTOGRAM EXPECTED RESULTS:")
    print("For 'bin balance span=10000 | stats count() by balance_bin':")
    
    # Based on our analysis
    expected_histogram = {
        0: 168,     # 0-9999
        10000: 213, # 10000-19999  
        20000: 217, # 20000-29999
        30000: 187, # 30000-39999
        40000: 215  # 40000-49999
    }
    
    for bin_val, count in sorted(expected_histogram.items()):
        print(f"  {bin_val}: {count} records")
    
    print("\n6. SCHEMA EXPECTATIONS:")
    print("Based on previous fixes:")
    print("  - account_number: bigint (not int)")
    print("  - balance: bigint")
    print("  - age: bigint (not int)")
    print("  - balance_bin: bigint (same type as balance)")
    print("  - age_bin: bigint (same type as age)")

if __name__ == "__main__":
    main()