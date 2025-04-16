#!/bin/bash

PROJECTS=("a/a_short" "b/b_short")
OBJECTS=("Account" "Case")
YEARS=("2024" "2025")
MONTHS=("01" "02" "11")
DAYS=("01" "02" "30")

# Create base directories
for project in "${PROJECTS[@]}"; do
        for object in "${OBJECTS[@]}"; do
            for year in "${YEARS[@]}"; do
                for month in "${MONTHS[@]}"; do
                    for day in "${DAYS[@]}"; do
                        mkdir -p "s3/raw/$project/$object/year=$year/month=$month/day=$day/appflow"
                    done
                done
            done
        done
done

# Function to generate random data file

for project in "${PROJECTS[@]}"; do
        for object in "${OBJECTS[@]}"; do
            for year in "${YEARS[@]}"; do
                for month in "${MONTHS[@]}"; do
                    for day in "${DAYS[@]}"; do
                        for i in {1..25}; do
                            base64 /dev/urandom | head -c 209715 > "s3/raw/$project/$object/year=$year/month=$month/day=$day/appflow/account_${i}.txt"
                        done
                    done
                done
            done
        done
done

echo "Test data generation complete!"
echo "Total size of generated files:"
du -sh a b
