#!/bin/bash

PROJECTS=("b/b_short")
OBJECTS=("Account" "Lead" "Contact" "User" "Product" "Opportunity")
YEARS=("2024" "2025")
MONTHS=("1" "2""11")
DAYS=("1" "2" "27")

# Create base directories
for project in "${PROJECTS[@]}"; do
        for object in "${OBJECTS[@]}"; do
            for year in "${YEARS[@]}"; do
                for month in "${MONTHS[@]}"; do
                    for day in "${DAYS[@]}"; do
                        if [ "$day" = "3" ]; then
                            mkdir -p "s3/raw/$project/$object/year=$year/month=$month/day=$day"
                        else
                            mkdir -p "s3/raw/$project/$object/year=$year/month=$month/day=$day/appflow"
                        fi
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
                        if [ "$day" = "3" ]; then
                            continue
                        fi
                        for i in {1..5}; do
                            base64 /dev/urandom | head -c 100 > "s3/raw/$project/$object/year=$year/month=$month/day=$day/appflow/account_${i}.txt"
                        done
                    done
                done
            done
        done
done

echo "Test data generation complete!"
echo "Total size of generated files:"
du -sh a b
