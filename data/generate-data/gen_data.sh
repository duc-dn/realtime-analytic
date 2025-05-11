#!/bin/bash

set -e

echo "1. Insert categories..."
python3 category_generator.py

echo "2. Insert brands..."
python3 brand_generator.py

echo "3. Insert users..."
python3 user_generator.py

echo "4. Insert products..."
python3 product_generator.py

echo "5. Insert order_status..."
python3 order_status_generator.py

echo "All data generated successfully."