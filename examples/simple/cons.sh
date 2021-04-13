#!/bin/sh
echo "AINTQ CONSUMER"
echo "-------------"
echo "In another terminal, run 'python main.py'"
echo "Stop the consumer using Ctrl+C"
python ../../aintq/bin/aintq_consumer.py main.aintq
