#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Kill any existing Python processes
pkill -f "python consumer/consumer.py"
pkill -f "python event_generator.py"
pkill -f "python transform_events.py"

echo "Starting pipeline components..."

# Start consumer first to ensure no events are missed
python consumer/consumer.py &
CONSUMER_PID=$!
echo "Started consumer (PID: $CONSUMER_PID)"

# Wait a moment to ensure consumer is ready
sleep 2

# Start event generator
python event_generator.py &
GENERATOR_PID=$!
echo "Started event generator (PID: $GENERATOR_PID)"

# Start transformer
python transform_events.py &
TRANSFORMER_PID=$!
echo "Started transformer (PID: $TRANSFORMER_PID)"

echo "All components started. Press Ctrl+C to stop all processes."

# Wait for Ctrl+C
trap "kill $CONSUMER_PID $GENERATOR_PID $TRANSFORMER_PID; exit" SIGINT
wait
