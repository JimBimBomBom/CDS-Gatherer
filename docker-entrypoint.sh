#!/bin/bash
# docker-entrypoint.sh
# Entrypoint script for CDS-CityFetch Docker container
# Handles both one-shot and scheduled execution modes

set -e

# Function to run the fetch command
run_fetch() {
    local cmd="cds-cityfetch fetch"
    
    # Add required languages
    cmd="$cmd -l $LANGUAGES"
    
    # Add output directory
    cmd="$cmd --dir $OUTPUT_DIR"
    
    # Add output format
    cmd="$cmd -f $OUTPUT_FORMAT"
    
    # Add optional parameters
    if [ -n "$WEBHOOK_URL" ]; then
        cmd="$cmd --webhook-url $WEBHOOK_URL"
    fi
    
    if [ -n "$WEBHOOK_SECRET" ]; then
        cmd="$cmd --webhook-secret $WEBHOOK_SECRET"
    fi
    
    if [ "$VERBOSE" = "true" ]; then
        cmd="$cmd -v"
    fi
    
    # Add batch size, max pages, page size
    cmd="$cmd --batch-size $BATCH_SIZE"
    cmd="$cmd --max-pages $MAX_PAGES"
    cmd="$cmd --page-size $PAGE_SIZE"
    
    echo "Executing: $cmd"
    eval $cmd
}

# Function to parse SCHEDULE and convert to seconds
get_interval_seconds() {
    local schedule="$1"
    
    # Remove 'DAYS' suffix and extract number
    local days=$(echo "$schedule" | sed 's/DAYS$//i' | sed 's/DAY$//i')
    
    # Validate it's a number
    if ! echo "$days" | grep -qE '^[0-9]+$'; then
        echo "Invalid SCHEDULE format: $schedule" >&2
        echo "Use format: 0DAYS, 1DAY, 7DAYS, 30DAYS, etc." >&2
        exit 1
    fi
    
    # Convert days to seconds
    echo $((days * 86400))
}

# Main logic
# Check if arguments were passed (override default behavior)
if [ $# -gt 0 ]; then
    # Arguments provided - pass them directly to cds-cityfetch
    exec cds-cityfetch "$@"
fi

echo "=========================================="
echo "CDS-CityFetch Docker Container"
echo "=========================================="
echo "Languages: $LANGUAGES"
echo "Output: $OUTPUT_DIR"
echo "Format: $OUTPUT_FORMAT"
echo "Schedule: $SCHEDULE"

# Check if running in one-shot mode or scheduled mode
if [ "$SCHEDULE" = "0DAYS" ] || [ "$SCHEDULE" = "0DAY" ]; then
    echo "Mode: ONE-SHOT (SCHEDULE=0DAYS)"
    echo "=========================================="
    run_fetch
    echo "=========================================="
    echo "Fetch complete. Container will exit."
    echo "=========================================="
    exit 0
else
    # Scheduled mode - run in a loop
    INTERVAL=$(get_interval_seconds "$SCHEDULE")
    echo "Mode: SCHEDULED (every $SCHEDULE = $INTERVAL seconds)"
    echo "=========================================="
    
    # Run initial fetch
    echo "Running initial fetch..."
    run_fetch
    
    # Loop forever
    echo "Entering scheduled mode. Press Ctrl+C to stop."
    while true; do
        echo ""
        echo "Sleeping for $SCHEDULE ($INTERVAL seconds)..."
        echo "Next run at: $(date -d "@$(($(date +%s) + INTERVAL))" '+%Y-%m-%d %H:%M:%S UTC' 2>/dev/null || date -v+${SCHEDULE} '+%Y-%m-%d %H:%M:%S UTC' 2>/dev/null || echo 'see logs for time')"
        sleep $INTERVAL
        
        echo ""
        echo "=========================================="
        echo "Starting scheduled fetch at $(date '+%Y-%m-%d %H:%M:%S UTC')"
        echo "=========================================="
        run_fetch
    done
fi
