# Delete consumer group to reset offset to beginning
# This allows the sink connector to replay all events from Kafka topic

$KAFKA_BOOTSTRAP = "72.61.233.209:9092"
$CONSUMER_GROUP = "connect-sink-pipeline-1-mssql-dbo"
$CONNECTOR_URL = "http://72.61.233.209:8083"
$SINK_CONNECTOR = "sink-pipeline-1-mssql-dbo"

Write-Host "Step 1: Stopping sink connector..." -ForegroundColor Cyan
try {
    Invoke-RestMethod -Uri "$CONNECTOR_URL/connectors/$SINK_CONNECTOR/pause" -Method Put
    Write-Host "Connector paused" -ForegroundColor Green
    Start-Sleep -Seconds 3
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
}

Write-Host "`nStep 2: Deleting consumer group..." -ForegroundColor Cyan
Write-Host "OPTION 1: Use Kafka UI" -ForegroundColor Yellow
Write-Host "  1. Open http://72.61.233.209:8080/" -ForegroundColor White
Write-Host "  2. Go to Consumers" -ForegroundColor White
Write-Host "  3. Find 'connect-sink-pipeline-1-mssql-dbo'" -ForegroundColor White
Write-Host "  4. Click Delete" -ForegroundColor White

Write-Host "`nOPTION 2: Use kafka-consumer-groups CLI (if available)" -ForegroundColor Yellow
Write-Host "  kafka-consumer-groups.sh --bootstrap-server $KAFKA_BOOTSTRAP --delete --group $CONSUMER_GROUP" -ForegroundColor White

Write-Host "`nStep 3: After deleting consumer group, restart sink connector..." -ForegroundColor Cyan
Write-Host "Press Enter when done deleting consumer group..." -ForegroundColor Yellow
Read-Host

Write-Host "Resuming sink connector..." -ForegroundColor Cyan
try {
    Invoke-RestMethod -Uri "$CONNECTOR_URL/connectors/$SINK_CONNECTOR/resume" -Method Put
    Write-Host "Connector resumed" -ForegroundColor Green
    Start-Sleep -Seconds 5
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
}

Write-Host "`nStep 4: Restarting sink connector task..." -ForegroundColor Cyan
try {
    Invoke-RestMethod -Uri "$CONNECTOR_URL/connectors/$SINK_CONNECTOR/restart" -Method Post
    Write-Host "Connector restarted" -ForegroundColor Green
    Start-Sleep -Seconds 5
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
}

Write-Host "`nDone! The sink connector should now process all events from the beginning." -ForegroundColor Green
Write-Host "Run 'python scripts/check_cdc_results.py' to verify." -ForegroundColor Yellow
