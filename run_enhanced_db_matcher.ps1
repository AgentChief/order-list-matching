#!/usr/bin/env pwsh

Write-Host "Enhanced Database-Driven Matcher" -ForegroundColor Green
Write-Host "Customer: GREYSON, PO: 4755" -ForegroundColor Yellow
Write-Host "Starting enhanced matching with database configuration..." -ForegroundColor Cyan

# Stop any running streamlit processes
Get-Process -Name "streamlit" -ErrorAction SilentlyContinue | Stop-Process -Force

# Run the enhanced matcher
python enhanced_db_matcher.py --customer "GREYSON" --po "4755"

Write-Host "Enhanced matching completed!" -ForegroundColor Green
Read-Host "Press Enter to continue..."
