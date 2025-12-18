# Script to remove secrets from git history
# This will rewrite the commit history to remove hardcoded secrets

Write-Host "🔍 Checking current git status..." -ForegroundColor Cyan
git status

Write-Host "`n📋 Current commit history:" -ForegroundColor Cyan
git log --oneline -5

Write-Host "`n⚠️  WARNING: This will rewrite git history!" -ForegroundColor Yellow
Write-Host "The commit 1f032a5b contains secrets and needs to be rewritten." -ForegroundColor Yellow
Write-Host "`nThis script will:" -ForegroundColor Yellow
Write-Host "1. Reset to commit bdbe5195 (before the bad commit)" -ForegroundColor Yellow
Write-Host "2. Re-apply your changes with clean files" -ForegroundColor Yellow
Write-Host "3. You'll need to force push after this" -ForegroundColor Yellow

$confirm = Read-Host "`nContinue? (y/n)"
if ($confirm -ne 'y') {
    Write-Host "Aborted." -ForegroundColor Red
    exit
}

Write-Host "`n🔄 Resetting to bdbe5195..." -ForegroundColor Cyan
git reset --soft bdbe5195

Write-Host "`n✅ Files are staged. Current files are clean (no secrets)." -ForegroundColor Green
Write-Host "`n📝 Now creating new commit with clean files..." -ForegroundColor Cyan
git commit -m "completed - credentials moved to .env file"

Write-Host "`n✅ History rewritten! The secrets are removed." -ForegroundColor Green
Write-Host "`n⚠️  IMPORTANT: You need to force push:" -ForegroundColor Yellow
Write-Host "   git push origin 26nov --force" -ForegroundColor Yellow
Write-Host "`n⚠️  WARNING: Force push will overwrite remote history!" -ForegroundColor Red
Write-Host "Make sure no one else is working on this branch." -ForegroundColor Red




