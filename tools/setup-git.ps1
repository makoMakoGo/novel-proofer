$ErrorActionPreference = "Stop"

$repoRoot = (git rev-parse --show-toplevel).Trim()
Set-Location $repoRoot

if (-not (Test-Path ".githooks/commit-msg")) {
  throw "Missing .githooks/commit-msg. Please run this script from a valid repo checkout."
}

git config core.hooksPath ".githooks"
git config commit.template ".gitmessage"

Write-Host "Configured git:" -ForegroundColor Green
Write-Host "  core.hooksPath = .githooks"
Write-Host "  commit.template = .gitmessage"

