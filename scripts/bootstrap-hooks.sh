#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[bootstrap-hooks] git 저장소 루트에서 실행해야 합니다."
  exit 1
fi

if [ ! -d ".githooks" ]; then
  echo "[bootstrap-hooks] .githooks 디렉터리를 찾을 수 없습니다."
  exit 1
fi

git config core.hooksPath .githooks

find .githooks -maxdepth 1 -type f -exec chmod +x {} \;

echo "[bootstrap-hooks] core.hooksPath=.githooks 설정 완료"
echo "[bootstrap-hooks] 훅 실행 권한 부여 완료"

if command -v actionlint >/dev/null 2>&1; then
  echo "[bootstrap-hooks] actionlint 확인 완료: $(actionlint -version | head -n 1)"
else
  echo "[bootstrap-hooks] actionlint 미설치 상태입니다."
  echo "[bootstrap-hooks] macOS: brew install actionlint"
fi