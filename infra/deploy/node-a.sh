#!/usr/bin/env bash
# eonje-a에서 스택을 주어진 커밋 sha의 이미지로 갱신한다.
# CD가 deploy 계정으로 실행한다:  ssh deploy@eonje-a "bash -s -- <sha>" < infra/deploy/node-a.sh
#
# 순서: 이미지 받기 → 저장소 기동 → 마이그레이션 → 버킷 → 전체 기동(헬스체크 대기)
# 마이그레이션이나 기동이 실패하면 여기서 멈추고 image.env는 직전 성공 sha를 유지한다.
# 롤백은 직전 sha(image.env.prev)로 다시 배포하면 된다.
set -euo pipefail

sha="${1:?usage: node-a.sh <40-char commit sha>}"
[[ "$sha" =~ ^[0-9a-f]{40}$ ]] || { echo "not a full commit sha: $sha" >&2; exit 2; }

cd "$HOME/eonje"
[[ -f .env ]] || { echo "missing ~/eonje/.env (managed on the server by hand)" >&2; exit 2; }

echo "EONJE_IMAGE_TAG=$sha" > image.env.next
compose=(docker compose
  -f infra/compose.node-a.yaml -f infra/compose.node-a.prod.yaml
  --env-file .env --env-file image.env.next)

# collector는 tools 프로필(migrate)에서만 쓰여 기본 pull 대상이 아니다
"${compose[@]}" --profile tools pull
"${compose[@]}" up -d --wait postgres seaweedfs
"${compose[@]}" run --rm migrate

bucket_out=$("${compose[@]}" exec -T seaweedfs sh -c \
  'echo "s3.bucket.create -name ${EONJE_RAW_BUCKET:-eonje-raw}" | weed shell' 2>&1)
echo "$bucket_out"
case "$bucket_out" in
  *"created bucket"*|*"already exists"*) ;;
  *) echo "bucket check failed" >&2; exit 1 ;;
esac

"${compose[@]}" up -d --wait --remove-orphans

# 성공했을 때만 태그 파일을 갱신한다
[[ -f image.env ]] && cp image.env image.env.prev
mv image.env.next image.env
"${compose[@]/image.env.next/image.env}" ps

# 일주일 넘게 안 쓰인 이미지 정리 (지금 쓰는 이미지는 남는다)
docker image prune -af --filter "until=168h" >/dev/null
echo "deployed $sha"
