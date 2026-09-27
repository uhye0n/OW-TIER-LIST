# OW-TIER-LIST

오버워치 티어 리스트

넥슨(한국)과 블리자드(아시아·아메리카·유럽) 공식 영웅 통계(경쟁전 역할 고정, PC)로 계산한 맵별·역할별 영웅 메타 티어 사이트입니다.
구간은 전체(모든 등급), 하위(브론즈·실버·골드), 중위(플래티넘·에메랄드·다이아몬드), 상위(마스터·그랜드마스터·챔피언) 네 가지입니다.
첫 화면(한눈에 보기)은 전체 맵 종합 역할별 티어표와 맵별 메타 조합(돌격 1·공격 2·지원 2) 카드이고, 카드를 누르면 그 맵의 상세 화면(맵·영웅 상세)으로 넘어갑니다. 표본 보정과 지수 가중치 설정은 상세 화면에 있습니다.

## 동작 방식

- `index.html`: 사이트 페이지. 열 때 `data/` 폴더의 서버별 데이터를 불러옵니다.
- `refresh.py`: 네 서버 × 8개 등급 × 전체 맵의 공식 통계 약 1,000개 화면을 수집해(2~3분) 다시 계산하고
  `data/`와 `history/`를 씁니다. 파이썬 표준 라이브러리와 `curl`만 씁니다.
  - `data/index.json`, `data/<서버>.json`: 페이지가 읽는 계산 결과
  - `history/<서버>.json`: 수치가 바뀔 때마다 남기는 기본 가중치 지수 기록(최근 40개). 티어 변동 표시와 패치 감지에 씁니다.
- `.github/workflows/refresh.yml`: GitHub Actions가 6시간마다 `refresh.py`를 실행하고, 수치가 바뀌었을 때만 커밋합니다.
  45일 동안 커밋이 없으면 예약 실행이 멈추지 않도록 빈 커밋을 남깁니다.
- `assets/ranks/`: 오버워치 공식 경쟁전 등급 아이콘(블리자드 공식 사이트 이미지). 영웅 초상화는 블리자드 공식 이미지 주소를 그대로 씁니다.
- `assets/maps/`: 맵 카드 배경 이미지(720×405로 줄인 공식 맵 이미지). 새 맵이 추가되면 `<맵 id>.jpg`로 넣어 주세요. 없으면 배경 없이 표시됩니다.
  모두 Blizzard Entertainment의 저작물입니다.

## 처음 설정

1. **Settings → Actions → General → Workflow permissions**에서 **Read and write permissions**를 선택합니다.
2. **Settings → Pages → Build and deployment**에서 Source를 **Deploy from a branch**, 브랜치를 **main / (root)**로 저장합니다.
3. **Actions** 탭에서 **공식 통계 갱신 → Run workflow**로 한 번 실행해 동작을 확인합니다.

사이트 주소: https://uhye0n.github.io/OW-TIER-LIST/

## 참고

- 수집이 실패하면(공식 페이지 구조 변경 등) 워크플로가 실패로 표시되고 GitHub가 알림 메일을 보냅니다.
  이때 사이트는 마지막으로 성공한 데이터를 그대로 보여줍니다.
- 로컬에서 직접 갱신: `python3 refresh.py` (변동이 없으면 아무것도 쓰지 않습니다. 강제로 다시 계산하려면 `--force`)
- 로컬에서 보기: `python3 -m http.server` 후 http://localhost:8000
