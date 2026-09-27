# OW-TIER-LIST

오버워치 티어 리스트

넥슨 오버워치 영웅 통계(한국, 경쟁전 역할 고정, PC)로 계산한 맵별·역할별 영웅 메타 티어 사이트입니다.
구간은 하위(브론즈·실버·골드), 중위(플래티넘·에메랄드·다이아몬드), 상위(마스터·그랜드마스터·챔피언) 세 가지입니다.

## 동작 방식

- `index.html`: 사이트 페이지. 계산된 데이터가 `/*OWDATA:BEGIN*/ … /*OWDATA:END*/` 사이에 들어 있습니다.
- `refresh.py`: 넥슨 통계 페이지 약 340개를 수집해(2~3분) 다시 계산하고, `index.html`의 데이터 부분만 바꿉니다.
  파이썬 표준 라이브러리와 `curl`만 씁니다.
- `.github/workflows/refresh.yml`: GitHub Actions가 6시간마다 `refresh.py`를 실행하고,
  수치가 바뀌었을 때만 커밋합니다. 커밋되면 GitHub Pages가 사이트를 자동으로 다시 배포합니다.

## 처음 설정

1. 이 파일들을 새 저장소에 올립니다(공개 저장소면 Actions와 Pages가 무료입니다).
2. **Settings → Actions → General → Workflow permissions**에서 **Read and write permissions**를 선택합니다.
3. **Settings → Pages → Build and deployment**에서 Source를 **Deploy from a branch**, 브랜치를 **main / (root)**로 저장합니다.
4. **Actions** 탭에서 **넥슨 통계 갱신 → Run workflow**로 한 번 실행해 동작을 확인합니다.

사이트 주소는 `https://<사용자 이름>.github.io/<저장소 이름>/`입니다.

## 참고

- 수집이 실패하면(넥슨 페이지 구조 변경 등) 워크플로가 실패로 표시되고 GitHub가 알림 메일을 보냅니다.
  이때 페이지는 마지막으로 성공한 데이터를 그대로 보여줍니다.
- GitHub는 60일 동안 저장소 활동이 없으면 예약 실행을 멈춥니다. 수치 변동이 오래 없어서 멈췄다면
  Actions 탭에서 다시 켜 주세요.
- 로컬에서 직접 갱신: `python3 refresh.py --page index.html --out index.html`

- `assets/ranks/`: 오버워치 공식 경쟁전 등급 아이콘(블리자드 공식 사이트 이미지). 저작권은 Blizzard Entertainment에 있습니다.
