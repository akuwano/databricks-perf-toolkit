## Summary

<!-- What does this PR do? 1-3 bullets. -->

## 変更内訳

<!-- 主要な変更点。ファイル単位ではなく機能/目的単位で。 -->

## Test plan

- [ ] `cd dabs/app && uv run pytest` 全pass
- [ ] 該当する場合: 新規テスト追加済み（新規モジュール/APIエンドポイント/バグ修正）
- [ ] CI 全ジョブ緑（Lint / Build / Type Check / Validate / Test）
- [ ] 手動動作確認（UI変更がある場合）

## チェックリスト

- [ ] コミットは Conventional Commits 形式 (`feat(scope): description`)
- [ ] PR サイズは < 1000 行（超える場合は分割検討）
- [ ] 機密情報 (.env, credentials, token) が含まれていない
- [ ] main への直接 push ではなく、このブランチから PR 経由
- [ ] **セルフマージはしない** — レビュアーを指定して review を待つ
