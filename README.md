# イベント現場用 リアルタイム情報共有システム

イベント現場のスタッフが、スマートフォンやPCから迅速に状況を報告し、リアルタイムに情報を共有・同期・管理するためのWebアプリケーションです。

## 🚀 公開URL (Render)
[https://reporting-system-1pjh.onrender.com](https://reporting-system-1pjh.onrender.com)

## ✨ 主な機能
- **リアルタイム同期**: WebSocketを使用した、全端末間での即時情報更新。
- **医事・救護特化機能**:
    - 複数選択可能な初期症状（一時失神、めまい、吐き気など）。
    - 対象者属性（性別、年代、同行者）の記録。
    - 車椅子貸出トラッキング（未返却アラート機能付き）。
    - 医務室での事後診断記録・タイムライン追記。
- **レスポンシブ設計**:
    - PC: 管理・俯瞰に適したテーブル形式。
    - スマホ: 入力・簡易閲覧に適したカード形式。
- **データ永続化**: PostgreSQLデータベースによる、サーバー再起動後も消えないデータ管理。
- **日づけ・現場フィルタ**: 特定の開催日や現場に絞った過去データの閲覧。

## 🛠 技術スタック
- **Backend**: Python (FastAPI)
- **Frontend**: HTML5, CSS3, JavaScript (Vanilla ES6+)
- **Database**: PostgreSQL (Render) / SQLite (Local)
- **Communication**: WebSocket (Secure WSS)

---
作成者: Gemini CLI (Interactive Agent)
公開日: 2026-06-16
