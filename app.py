import re
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime
from functools import wraps
from pathlib import Path

from flask import Flask, flash, g, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "ai-learning-secret"
    app.config["DATABASE"] = Path(app.root_path) / "academy.db"
    momo_phone = "0375.595.019"
    momo_owner = "Bach Sy Khang"

    courses = [
        {
            "slug": "ai-cho-nguoi-moi-bat-dau",
            "title": "AI Cho Người Mới Bắt Đầu",
            "level": "Cơ bản",
            "category": "Nền tảng AI",
            "career_track": "AI Product Starter",
            "duration": "6 tuần",
            "duration_weeks": 6,
            "price": "1.290.000đ",
            "rating": 4.8,
            "students": 1240,
            "description": "Nắm chắc nền tảng AI, Machine Learning và cách ứng dụng vào học tập, công việc.",
            "skills": [
                "Tư duy AI thực chiến",
                "Tiền xử lý dữ liệu cơ bản",
                "Prompt hiệu quả cho công cụ GenAI",
                "Đánh giá chất lượng mô hình",
            ],
            "outcomes": [
                "Hiểu rõ lộ trình học AI từ cơ bản đến thực chiến",
                "Tự làm được mini project AI đầu tay",
                "Biết cách dùng AI để tăng năng suất làm việc hằng ngày",
            ],
            "requirements": [
                "Biết sử dụng máy tính và internet",
                "Không yêu cầu nền tảng lập trình trước đó",
            ],
            "lessons": [
                "AI là gì và hệ sinh thái hiện nay",
                "Tư duy dữ liệu cho người không chuyên",
                "Học có giám sát và không giám sát",
                "Prompt engineering nền tảng",
                "Dự án mini: trợ lý học tập AI",
            ],
            "syllabus": [
                {"week": 1, "title": "Bản đồ AI hiện đại", "summary": "Nắm các khái niệm cốt lõi và use case thực tế."},
                {"week": 2, "title": "Dữ liệu và chất lượng dữ liệu", "summary": "Hiểu vì sao dữ liệu quyết định chất lượng AI."},
                {"week": 3, "title": "Machine Learning nền tảng", "summary": "Phân biệt bài toán phân loại, hồi quy, clustering."},
                {"week": 4, "title": "Prompt thực chiến", "summary": "Viết prompt rõ mục tiêu, đo chất lượng output."},
                {"week": 5, "title": "Ứng dụng AI vào công việc", "summary": "Tự động hóa tác vụ lặp lại với AI tools."},
                {"week": 6, "title": "Mini project & demo", "summary": "Xây trợ lý AI cá nhân và trình bày kết quả."},
            ],
            "projects": [
                "Trợ lý học tập AI cho sinh viên",
                "Bộ prompt tăng năng suất công việc văn phòng",
            ],
            "faq": [
                {"q": "Không biết code có học được không?", "a": "Có. Khóa này thiết kế cho người mới bắt đầu hoàn toàn."},
                {"q": "Học xong làm được gì?", "a": "Bạn có thể áp dụng AI vào học tập, công việc và tự làm mini project."},
            ],
        },
        {
            "slug": "machine-learning-thuc-hanh",
            "title": "Machine Learning Thực Hành",
            "level": "Trung cấp",
            "category": "Machine Learning",
            "career_track": "ML Engineer",
            "duration": "8 tuần",
            "duration_weeks": 8,
            "price": "2.490.000đ",
            "rating": 4.9,
            "students": 860,
            "description": "Xây dựng pipeline ML hoàn chỉnh với Python, đánh giá mô hình và tối ưu hiệu năng.",
            "skills": [
                "Scikit-learn thực tế",
                "Cross-validation và tuning",
                "Giải thích mô hình",
                "Phân tích lỗi và cải tiến",
            ],
            "outcomes": [
                "Xây được pipeline ML end-to-end",
                "Biết đánh giá và tối ưu model theo mục tiêu kinh doanh",
                "Đọc hiểu metric và giải thích model cho team sản phẩm",
            ],
            "requirements": [
                "Biết Python cơ bản",
                "Biết thao tác dữ liệu với pandas ở mức nền tảng",
            ],
            "lessons": [
                "Thiết kế pipeline ML",
                "Huấn luyện model phân loại, hồi quy",
                "Tối ưu hyperparameter",
                "Feature importance và SHAP",
                "Capstone: dự đoán churn khách hàng",
            ],
            "syllabus": [
                {"week": 1, "title": "Problem framing", "summary": "Chuyển bài toán kinh doanh thành bài toán ML."},
                {"week": 2, "title": "Data preprocessing", "summary": "Làm sạch dữ liệu, xử lý missing/outlier."},
                {"week": 3, "title": "Baseline models", "summary": "Xây baseline để có mốc cải tiến rõ ràng."},
                {"week": 4, "title": "Model evaluation", "summary": "Chọn metric phù hợp theo mục tiêu hệ thống."},
                {"week": 5, "title": "Hyperparameter tuning", "summary": "Grid/Random search và tránh overfitting."},
                {"week": 6, "title": "Explainable ML", "summary": "SHAP, feature importance, error slicing."},
                {"week": 7, "title": "Model packaging", "summary": "Đóng gói model và chuẩn bị deploy."},
                {"week": 8, "title": "Capstone demo", "summary": "Trình bày dự án churn prediction hoàn chỉnh."},
            ],
            "projects": [
                "Churn prediction pipeline cho doanh nghiệp SaaS",
                "Hệ thống scoring khách hàng ưu tiên chăm sóc",
            ],
            "faq": [
                {"q": "Khóa này có khó không?", "a": "Mức trung cấp, cần biết Python cơ bản trước khi học."},
                {"q": "Có dự án thật không?", "a": "Có capstone dùng dữ liệu thực tế và trình bày như đi làm."},
            ],
        },
        {
            "slug": "xay-dung-ung-dung-genai",
            "title": "Xây Dựng Ứng Dụng GenAI",
            "level": "Nâng cao",
            "category": "Generative AI",
            "career_track": "GenAI Developer",
            "duration": "10 tuần",
            "duration_weeks": 10,
            "price": "3.990.000đ",
            "rating": 4.9,
            "students": 540,
            "description": "Thiết kế và triển khai ứng dụng AI dùng LLM, RAG, tối ưu chi phí và bảo mật đầu ra.",
            "skills": [
                "Tích hợp API LLM",
                "RAG và Vector Database",
                "Thiết kế prompt template",
                "An toàn và kiểm soát output",
            ],
            "outcomes": [
                "Xây được ứng dụng GenAI có truy xuất tri thức nội bộ",
                "Tối ưu độ chính xác, latency và chi phí vận hành",
                "Thiết kế guardrails cho tình huống rủi ro",
            ],
            "requirements": [
                "Biết Python và API cơ bản",
                "Hiểu nền tảng machine learning là lợi thế",
            ],
            "lessons": [
                "Kiến trúc ứng dụng LLM",
                "Chunking và retrieval",
                "Tool calling và agent workflow",
                "Quan trắc chi phí và latency",
                "Dự án cuối kỳ: AI Copilot nội bộ",
            ],
            "syllabus": [
                {"week": 1, "title": "GenAI architecture", "summary": "Tổng quan kiến trúc ứng dụng LLM production."},
                {"week": 2, "title": "Prompt system design", "summary": "Thiết kế prompt template có thể tái sử dụng."},
                {"week": 3, "title": "RAG foundation", "summary": "Embeddings, vector DB và retrieval cơ bản."},
                {"week": 4, "title": "RAG nâng cao", "summary": "Re-ranking, hybrid search, đánh giá retrieval."},
                {"week": 5, "title": "Tool calling", "summary": "Kết nối LLM với tools/service ngoài."},
                {"week": 6, "title": "Agent workflow", "summary": "Thiết kế multi-step reasoning an toàn."},
                {"week": 7, "title": "Eval & safety", "summary": "Xây bộ đánh giá và guardrails đầu ra."},
                {"week": 8, "title": "Cost & latency", "summary": "Tối ưu token, cache, batching."},
                {"week": 9, "title": "Deployment", "summary": "Đưa ứng dụng lên môi trường thực tế."},
                {"week": 10, "title": "Final demo", "summary": "Trình bày AI Copilot nội bộ hoàn chỉnh."},
            ],
            "projects": [
                "AI Copilot tra cứu tri thức nội bộ dùng RAG",
                "Hệ thống trợ lý bán hàng đa kênh có guardrails",
            ],
            "faq": [
                {"q": "Khóa này có dạy deploy không?", "a": "Có, bạn sẽ học cách deploy và quan trắc ứng dụng GenAI."},
                {"q": "Có hỗ trợ tối ưu chi phí API không?", "a": "Có, đây là một phần trọng tâm của tuần 8."},
            ],
        },
    ]

    def get_db() -> sqlite3.Connection:
        if "db" not in g:
            conn = sqlite3.connect(app.config["DATABASE"])
            conn.row_factory = sqlite3.Row
            g.db = conn
        return g.db

    @app.teardown_appcontext
    def close_db(_exception) -> None:
        db = g.pop("db", None)
        if db is not None:
            db.close()

    def init_db() -> None:
        db = get_db()
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                username TEXT UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('member', 'admin')) DEFAULT 'member',
                balance INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS enrollments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                course_slug TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(user_id, course_slug),
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS learning_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                course_slug TEXT NOT NULL,
                completed_steps INTEGER NOT NULL DEFAULT 0,
                completed_lessons INTEGER NOT NULL DEFAULT 0,
                quiz_score INTEGER NOT NULL DEFAULT 0,
                quiz_passed INTEGER NOT NULL DEFAULT 0,
                certificate_issued INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                UNIQUE(user_id, course_slug),
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                tx_type TEXT NOT NULL,
                amount INTEGER NOT NULL,
                note TEXT NOT NULL,
                balance_after INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS code_submissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                course_slug TEXT NOT NULL,
                week INTEGER NOT NULL,
                code TEXT NOT NULL,
                output TEXT NOT NULL,
                passed INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )

        columns = {row["name"] for row in db.execute("PRAGMA table_info(users)").fetchall()}
        if "username" not in columns:
            db.execute("ALTER TABLE users ADD COLUMN username TEXT")
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users(username)")

        learning_columns = {row["name"] for row in db.execute("PRAGMA table_info(learning_progress)").fetchall()}
        if "completed_lessons" not in learning_columns:
            db.execute("ALTER TABLE learning_progress ADD COLUMN completed_lessons INTEGER NOT NULL DEFAULT 0")
        if "quiz_score" not in learning_columns:
            db.execute("ALTER TABLE learning_progress ADD COLUMN quiz_score INTEGER NOT NULL DEFAULT 0")
        if "quiz_passed" not in learning_columns:
            db.execute("ALTER TABLE learning_progress ADD COLUMN quiz_passed INTEGER NOT NULL DEFAULT 0")
        if "certificate_issued" not in learning_columns:
            db.execute("ALTER TABLE learning_progress ADD COLUMN certificate_issued INTEGER NOT NULL DEFAULT 0")

        admin_email = "admin@sykhang.vn"
        admin_exists = db.execute("SELECT id FROM users WHERE email = ?", (admin_email,)).fetchone()
        if not admin_exists:
            db.execute(
                """
                INSERT INTO users (full_name, email, username, password_hash, role, balance, created_at)
                VALUES (?, ?, ?, ?, 'admin', ?, ?)
                """,
                (
                    "Quản trị viên",
                    admin_email,
                    "admin",
                    generate_password_hash("adminsykhangz1000"),
                    5000000,
                    datetime.now().isoformat(timespec="seconds"),
                ),
            )

        users_missing_username = db.execute(
            "SELECT id, email FROM users WHERE username IS NULL OR TRIM(username) = ''"
        ).fetchall()
        for user in users_missing_username:
            base = (user["email"].split("@")[0] if user["email"] else "").lower()
            base = re.sub(r"[^a-z0-9_]", "", base)
            if len(base) < 3:
                base = f"user{user['id']}"
            username = base
            suffix = 1
            while db.execute(
                "SELECT id FROM users WHERE username = ? AND id <> ?",
                (username, user["id"]),
            ).fetchone():
                username = f"{base}{suffix}"
                suffix += 1
            db.execute("UPDATE users SET username = ? WHERE id = ?", (username, user["id"]))
        db.commit()

    with app.app_context():
        init_db()

    def get_course(slug: str):
        return next((course for course in courses if course["slug"] == slug), None)

    def parse_price_vnd(price_text: str) -> int:
        digits = re.sub(r"[^\d]", "", price_text or "")
        return int(digits) if digits else 0

    def build_learning_units(course: dict) -> list[dict]:
        units = []
        for item in course["syllabus"]:
            week = item["week"]
            units.append(
                {
                    "week": week,
                    "title": item["title"],
                    "summary": item["summary"],
                    "video_minutes": 20 + (week * 3),
                    "reading": f"Tài liệu tuần {week}: {item['title']}",
                    "exercise": f"Bài tập tuần {week}: ứng dụng {item['title'].lower()} vào case thực tế.",
                }
            )
        return units

    def build_quiz_questions(course: dict) -> list[dict]:
        return [
            {
                "id": "q1",
                "question": f"Đâu là mục tiêu quan trọng nhất của khóa {course['title']}?",
                "options": [
                    "Xây kỹ năng thực chiến và làm dự án thật",
                    "Chỉ học lý thuyết không cần thực hành",
                    "Tập trung ghi nhớ công thức",
                    "Không cần đánh giá kết quả học tập",
                ],
                "answer": 0,
            },
            {
                "id": "q2",
                "question": "Khi xây hệ thống AI, bước nào giúp đảm bảo chất lượng đầu ra?",
                "options": [
                    "Bỏ qua dữ liệu đầu vào",
                    "Đánh giá mô hình và kiểm thử liên tục",
                    "Chỉ tăng số lượng prompt",
                    "Triển khai ngay mà không theo dõi",
                ],
                "answer": 1,
            },
            {
                "id": "q3",
                "question": "Kết quả cuối khóa nên có là gì?",
                "options": [
                    "Một dự án có thể demo",
                    "Chỉ là ghi chú cá nhân",
                    "Không cần sản phẩm",
                    "Không cần trình bày kết quả",
                ],
                "answer": 0,
            },
        ]

    def build_coding_exercise(week: int) -> dict:
        templates = [
            {
                "title": "Chuẩn hóa văn bản",
                "prompt": "Viết hàm normalize_text(text) trả về chuỗi đã strip và lower.",
                "starter": "def normalize_text(text):\n    # TODO: your code\n    pass\n",
                "tests": """
assert normalize_text("  Xin Chao AI  ") == "xin chao ai"
assert normalize_text("DATA ") == "data"
print("OK: normalize_text")
""",
            },
            {
                "title": "Đếm phần tử thiếu",
                "prompt": "Viết hàm count_missing(values) trả về số phần tử None trong list.",
                "starter": "def count_missing(values):\n    # TODO: your code\n    pass\n",
                "tests": """
assert count_missing([1, None, 2, None, 3]) == 2
assert count_missing([]) == 0
print("OK: count_missing")
""",
            },
            {
                "title": "Hàm dự đoán tuyến tính",
                "prompt": "Viết hàm linear_predict(x, m, b) trả về m*x + b.",
                "starter": "def linear_predict(x, m, b):\n    # TODO: your code\n    pass\n",
                "tests": """
assert linear_predict(5, 2, 1) == 11
assert linear_predict(-1, 3, 4) == 1
print("OK: linear_predict")
""",
            },
            {
                "title": "Top-k điểm cao nhất",
                "prompt": "Viết hàm top_k_scores(scores, k) trả về k phần tử lớn nhất theo thứ tự giảm dần.",
                "starter": "def top_k_scores(scores, k):\n    # TODO: your code\n    pass\n",
                "tests": """
assert top_k_scores([3, 10, 7, 8], 2) == [10, 8]
assert top_k_scores([1, 2], 5) == [2, 1]
print("OK: top_k_scores")
""",
            },
            {
                "title": "Độ chính xác mô hình",
                "prompt": "Viết hàm accuracy(y_true, y_pred) trả về tỉ lệ dự đoán đúng từ 0 đến 1.",
                "starter": "def accuracy(y_true, y_pred):\n    # TODO: your code\n    pass\n",
                "tests": """
assert abs(accuracy([1,0,1,1], [1,1,1,0]) - 0.5) < 1e-9
assert abs(accuracy([1,1], [1,1]) - 1.0) < 1e-9
print("OK: accuracy")
""",
            },
            {
                "title": "Tạo prompt chuẩn",
                "prompt": "Viết hàm build_prompt(task, style) trả về chuỗi dạng: 'Task: <task> | Style: <style>'.",
                "starter": "def build_prompt(task, style):\n    # TODO: your code\n    pass\n",
                "tests": """
assert build_prompt("Tóm tắt", "ngắn gọn") == "Task: Tóm tắt | Style: ngắn gọn"
assert build_prompt("Phân tích", "chuyên sâu") == "Task: Phân tích | Style: chuyên sâu"
print("OK: build_prompt")
""",
            },
        ]
        return templates[(week - 1) % len(templates)]

    def run_user_code(code: str, tests: str) -> tuple[bool, str]:
        banned_tokens = [
            "import os",
            "import sys",
            "subprocess",
            "open(",
            "__import__",
            "eval(",
            "exec(",
            "socket",
            "pathlib",
            "shutil",
            "requests",
        ]
        lowered = code.lower()
        for token in banned_tokens:
            if token in lowered:
                return False, f"Code chứa cú pháp không được phép: {token}"

        script = f"{code}\n\n{tests}\n"
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False, encoding="utf-8") as tmp_file:
                tmp_file.write(script)
                tmp_path = tmp_file.name
            result = subprocess.run(
                [sys.executable, tmp_path],
                capture_output=True,
                text=True,
                timeout=3,
            )
            output = (result.stdout or "") + (result.stderr or "")
            if result.returncode == 0:
                return True, output.strip() or "Bài làm đúng. Tests passed."
            return False, output.strip() or "Bài làm chưa đạt."
        except subprocess.TimeoutExpired:
            return False, "Code chạy quá thời gian cho phép (timeout 3 giây)."
        except Exception as exc:  # pragma: no cover
            return False, f"Lỗi thực thi: {exc}"
        finally:
            if tmp_path:
                try:
                    Path(tmp_path).unlink(missing_ok=True)
                except Exception:
                    pass

    def user_has_enrollment(user_id: int, course_slug: str) -> bool:
        db = get_db()
        enrollment = db.execute(
            "SELECT id FROM enrollments WHERE user_id = ? AND course_slug = ?",
            (user_id, course_slug),
        ).fetchone()
        return bool(enrollment)

    def get_learning_progress(user_id: int, course_slug: str):
        db = get_db()
        progress = db.execute(
            """
            SELECT completed_steps, completed_lessons, quiz_score, quiz_passed, certificate_issued, updated_at
            FROM learning_progress
            WHERE user_id = ? AND course_slug = ?
            """,
            (user_id, course_slug),
        ).fetchone()
        if progress:
            return progress
        with db:
            db.execute(
                """
                INSERT INTO learning_progress (
                    user_id, course_slug, completed_steps, completed_lessons,
                    quiz_score, quiz_passed, certificate_issued, updated_at
                )
                VALUES (?, ?, 0, 0, 0, 0, 0, ?)
                """,
                (user_id, course_slug, datetime.now().isoformat(timespec="seconds")),
            )
        return db.execute(
            """
            SELECT completed_steps, completed_lessons, quiz_score, quiz_passed, certificate_issued, updated_at
            FROM learning_progress
            WHERE user_id = ? AND course_slug = ?
            """,
            (user_id, course_slug),
        ).fetchone()

    def get_user_by_id(user_id: int):
        db = get_db()
        return db.execute(
            "SELECT id, full_name, email, username, role, balance, created_at FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()

    def log_transaction(db: sqlite3.Connection, user_id: int, tx_type: str, amount: int, note: str) -> None:
        user = db.execute("SELECT balance FROM users WHERE id = ?", (user_id,)).fetchone()
        if not user:
            return
        db.execute(
            """
            INSERT INTO transactions (user_id, tx_type, amount, note, balance_after, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (user_id, tx_type, amount, note, user["balance"], datetime.now().isoformat(timespec="seconds")),
        )

    def login_required(view):
        @wraps(view)
        def wrapped_view(*args, **kwargs):
            if "user_id" not in session:
                flash("Vui lòng đăng nhập để tiếp tục.", "error")
                return redirect(url_for("login", next=request.path))
            return view(*args, **kwargs)

        return wrapped_view

    def admin_required(view):
        @wraps(view)
        def wrapped_view(*args, **kwargs):
            user = g.get("current_user")
            if not user or user["role"] != "admin":
                flash("Bạn không có quyền truy cập khu vực quản trị.", "error")
                return redirect(url_for("dashboard"))
            return view(*args, **kwargs)

        return wrapped_view

    @app.before_request
    def load_current_user():
        g.current_user = None
        user_id = session.get("user_id")
        if user_id:
            g.current_user = get_user_by_id(user_id)

    @app.context_processor
    def inject_globals():
        return {
            "current_year": datetime.now().year,
            "current_user": g.get("current_user"),
            "is_admin": bool(g.get("current_user") and g.current_user["role"] == "admin"),
        }

    @app.route("/")
    def home():
        total_students = sum(course["students"] for course in courses)
        avg_rating = round(sum(course["rating"] for course in courses) / len(courses), 1)
        learning_paths = sorted({course["career_track"] for course in courses})
        return render_template(
            "index.html",
            featured_courses=courses,
            total_courses=len(courses),
            total_students=total_students,
            avg_rating=avg_rating,
            learning_paths=learning_paths,
        )

    @app.route("/courses")
    def course_list():
        level = request.args.get("level", "Tất cả")
        topic = request.args.get("topic", "Tất cả")
        track = request.args.get("track", "Tất cả")
        sort = request.args.get("sort", "popular")
        query = request.args.get("q", "").strip().lower()
        valid_levels = ["Tất cả", "Cơ bản", "Trung cấp", "Nâng cao"]
        topics = ["Tất cả"] + sorted({c["category"] for c in courses})
        tracks = ["Tất cả"] + sorted({c["career_track"] for c in courses})
        valid_sorts = {"popular", "rating", "price_asc", "price_desc", "duration"}

        if level not in valid_levels:
            level = "Tất cả"
        if topic not in topics:
            topic = "Tất cả"
        if track not in tracks:
            track = "Tất cả"
        if sort not in valid_sorts:
            sort = "popular"

        filtered_courses = courses if level == "Tất cả" else [c for c in courses if c["level"] == level]
        if topic != "Tất cả":
            filtered_courses = [c for c in filtered_courses if c["category"] == topic]
        if track != "Tất cả":
            filtered_courses = [c for c in filtered_courses if c["career_track"] == track]
        if query:
            filtered_courses = [
                c
                for c in filtered_courses
                if query in c["title"].lower()
                or query in c["description"].lower()
                or any(query in skill.lower() for skill in c["skills"])
                or query in c["career_track"].lower()
            ]
        if sort == "rating":
            filtered_courses = sorted(filtered_courses, key=lambda c: c["rating"], reverse=True)
        elif sort == "price_asc":
            filtered_courses = sorted(filtered_courses, key=lambda c: parse_price_vnd(c["price"]))
        elif sort == "price_desc":
            filtered_courses = sorted(filtered_courses, key=lambda c: parse_price_vnd(c["price"]), reverse=True)
        elif sort == "duration":
            filtered_courses = sorted(filtered_courses, key=lambda c: c["duration_weeks"])
        else:
            filtered_courses = sorted(filtered_courses, key=lambda c: c["students"], reverse=True)

        return render_template(
            "courses.html",
            courses=filtered_courses,
            selected_level=level,
            levels=valid_levels,
            selected_topic=topic,
            topics=topics,
            selected_track=track,
            tracks=tracks,
            selected_sort=sort,
            search_query=query,
            total_courses=len(filtered_courses),
        )

    @app.route("/courses/<slug>")
    def course_detail(slug: str):
        course = get_course(slug)
        if not course:
            return render_template("404.html"), 404
        related_courses = [
            c for c in courses if c["slug"] != course["slug"] and c["category"] == course["category"]
        ][:2]
        is_enrolled = False
        if g.current_user:
            is_enrolled = user_has_enrollment(g.current_user["id"], slug)
        return render_template(
            "course_detail.html",
            course=course,
            related_courses=related_courses,
            is_enrolled=is_enrolled,
        )

    @app.route("/learn/<slug>")
    @login_required
    def learn_course(slug: str):
        course = get_course(slug)
        if not course:
            return render_template("404.html"), 404
        if not user_has_enrollment(g.current_user["id"], slug):
            flash("Bạn cần mua khóa học trước khi vào học.", "error")
            return redirect(url_for("course_detail", slug=slug))
        units = build_learning_units(course)
        progress = get_learning_progress(g.current_user["id"], slug)
        completed_steps = progress["completed_steps"]
        completed_lessons = progress["completed_lessons"]
        quiz_score = progress["quiz_score"]
        quiz_passed = bool(progress["quiz_passed"])
        certificate_issued = bool(progress["certificate_issued"])
        updated_at = progress["updated_at"]

        total_steps = len(units)
        completion_percent = int((completed_steps / total_steps) * 100) if total_steps else 0
        next_step = completed_steps + 1 if completed_steps < total_steps else total_steps
        next_unit = units[next_step - 1] if total_steps and next_step >= 1 else None
        return render_template(
            "learn_course.html",
            course=course,
            units=units,
            completed_steps=completed_steps,
            completed_lessons=completed_lessons,
            total_steps=total_steps,
            completion_percent=completion_percent,
            next_step=next_step,
            next_unit=next_unit,
            quiz_score=quiz_score,
            quiz_passed=quiz_passed,
            certificate_issued=certificate_issued,
            updated_at=updated_at,
        )

    @app.route("/learn/<slug>/lesson/<int:week>")
    @login_required
    def learn_lesson(slug: str, week: int):
        course = get_course(slug)
        if not course:
            return render_template("404.html"), 404
        if not user_has_enrollment(g.current_user["id"], slug):
            flash("Bạn cần mua khóa học trước khi vào học.", "error")
            return redirect(url_for("course_detail", slug=slug))

        units = build_learning_units(course)
        lesson = next((u for u in units if u["week"] == week), None)
        if not lesson:
            return render_template("404.html"), 404

        exercise = build_coding_exercise(week)

        progress = get_learning_progress(g.current_user["id"], slug)
        completed_steps = progress["completed_steps"]
        can_mark = week <= (completed_steps + 1)
        is_done = week <= completed_steps
        db = get_db()
        last_submission = db.execute(
            """
            SELECT code, output, passed, created_at
            FROM code_submissions
            WHERE user_id = ? AND course_slug = ? AND week = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (g.current_user["id"], slug, week),
        ).fetchone()
        starter_code = last_submission["code"] if last_submission else exercise["starter"]
        return render_template(
            "learn_lesson.html",
            course=course,
            units=units,
            lesson=lesson,
            exercise=exercise,
            completed_steps=completed_steps,
            can_mark=can_mark,
            is_done=is_done,
            starter_code=starter_code,
            last_submission=last_submission,
        )

    @app.route("/learn/<slug>/lesson/<int:week>/code", methods=["POST"])
    @login_required
    def run_lesson_code(slug: str, week: int):
        course = get_course(slug)
        if not course:
            return render_template("404.html"), 404
        if not user_has_enrollment(g.current_user["id"], slug):
            flash("Bạn cần mua khóa học trước khi cập nhật tiến độ.", "error")
            return redirect(url_for("course_detail", slug=slug))

        units = build_learning_units(course)
        total_steps = len(units)
        if week < 1 or week > total_steps:
            return render_template("404.html"), 404

        code = request.form.get("code", "")
        action = request.form.get("action", "run")
        exercise = build_coding_exercise(week)
        if not code.strip():
            flash("Vui lòng nhập code trước khi chạy.", "error")
            return redirect(url_for("learn_lesson", slug=slug, week=week))

        passed, output = run_user_code(code, exercise["tests"])
        progress = get_learning_progress(g.current_user["id"], slug)
        completed_steps = progress["completed_steps"]
        can_unlock = week <= (completed_steps + 1)
        if not can_unlock:
            flash("Bạn cần học tuần trước trước khi mở tuần này.", "error")
            return redirect(url_for("learn_lesson", slug=slug, week=completed_steps + 1))

        db = get_db()
        with db:
            db.execute(
                """
                INSERT INTO code_submissions (user_id, course_slug, week, code, output, passed, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    g.current_user["id"],
                    slug,
                    week,
                    code,
                    output,
                    1 if passed else 0,
                    datetime.now().isoformat(timespec="seconds"),
                ),
            )
            new_completed = completed_steps
            if passed and action == "submit":
                new_completed = max(completed_steps, week)
            db.execute(
                """
                UPDATE learning_progress
                SET completed_steps = ?, completed_lessons = ?, updated_at = ?
                WHERE user_id = ? AND course_slug = ?
                """,
                (
                    new_completed,
                    new_completed,
                    datetime.now().isoformat(timespec="seconds"),
                    g.current_user["id"],
                    slug,
                ),
            )

        if not passed:
            flash("Code chưa đạt. Xem output để sửa và chạy lại.", "error")
            return redirect(url_for("learn_lesson", slug=slug, week=week))
        if action == "run":
            flash("Code chạy thành công. Nhấn 'Nộp bài' để ghi nhận hoàn thành.", "success")
            return redirect(url_for("learn_lesson", slug=slug, week=week))

        if new_completed >= total_steps:
            flash("Bạn đã hoàn thành toàn bộ bài học. Hãy làm quiz cuối khóa.", "success")
            return redirect(url_for("learn_quiz", slug=slug))
        flash("Nộp bài thành công và đã mở khóa bài tiếp theo.", "success")
        return redirect(url_for("learn_lesson", slug=slug, week=new_completed + 1))

    @app.route("/learn/<slug>/progress", methods=["POST"])
    @login_required
    def update_learning_progress(slug: str):
        course = get_course(slug)
        if not course:
            return render_template("404.html"), 404
        if not user_has_enrollment(g.current_user["id"], slug):
            flash("Bạn cần mua khóa học trước khi cập nhật tiến độ.", "error")
            return redirect(url_for("course_detail", slug=slug))

        action = request.form.get("action", "next")
        total_steps = len(build_learning_units(course))
        progress = get_learning_progress(g.current_user["id"], slug)
        completed_steps = progress["completed_steps"]

        if action == "next":
            completed_steps = min(completed_steps + 1, total_steps)
        elif action == "reset":
            completed_steps = 0

        db = get_db()
        with db:
            db.execute(
                """
                UPDATE learning_progress
                SET completed_steps = ?, completed_lessons = ?, updated_at = ?
                WHERE user_id = ? AND course_slug = ?
                """,
                (
                    completed_steps,
                    completed_steps,
                    datetime.now().isoformat(timespec="seconds"),
                    g.current_user["id"],
                    slug,
                ),
            )

        flash("Đã cập nhật tiến độ học tập.", "success")
        return redirect(url_for("learn_course", slug=slug))

    @app.route("/learn/<slug>/quiz", methods=["GET", "POST"])
    @login_required
    def learn_quiz(slug: str):
        course = get_course(slug)
        if not course:
            return render_template("404.html"), 404
        if not user_has_enrollment(g.current_user["id"], slug):
            flash("Bạn cần mua khóa học trước khi làm quiz.", "error")
            return redirect(url_for("course_detail", slug=slug))

        units = build_learning_units(course)
        progress = get_learning_progress(g.current_user["id"], slug)
        if progress["completed_steps"] < len(units):
            flash("Bạn cần hoàn thành tất cả bài học trước khi làm quiz.", "error")
            return redirect(url_for("learn_course", slug=slug))

        questions = build_quiz_questions(course)
        if request.method == "POST":
            score = 0
            for q in questions:
                answer = request.form.get(q["id"], "-1")
                if answer.isdigit() and int(answer) == q["answer"]:
                    score += 1
            percent = int((score / len(questions)) * 100)
            passed = percent >= 70

            db = get_db()
            with db:
                db.execute(
                    """
                    UPDATE learning_progress
                    SET quiz_score = ?, quiz_passed = ?, certificate_issued = ?, updated_at = ?
                    WHERE user_id = ? AND course_slug = ?
                    """,
                    (
                        percent,
                        1 if passed else 0,
                        1 if passed else 0,
                        datetime.now().isoformat(timespec="seconds"),
                        g.current_user["id"],
                        slug,
                    ),
                )
            if passed:
                flash("Chúc mừng! Bạn đã vượt qua quiz cuối khóa và nhận chứng nhận.", "success")
                return redirect(url_for("course_certificate", slug=slug))
            flash("Bạn chưa đạt quiz (cần >= 70%). Hãy ôn lại và thử lại.", "error")
            return redirect(url_for("learn_quiz", slug=slug))

        return render_template(
            "learn_quiz.html",
            course=course,
            questions=questions,
            previous_score=progress["quiz_score"],
        )

    @app.route("/learn/<slug>/certificate")
    @login_required
    def course_certificate(slug: str):
        course = get_course(slug)
        if not course:
            return render_template("404.html"), 404
        if not user_has_enrollment(g.current_user["id"], slug):
            flash("Bạn cần mua khóa học trước khi xem chứng nhận.", "error")
            return redirect(url_for("course_detail", slug=slug))

        progress = get_learning_progress(g.current_user["id"], slug)
        if not progress["quiz_passed"]:
            flash("Bạn cần vượt qua quiz cuối khóa để nhận chứng nhận.", "error")
            return redirect(url_for("learn_quiz", slug=slug))

        return render_template(
            "certificate.html",
            course=course,
            learner_name=g.current_user["full_name"],
            learner_username=g.current_user["username"],
            score=progress["quiz_score"],
            issue_date=datetime.now().strftime("%d/%m/%Y"),
        )

    @app.route("/register", methods=["GET", "POST"])
    def register():
        if g.current_user:
            return redirect(url_for("dashboard"))

        if request.method == "POST":
            full_name = request.form.get("full_name", "").strip()
            email = request.form.get("email", "").strip().lower()
            username = request.form.get("username", "").strip().lower()
            password = request.form.get("password", "")

            email_valid = re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email)
            username_valid = re.fullmatch(r"[a-z0-9_]{3,30}", username)
            if len(full_name) < 2 or not email_valid or not username_valid or len(password) < 6:
                flash("Thông tin chưa hợp lệ. Mật khẩu cần tối thiểu 6 ký tự.", "error")
                return render_template(
                    "register.html",
                    form={"full_name": full_name, "email": email, "username": username},
                )

            db = get_db()
            exists = db.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
            if exists:
                flash("Email đã tồn tại. Vui lòng dùng email khác.", "error")
                return render_template(
                    "register.html",
                    form={"full_name": full_name, "email": email, "username": username},
                )
            username_exists = db.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
            if username_exists:
                flash("Username đã tồn tại. Vui lòng chọn username khác.", "error")
                return render_template(
                    "register.html",
                    form={"full_name": full_name, "email": email, "username": username},
                )

            db.execute(
                """
                INSERT INTO users (full_name, email, username, password_hash, role, balance, created_at)
                VALUES (?, ?, ?, ?, 'member', 0, ?)
                """,
                (
                    full_name,
                    email,
                    username,
                    generate_password_hash(password),
                    datetime.now().isoformat(timespec="seconds"),
                ),
            )
            db.commit()
            flash("Tạo tài khoản thành công. Vui lòng đăng nhập.", "success")
            return redirect(url_for("login"))

        return render_template("register.html", form={"full_name": "", "email": "", "username": ""})

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if g.current_user:
            return redirect(url_for("dashboard"))

        if request.method == "POST":
            email = request.form.get("email", "").strip().lower()
            password = request.form.get("password", "")

            db = get_db()
            user = db.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
            if not user or not check_password_hash(user["password_hash"], password):
                flash("Email hoặc mật khẩu không đúng.", "error")
                return render_template("login.html", form={"email": email})

            session.clear()
            session["user_id"] = user["id"]
            flash(f"Xin chào, {user['full_name']}.", "success")
            next_url = request.args.get("next")
            if next_url and next_url.startswith("/"):
                return redirect(next_url)
            return redirect(url_for("dashboard"))

        return render_template("login.html", form={"email": ""})

    @app.route("/logout", methods=["POST"])
    @login_required
    def logout():
        session.clear()
        flash("Bạn đã đăng xuất.", "success")
        return redirect(url_for("home"))

    @app.route("/dashboard")
    @login_required
    def dashboard():
        db = get_db()
        tx_type = request.args.get("tx_type", "all").strip().lower()
        valid_tx_types = {
            "all",
            "topup",
            "purchase_course",
            "transfer_in",
            "transfer_out",
            "admin_adjust_balance",
        }
        if tx_type not in valid_tx_types:
            tx_type = "all"

        enrollments = db.execute(
            """
            SELECT course_slug, created_at FROM enrollments
            WHERE user_id = ?
            ORDER BY created_at DESC
            """,
            (g.current_user["id"],),
        ).fetchall()
        enrolled_courses = [
            {
                "course": get_course(item["course_slug"]),
                "created_at": item["created_at"],
            }
            for item in enrollments
            if get_course(item["course_slug"])
        ]
        if tx_type == "all":
            transactions = db.execute(
                """
                SELECT tx_type, amount, note, balance_after, created_at
                FROM transactions
                WHERE user_id = ?
                ORDER BY id DESC
                LIMIT 20
                """,
                (g.current_user["id"],),
            ).fetchall()
        else:
            transactions = db.execute(
                """
                SELECT tx_type, amount, note, balance_after, created_at
                FROM transactions
                WHERE user_id = ? AND tx_type = ?
                ORDER BY id DESC
                LIMIT 20
                """,
                (g.current_user["id"], tx_type),
            ).fetchall()
        return render_template(
            "dashboard.html",
            enrolled_courses=enrolled_courses,
            transactions=transactions,
            momo_phone=momo_phone,
            momo_owner=momo_owner,
            username_hint=g.current_user["username"],
            tx_type=tx_type,
        )

    @app.route("/profile", methods=["GET", "POST"])
    @login_required
    def profile():
        if request.method == "POST":
            full_name = request.form.get("full_name", "").strip()
            current_password = request.form.get("current_password", "")
            new_password = request.form.get("new_password", "")

            if len(full_name) < 2:
                flash("Họ tên phải có ít nhất 2 ký tự.", "error")
                return redirect(url_for("profile"))

            db = get_db()
            user_row = db.execute(
                "SELECT id, password_hash FROM users WHERE id = ?",
                (g.current_user["id"],),
            ).fetchone()
            if not user_row:
                flash("Không tìm thấy tài khoản.", "error")
                return redirect(url_for("dashboard"))

            if new_password:
                if len(new_password) < 6:
                    flash("Mật khẩu mới phải có ít nhất 6 ký tự.", "error")
                    return redirect(url_for("profile"))
                if not current_password or not check_password_hash(user_row["password_hash"], current_password):
                    flash("Mật khẩu hiện tại không đúng.", "error")
                    return redirect(url_for("profile"))

            with db:
                db.execute("UPDATE users SET full_name = ? WHERE id = ?", (full_name, g.current_user["id"]))
                if new_password:
                    db.execute(
                        "UPDATE users SET password_hash = ? WHERE id = ?",
                        (generate_password_hash(new_password), g.current_user["id"]),
                    )
            flash("Cập nhật hồ sơ thành công.", "success")
            return redirect(url_for("profile"))

        return render_template("profile.html")

    @app.route("/wallet/topup", methods=["POST"])
    @login_required
    def wallet_topup():
        if g.current_user["role"] != "admin":
            flash(
                "Thành viên nạp tiền qua MoMo 0375.595.019 (Bạch Sỹ Khang), nội dung: napaisk username so_tien.",
                "error",
            )
            return redirect(url_for("dashboard"))

        amount_str = request.form.get("amount", "0").strip()
        if not amount_str.isdigit():
            flash("Số tiền nạp không hợp lệ.", "error")
            return redirect(url_for("dashboard"))

        amount = int(amount_str)
        if amount < 10000 or amount > 50000000:
            flash("Số tiền nạp phải từ 10.000đ đến 50.000.000đ.", "error")
            return redirect(url_for("dashboard"))

        db = get_db()
        with db:
            db.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (amount, g.current_user["id"]))
            log_transaction(db, g.current_user["id"], "topup", amount, "Nạp tiền vào ví")
        flash(f"Nạp tiền thành công: {amount:,.0f}đ", "success")
        return redirect(url_for("dashboard"))

    @app.route("/enroll", methods=["POST"])
    @login_required
    def enroll():
        course_slug = request.form.get("course_slug", "").strip()
        course = get_course(course_slug)
        if not course:
            flash("Khóa học không tồn tại.", "error")
            return redirect(url_for("course_list"))

        db = get_db()
        existing = db.execute(
            "SELECT id FROM enrollments WHERE user_id = ? AND course_slug = ?",
            (g.current_user["id"], course_slug),
        ).fetchone()
        if existing:
            flash("Bạn đã mua khóa học này trước đó. Chuyển đến trang học.", "success")
            return redirect(url_for("learn_course", slug=course_slug))

        course_price = parse_price_vnd(course["price"])
        if course_price <= 0:
            flash("Không thể mua khóa học do giá khóa học không hợp lệ.", "error")
            return redirect(url_for("course_detail", slug=course_slug))

        try:
            with db:
                updated = db.execute(
                    """
                    UPDATE users
                    SET balance = balance - ?
                    WHERE id = ? AND balance >= ?
                    """,
                    (course_price, g.current_user["id"], course_price),
                ).rowcount
                if updated == 0:
                    flash("Số dư không đủ để mua khóa học này.", "error")
                    return redirect(url_for("course_detail", slug=course_slug))

                db.execute(
                    "INSERT INTO enrollments (user_id, course_slug, created_at) VALUES (?, ?, ?)",
                    (g.current_user["id"], course_slug, datetime.now().isoformat(timespec="seconds")),
                )
                db.execute(
                    """
                    INSERT INTO learning_progress (user_id, course_slug, completed_steps, updated_at)
                    VALUES (?, ?, 0, ?)
                    ON CONFLICT(user_id, course_slug)
                    DO NOTHING
                    """,
                    (g.current_user["id"], course_slug, datetime.now().isoformat(timespec="seconds")),
                )
                log_transaction(
                    db,
                    g.current_user["id"],
                    "purchase_course",
                    -course_price,
                    f"Mua khóa học: {course['title']}",
                )
        except sqlite3.IntegrityError:
            flash("Bạn đã đăng ký khóa học này trước đó.", "error")
            return redirect(url_for("course_detail", slug=course_slug))

        flash(f"Mua thành công khóa {course['title']} với giá {course['price']}.", "success")
        return redirect(url_for("learn_course", slug=course_slug))

    @app.route("/admin/users")
    @login_required
    @admin_required
    def admin_users():
        db = get_db()
        users = db.execute(
            "SELECT id, full_name, username, email, role, balance, created_at FROM users ORDER BY id DESC"
        ).fetchall()
        return render_template("admin_users.html", users=users)

    @app.route("/admin/overview")
    @login_required
    @admin_required
    def admin_overview():
        db = get_db()
        total_users = db.execute("SELECT COUNT(*) AS total FROM users").fetchone()["total"]
        member_count = db.execute("SELECT COUNT(*) AS total FROM users WHERE role = 'member'").fetchone()["total"]
        admin_count = db.execute("SELECT COUNT(*) AS total FROM users WHERE role = 'admin'").fetchone()["total"]
        total_balance = db.execute("SELECT COALESCE(SUM(balance), 0) AS total FROM users").fetchone()["total"]
        total_revenue = (
            db.execute(
                "SELECT COALESCE(SUM(amount), 0) AS total FROM transactions WHERE tx_type = 'purchase_course'"
            ).fetchone()["total"]
            * -1
        )
        recent_transactions = db.execute(
            """
            SELECT t.tx_type, t.amount, t.note, t.created_at, u.username
            FROM transactions t
            JOIN users u ON u.id = t.user_id
            ORDER BY t.id DESC
            LIMIT 8
            """
        ).fetchall()
        return render_template(
            "admin_overview.html",
            total_users=total_users,
            member_count=member_count,
            admin_count=admin_count,
            total_balance=total_balance,
            total_revenue=total_revenue,
            recent_transactions=recent_transactions,
        )

    @app.route("/admin/users/<int:user_id>/update", methods=["POST"])
    @login_required
    @admin_required
    def admin_update_user(user_id: int):
        full_name = request.form.get("full_name", "").strip()
        role = request.form.get("role", "member")
        balance_str = request.form.get("balance", "").strip()

        if len(full_name) < 2:
            flash("Tên người dùng không hợp lệ.", "error")
            return redirect(url_for("admin_users"))
        if role not in {"member", "admin"}:
            flash("Vai trò không hợp lệ.", "error")
            return redirect(url_for("admin_users"))
        if not re.fullmatch(r"\d+", balance_str):
            flash("Số dư phải là số nguyên không âm.", "error")
            return redirect(url_for("admin_users"))

        balance = int(balance_str)
        db = get_db()
        target = db.execute("SELECT id, balance FROM users WHERE id = ?", (user_id,)).fetchone()
        if not target:
            flash("Không tìm thấy tài khoản cần sửa.", "error")
            return redirect(url_for("admin_users"))

        old_balance = target["balance"]
        with db:
            db.execute(
                "UPDATE users SET full_name = ?, role = ?, balance = ? WHERE id = ?",
                (full_name, role, balance, user_id),
            )
            delta = balance - old_balance
            if delta != 0:
                log_transaction(
                    db,
                    user_id,
                    "admin_adjust_balance",
                    delta,
                    f"Admin cập nhật số dư bởi tài khoản ID {g.current_user['id']}",
                )
        flash("Đã cập nhật thông tin tài khoản.", "success")
        return redirect(url_for("admin_users"))

    @app.route("/admin/transfer", methods=["POST"])
    @login_required
    @admin_required
    def admin_transfer():
        to_user_id = request.form.get("to_user_id", "").strip()
        amount_str = request.form.get("amount", "").strip()

        if not re.fullmatch(r"\d+", to_user_id) or not re.fullmatch(r"\d+", amount_str):
            flash("Thông tin chuyển tiền không hợp lệ.", "error")
            return redirect(url_for("admin_users"))

        target_id = int(to_user_id)
        amount = int(amount_str)
        if amount <= 0:
            flash("Số tiền chuyển phải lớn hơn 0.", "error")
            return redirect(url_for("admin_users"))
        if target_id == g.current_user["id"]:
            flash("Không thể tự chuyển tiền cho chính tài khoản admin.", "error")
            return redirect(url_for("admin_users"))

        db = get_db()
        admin_user = db.execute("SELECT id, balance FROM users WHERE id = ?", (g.current_user["id"],)).fetchone()
        target_user = db.execute("SELECT id FROM users WHERE id = ?", (target_id,)).fetchone()
        if not target_user:
            flash("Tài khoản nhận không tồn tại.", "error")
            return redirect(url_for("admin_users"))

        if admin_user["balance"] < amount:
            flash("Số dư admin không đủ để chuyển.", "error")
            return redirect(url_for("admin_users"))

        with db:
            db.execute("UPDATE users SET balance = balance - ? WHERE id = ?", (amount, g.current_user["id"]))
            db.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (amount, target_id))
            log_transaction(
                db,
                g.current_user["id"],
                "transfer_out",
                -amount,
                f"Chuyển tiền đến tài khoản ID {target_id}",
            )
            log_transaction(
                db,
                target_id,
                "transfer_in",
                amount,
                f"Nhận tiền từ admin ID {g.current_user['id']}",
            )
        flash(f"Chuyển {amount:,.0f}đ thành công đến tài khoản ID {target_id}.", "success")
        return redirect(url_for("admin_users"))

    @app.route("/about")
    def about():
        db = get_db()
        enrollment_count = db.execute("SELECT COUNT(*) AS total FROM enrollments").fetchone()["total"]
        member_count = db.execute("SELECT COUNT(*) AS total FROM users WHERE role = 'member'").fetchone()["total"]
        return render_template(
            "about.html",
            enrollment_count=enrollment_count,
            member_count=member_count,
        )

    return app


if __name__ == "__main__":
    create_app().run(debug=True)

