import re
import json
import scrapy

ALGOLIA_SEARCH = (
    "https://hn.algolia.com/api/v1/search_by_date"
    "?tags=ask_hn&query=who+is+hiring&hitsPerPage=10"
)

ALGOLIA_COMMENTS = (
    "https://hn.algolia.com/api/v1/search_by_date"
    "?tags=comment,story_{story_id}&hitsPerPage=1000&page={page}"
)

# Canonical skill list — extend freely
KNOWN_SKILLS = [
    # Languages
    "python", "javascript", "typescript", "java", "kotlin", "scala",
    "go", "golang", "rust", "c++", "c#", "ruby", "php", "swift",
    "r", "matlab", "bash", "shell", "perl", "haskell", "elixir",
    
    "sql", "postgresql", "postgres", "mysql", "sqlite", "mongodb",
    "redis", "elasticsearch", "cassandra", "bigquery", "snowflake",
    "dbt", "airflow", "spark", "kafka", "hadoop", "flink",
    "pandas", "numpy", "scikit-learn", "sklearn", "tensorflow",
    "pytorch", "keras", "xgboost", "lightgbm", "mlflow",
    "hugging face", "openai", "langchain",
    
    "aws", "gcp", "azure", "docker", "kubernetes", "k8s",
    "terraform", "ansible", "jenkins", "github actions", "ci/cd",
    "linux", "nginx", "rabbitmq",
    
    "react", "vue", "angular", "next.js", "node.js", "django",
    "flask", "fastapi", "graphql", "rest", "grpc",
    
    "power bi", "tableau", "looker", "grafana", "metabase",
    
    "git", "jira", "agile", "scrum",
]

_SKILLS_RE = {
    skill: re.compile(r"\b" + re.escape(skill) + r"\b", re.IGNORECASE)
    for skill in KNOWN_SKILLS
}

_SALARY_PATTERNS = [
    # $120k–$160k  /  $120K - $160K
    re.compile(
        r"\$\s*(\d{2,3})[kK]\s*[-–—to]+\s*\$?\s*(\d{2,3})[kK]"
    ),
    # $120,000–$160,000
    re.compile(
        r"\$\s*([\d,]{5,7})\s*[-–—to]+\s*\$?\s*([\d,]{5,7})"
    ),
    # up to $150k  /  up to $150,000
    re.compile(
        r"up\s+to\s+\$\s*(\d{2,3})[kK]", re.IGNORECASE
    ),
    # $150k+  /  $150K+
    re.compile(r"\$\s*(\d{2,3})[kK]\+"),
    # single figure: $120k  /  $120,000
    re.compile(r"\$\s*(\d{2,3})[kK]"),
    re.compile(r"\$\s*([\d,]{5,7})(?!\d)"),
]

# Location / remote signals
_REMOTE_RE = re.compile(
    r"\b(remote|wfh|work[\s-]from[\s-]home|fully\s+remote|remote[\s-]first"
    r"|remote[\s-]friendly|distributed)\b",
    re.IGNORECASE,
)
_ONSITE_RE = re.compile(
    r"\b(on[\s-]?site|in[\s-]?office|onsite|no\s+remote)\b",
    re.IGNORECASE,
)

# Common job title keywords (first-pass heuristic)
_TITLE_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"(senior|sr\.?|junior|jr\.?|lead|staff|principal|head of)?\s*"
        r"(software|data|ml|machine learning|backend|frontend|full[\s-]?stack"
        r"|platform|devops|site reliability|sre|analytics|research)\s*"
        r"(engineer|developer|scientist|analyst|architect|manager|lead|intern)",
        r"(cto|cpo|vp of engineering|engineering manager|product manager|"
        r"data engineer|data analyst|data scientist|ml engineer|"
        r"devops engineer|solutions architect)",
    ]
]

# Location heuristic — city/country after REMOTE or | or location:
_LOCATION_RE = re.compile(
    r"(?:location|based in|located in|office in)[:\s]+([A-Za-z ,]+?)(?:\||$|\n|;)",
    re.IGNORECASE,
)

def _parse_salary(text: str):
    """
    Returns (salary_raw, salary_min, salary_max) or (None, None, None).
    All figures annualised to USD integers.
    """
    for pat in _SALARY_PATTERNS:
        m = pat.search(text)
        if not m:
            continue
        raw = m.group(0)
        groups = m.groups()
        nums = []
        for g in groups:
            if g is None:
                continue
            cleaned = g.replace(",", "")
            val = int(cleaned)
            # k-multiplier already in pattern group for k-suffixed patterns
            if val < 1000:
                val *= 1000
            nums.append(val)
        if len(nums) == 2:
            return raw, min(nums), max(nums)
        if len(nums) == 1:
            return raw, nums[0], None
    return None, None, None


def _extract_skills(text: str) -> list:
    found = sorted(
        {skill for skill, pat in _SKILLS_RE.items() if pat.search(text)}
    )
    return found


def _extract_title(text: str):
    for pat in _TITLE_PATTERNS:
        m = pat.search(text)
        if m:
            return m.group(0).strip()
    return None


def _extract_location(text: str):
    m = _LOCATION_RE.search(text)
    if m:
        return m.group(1).strip()
    # Fallback: look for city-like patterns after a pipe separator
    pipe_parts = [p.strip() for p in text.split("|")]
    for part in pipe_parts[1:4]:          # skip first (usually company/title)
        if 3 < len(part) < 40 and not part.startswith("http"):
            if re.search(r"[A-Z][a-z]", part):   # looks like a proper noun
                return part
    return None


def _extract_company(text: str):
    """
    HN hiring comments typically start with:
      'CompanyName | Role | Location | ...'
    or
      'CompanyName (http://...) | ...'
    We take the first pipe-delimited segment and clean it.
    """
    first_line = text.split("\n")[0]
    parts = first_line.split("|")
    if parts:
        candidate = re.sub(r"\(https?://\S+\)", "", parts[0]).strip()
        if 1 < len(candidate) < 60:
            return candidate
    return None


def _remote_flag(text: str):
    has_remote = bool(_REMOTE_RE.search(text))
    has_onsite = bool(_ONSITE_RE.search(text))
    if has_remote and not has_onsite:
        return True
    if has_onsite and not has_remote:
        return False
    return None     # ambiguous / not mentioned

class HNJobsSpider(scrapy.Spider):
    name = "hn_jobs"
    custom_settings = {
        "ROBOTSTXT_OBEY": False,          # HN Algolia API, no robots.txt
        "DOWNLOAD_DELAY": 1.0,            # be polite
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2,
        "USER_AGENT": (
            "JobScope-research-bot/1.0 "
            "(portfolio project; contact: your@email.com)"
        ),
        "FEEDS": {
            "data/hn_raw.json": {
                "format": "json",
                "encoding": "utf8",
                "overwrite": False,       # delta loads: append new runs
            }
        },
        "LOG_LEVEL": "INFO",
    }

    # Optional: pass via -a max_threads=1 to limit to the latest thread only
    def __init__(self, max_threads=3, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_threads = int(max_threads)

    def start_requests(self):
        """Find the most recent 'Who is hiring?' Ask HN stories."""
        yield scrapy.Request(
            url=ALGOLIA_SEARCH,
            callback=self.parse_thread_list,
        )

    def parse_thread_list(self, response):
        data = json.loads(response.text)
        hits = data.get("hits", [])
        seen = 0
        for hit in hits:
            title = hit.get("title", "")
            if "who is hiring" not in title.lower():
                continue
            story_id = hit.get("objectID")
            self.logger.info(f"Found thread: {title} (id={story_id})")
            yield scrapy.Request(
                url=ALGOLIA_COMMENTS.format(story_id=story_id, page=0),
                callback=self.parse_comments,
                meta={"story_id": story_id, "page": 0},
            )
            seen += 1
            if seen >= self.max_threads:
                break

    def parse_comments(self, response):
        data = json.loads(response.text)
        hits = data.get("hits", [])
        story_id = response.meta["story_id"]
        page = response.meta["page"]

        for hit in hits:
            text = hit.get("comment_text") or ""
            # Strip HTML tags for cleaner parsing
            text_clean = re.sub(r"<[^>]+>", " ", text).strip()
            text_clean = re.sub(r"\s+", " ", text_clean)

            if len(text_clean) < 40:        # skip very short / mod comments
                continue

            salary_raw, salary_min, salary_max = _parse_salary(text_clean)

            yield {
                "source":      "hn_who_is_hiring",
                "thread_id":   int(story_id),
                "comment_id":  int(hit.get("objectID", 0)),
                "author":      hit.get("author"),
                "posted_at":   hit.get("created_at"),
                "raw_text":    text_clean,
                "company":     _extract_company(text_clean),
                "job_title":   _extract_title(text_clean),
                "location":    _extract_location(text_clean),
                "remote":      _remote_flag(text_clean),
                "salary_raw":  salary_raw,
                "salary_min":  salary_min,
                "salary_max":  salary_max,
                "skills":      _extract_skills(text_clean),
                "url": (
                    f"https://news.ycombinator.com/item"
                    f"?id={hit.get('objectID')}"
                ),
            }

        # Paginate if there are more comments
        nb_hits = data.get("nbHits", 0)
        hits_per_page = data.get("hitsPerPage", 1000)
        if (page + 1) * hits_per_page < nb_hits:
            next_page = page + 1
            yield scrapy.Request(
                url=ALGOLIA_COMMENTS.format(
                    story_id=story_id, page=next_page
                ),
                callback=self.parse_comments,
                meta={"story_id": story_id, "page": next_page},
            )
