from flask import Blueprint, current_app, render_template, render_template_string, request, make_response

landing_bp = Blueprint("landing", __name__)


def _render_scorer_html():
    scorer_service = current_app.extensions["scorer_service"]
    template = scorer_service.template_source()
    context = scorer_service.build_context()
    return render_template_string(template, **context)


def _scorer_response(download: bool = False):
    scorer_service = current_app.extensions["scorer_service"]
    html = _render_scorer_html()
    response = make_response(html)
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    if download:
        response.headers["Content-Disposition"] = f'attachment; filename="{scorer_service.download_filename()}"'
    return response


def _season_rank(season):
    slug = (season.get("slug") or "").strip().lower()
    if not slug:
        return (-1, "")

    parts = slug.split("-")
    for part in reversed(parts):
        if part.isdigit():
            return (int(part), slug)

    return (-1, slug)


@landing_bp.get("/")
def index():
    fantasy_service = current_app.extensions["fantasy_service"]
    seasons = fantasy_service.list_published_sessions()

    selected_season = (request.args.get("season") or "").strip().lower()
    valid_slugs = {season.get("slug") for season in seasons}
    if selected_season not in valid_slugs:
        highest_season = max(seasons, key=_season_rank, default=None)
        selected_season = (highest_season or {}).get("slug", "")

    return render_template(
        "landing.html",
        seasons=seasons,
        selected_season=selected_season,
    )


@landing_bp.get("/scorer")
def scorer():
    download = (request.args.get("download") or "").strip().lower() in {"1", "true", "yes", "on"}
    if download:
        return _scorer_response(download=True)
    return _scorer_response(download=False)


@landing_bp.get("/scorer/download")
def scorer_download():
    return _scorer_response(download=True)
