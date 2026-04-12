from flask import Blueprint, current_app, render_template, request

landing_bp = Blueprint("landing", __name__)


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
