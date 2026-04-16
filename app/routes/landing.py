from flask import Blueprint, current_app, abort, render_template, render_template_string, request, make_response

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


@landing_bp.get("/matches")
def matches_home():
    scorer_service = current_app.extensions["scorer_service"]
    seasons = scorer_service.list_match_seasons()

    selected_season = (request.args.get("season") or "").strip().lower()
    valid_slugs = {season.get("slug") for season in seasons}
    if selected_season not in valid_slugs:
        selected_season = (seasons[0].get("slug") if seasons else "")

    matches = scorer_service.list_match_registry(season_slug=selected_season, limit=500) if selected_season else []
    return render_template(
        "matches/index.html",
        seasons=seasons,
        selected_season=selected_season,
        matches=matches,
    )


@landing_bp.get("/matches/<season_slug>")
def matches_by_season(season_slug):
    scorer_service = current_app.extensions["scorer_service"]
    safe_season_slug = (season_slug or "").strip().lower()
    seasons = scorer_service.list_match_seasons()
    matches = scorer_service.list_match_registry(season_slug=safe_season_slug, limit=500)
    return render_template(
        "matches/index.html",
        seasons=seasons,
        selected_season=safe_season_slug,
        matches=matches,
    )


@landing_bp.get("/matches/<season_slug>/<match_id>")
def match_summary(season_slug, match_id):
    scorer_service = current_app.extensions["scorer_service"]
    summary = scorer_service.get_match_summary(season_slug, match_id)
    if not summary:
        abort(404)

    return render_template("matches/summary.html", summary=summary)


@landing_bp.get("/table")
def league_table_home():
    scorer_service = current_app.extensions["scorer_service"]
    seasons = scorer_service.list_match_seasons()

    selected_season = (request.args.get("season") or "").strip().lower()
    valid_slugs = {season.get("slug") for season in seasons}
    if selected_season not in valid_slugs:
        selected_season = (seasons[0].get("slug") if seasons else "")

    standings = scorer_service.list_season_league_table(selected_season) if selected_season else []
    return render_template(
        "matches/table.html",
        seasons=seasons,
        selected_season=selected_season,
        standings=standings,
    )


@landing_bp.get("/table/<season_slug>")
def league_table_by_season(season_slug):
    scorer_service = current_app.extensions["scorer_service"]
    safe_season_slug = (season_slug or "").strip().lower()
    seasons = scorer_service.list_match_seasons()
    standings = scorer_service.list_season_league_table(safe_season_slug)
    return render_template(
        "matches/table.html",
        seasons=seasons,
        selected_season=safe_season_slug,
        standings=standings,
    )


@landing_bp.get("/finances")
def finances_home():
    scorer_service = current_app.extensions["scorer_service"]
    seasons = scorer_service.list_match_seasons()

    selected_season = (request.args.get("season") or "").strip().lower()
    valid_slugs = {season.get("slug") for season in seasons}
    if selected_season not in valid_slugs:
        selected_season = (seasons[0].get("slug") if seasons else "")

    finances = scorer_service.list_season_finances(selected_season) if selected_season else []
    transactions = scorer_service.list_season_finance_transactions(selected_season) if selected_season else []
    return render_template(
        "matches/finances.html",
        seasons=seasons,
        selected_season=selected_season,
        finances=finances,
        transactions=transactions,
    )


@landing_bp.get("/finances/<season_slug>")
def finances_by_season(season_slug):
    scorer_service = current_app.extensions["scorer_service"]
    safe_season_slug = (season_slug or "").strip().lower()
    seasons = scorer_service.list_match_seasons()
    finances = scorer_service.list_season_finances(safe_season_slug)
    transactions = scorer_service.list_season_finance_transactions(safe_season_slug)
    return render_template(
        "matches/finances.html",
        seasons=seasons,
        selected_season=safe_season_slug,
        finances=finances,
        transactions=transactions,
    )


@landing_bp.get("/teams")
def teams_home():
    scorer_service = current_app.extensions["scorer_service"]
    teams = scorer_service.list_global_teams_overview(limit=1000)
    return render_template("teams/index.html", teams=teams)


@landing_bp.get("/teams/<team_slug>")
def team_profile(team_slug):
    scorer_service = current_app.extensions["scorer_service"]
    profile = scorer_service.get_team_profile(team_slug)
    if not profile:
        abort(404)
    return render_template("teams/detail.html", profile=profile)


@landing_bp.get("/players/<player_slug>")
def player_profile(player_slug):
    scorer_service = current_app.extensions["scorer_service"]
    profile = scorer_service.get_player_profile(player_slug)
    if not profile:
        abort(404)
    return render_template("players/detail.html", profile=profile)
