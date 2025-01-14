# coding=utf-8
import re
from re import match

from mapi.exceptions import (
    MapiNetworkException,
    MapiNotFoundException,
    MapiProviderException,
)
from mapi.utils import clean_dict, request_json

__all__ = [
    "omdb_search",
    "omdb_title",
    "tmdb_find",
    "tmdb_movies_or_series",
    "tmdb_search",
    "tvdb_episodes_id",
    "tvdb_login",
    "tvdb_refresh_token",
    "tvdb_search_series",
    "tvdb_series_id",
    "tvdb_series_id_episodes",
    "tvdb_series_id_episodes_query",
    "tvdbv4_login",
    "tvdbv4_search",
    "imdb_suggestion",
]

OMDB_MEDIA_TYPES = {"episode", "movie", "series"}
OMDB_PLOT_TYPES = {"short", "long"}
TVDB_LANGUAGE_CODES = [
    "cs",
    "da",
    "de",
    "el",
    "en",
    "es",
    "fi",
    "fr",
    "he",
    "hr",
    "hu",
    "it",
    "ja",
    "ko",
    "nl",
    "no",
    "pl",
    "pt",
    "ru",
    "sl",
    "sv",
    "tr",
    "zh",
    "ar",
]


def omdb_title(
        api_key,
        id_imdb=None,
        media_type=None,
        title=None,
        season=None,
        episode=None,
        year=None,
        plot=None,
        cache=True,
):
    """
    Lookup media using the Open Movie Database.

    Online docs: http://www.omdbapi.com/#parameters
    """
    if (not title and not id_imdb) or (title and id_imdb):
        raise MapiProviderException("either id_imdb or title must be specified")
    elif media_type and media_type not in OMDB_MEDIA_TYPES:
        raise MapiProviderException(
            "media_type must be one of %s" % ",".join(OMDB_MEDIA_TYPES)
        )
    elif plot and plot not in OMDB_PLOT_TYPES:
        raise MapiProviderException(
            "plot must be one of %s" % ",".join(OMDB_PLOT_TYPES)
        )
    url = "http://www.omdbapi.com"
    parameters = {
        "apikey": api_key,
        "i": id_imdb,
        "t": title,
        "y": year,
        "season": season,
        "episode": episode,
        "type": media_type,
        "plot": plot,
    }
    parameters = clean_dict(parameters)
    status, content = request_json(url, parameters, cache=cache)
    error = content.get("Error") if isinstance(content, dict) else None
    if status == 401:
        raise MapiProviderException("invalid API key")
    elif status != 200 or not isinstance(content, dict):
        raise MapiNetworkException("OMDb down or unavailable?")
    elif error:
        raise MapiNotFoundException(error)
    return content


def omdb_search(api_key, query, year=None, media_type=None, page=1, cache=True):
    """
    Search for media using the Open Movie Database.

    Online docs: http://www.omdbapi.com/#parameters.
    """
    if media_type and media_type not in OMDB_MEDIA_TYPES:
        raise MapiProviderException(
            "media_type must be one of %s" % ",".join(OMDB_MEDIA_TYPES)
        )
    if 1 > page > 100:
        raise MapiProviderException("page must be between 1 and 100")
    url = "http://www.omdbapi.com"
    parameters = {
        "apikey": api_key,
        "s": query,
        "y": year,
        "type": media_type,
        "page": page,
    }
    parameters = clean_dict(parameters)
    status, content = request_json(url, parameters, cache=cache)
    if status == 401:
        raise MapiProviderException("invalid API key")
    elif content and not content.get("totalResults"):
        raise MapiNotFoundException()
    elif not content or status != 200:  # pragma: no cover
        raise MapiNetworkException("OMDb down or unavailable?")
    return content


def tmdb_find(
        api_key, external_source, external_id, language="en-US", cache=True
):
    """
    Search for The Movie Database objects using another DB's foreign key.

    Note: language codes aren't checked on this end or by TMDb, so if you
        enter an invalid language code your search itself will succeed, but
        certain fields like synopsis will just be empty.

    Online docs: developers.themoviedb.org/3/find.
    """
    sources = ["imdb_id", "freebase_mid", "freebase_id", "tvdb_id", "tvrage_id", "facebook_id", "twitter_id", "instagram_id", "tiktok_id", "wikidata_id", "youtube_id"]
    if external_source not in sources:
        raise MapiProviderException("external_source must be in %s" % sources)
    if external_source == "imdb_id" and not match(r"tt\d+", external_id):
        raise MapiProviderException("invalid imdb tt-const value")
    url = "https://api.themoviedb.org/3/find/" + external_id or ""
    parameters = {
        "api_key": api_key,
        "external_source": external_source,
        "language": language,
    }
    keys = [
        "movie_results",
        "person_results",
        "tv_episode_results",
        "tv_results",
        "tv_season_results",
    ]
    status, content = request_json(url, parameters, cache=cache)
    if status == 401:
        raise MapiProviderException("invalid API key")
    elif status != 200 or not any(content.keys()):  # pragma: no cover
        raise MapiNetworkException("TMDb down or unavailable?")
    elif status == 404 or not any(content.get(k, {}) for k in keys):
        raise MapiNotFoundException
    return content


def tmdb_movies_or_series(api_key, id_tmdb, kind='movie', language="en-US", cache=True):
    """
    Lookup a movie item using The Movie Database.

    Online docs: developers.themoviedb.org/3/movies.
    """
    try:
        if kind == 'movie':
            url = "https://api.themoviedb.org/3/movie/%d" % int(id_tmdb)
        else:
            url = "https://api.themoviedb.org/3/tv/%d" % int(id_tmdb)
    except ValueError:
        raise MapiProviderException("id_tmdb must be numeric")
    parameters = {"api_key": api_key, "language": language,
                  "append_to_response": "external_ids,translations,alternative_titles"}
    status, content = request_json(url, parameters, cache=cache)
    if status == 401:
        raise MapiProviderException("invalid API key")
    elif status == 404:
        raise MapiNotFoundException
    elif status != 200 or not any(content.keys()):  # pragma: no cover
        raise MapiNetworkException("TMDb down or unavailable?")
    return content


def tmdb_search(
        api_key, title, year=None, kind=None, adult=False, region=None, page=1, cache=True
):
    """
    Search for movies using The Movie Database.

    Online docs: developers.themoviedb.org/3/search/search-movies.
    """
    url = "https://api.themoviedb.org/3/search/movie" if not kind or kind == "movie" else "https://api.themoviedb.org/3/search/tv"
    try:
        if year:
            year = int(year)
    except ValueError:
        raise MapiProviderException("year must be numeric")
    parameters = {
        "api_key": api_key,
        "query": title,
        "page": page,
        "include_adult": adult,
        "region": region,
        "year": year,
        "append_to_response": "external_ids,translations,alternative_titles",
    }
    status, content = request_json(url, parameters, cache=cache)
    if status == 401:
        raise MapiProviderException("invalid API key")
    elif status != 200 or not any(content.keys()):  # pragma: no cover
        raise MapiNetworkException("TMDb down or unavailable?")
    elif status == 404 or status == 422 or not content.get("total_results"):
        raise MapiNotFoundException
    return content


def tvdb_login(api_key):
    """
    Logs into TVDb using the provided api key.

    Note: You can register for a free TVDb key at thetvdb.com/?tab=apiregister
    Online docs: api.thetvdb.com/swagger#!/Authentication/post_login.
    """
    url = "https://api.thetvdb.com/login"
    body = {"apikey": api_key}
    status, content = request_json(url, body=body, cache=False)
    if status == 401:
        raise MapiProviderException("invalid api key")
    elif status != 200 or not content.get("token"):  # pragma: no cover
        raise MapiNetworkException("TVDb down or unavailable?")
    return content["token"]


def tvdb_refresh_token(token):
    """
    Refreshes JWT token.

    Online docs: api.thetvdb.com/swagger#!/Authentication/get_refresh_token.
    """
    url = "https://api.thetvdb.com/refresh_token"
    headers = {"Authorization": "Bearer %s" % token}
    status, content = request_json(url, headers=headers, cache=False)
    if status == 401:
        raise MapiProviderException("invalid token")
    elif status != 200 or not content.get("token"):  # pragma: no cover
        raise MapiNetworkException("TVDb down or unavailable?")
    return content["token"]


def tvdb_episodes_id(token, id_tvdb, lang="en", cache=True):
    """
    Returns the full information for a given episode id.

    Online docs: https://api.thetvdb.com/swagger#!/Episodes.
    """
    if lang not in TVDB_LANGUAGE_CODES:
        raise MapiProviderException(
            "'lang' must be one of %s" % ",".join(TVDB_LANGUAGE_CODES)
        )
    try:
        url = "https://api.thetvdb.com/episodes/%d" % int(id_tvdb)
    except ValueError:
        raise MapiProviderException("id_tvdb must be numeric")
    headers = {"Accept-Language": lang, "Authorization": "Bearer %s" % token}
    status, content = request_json(url, headers=headers, cache=cache)
    if status == 401:
        raise MapiProviderException("invalid token")
    elif status == 404:
        raise MapiNotFoundException
    elif status == 200 and "invalidLanguage" in content.get("errors", {}):
        raise MapiNotFoundException
    elif status != 200 or not content.get("data"):  # pragma: no cover
        raise MapiNetworkException("TVDb down or unavailable?")
    return content


def tvdb_series_id(token, id_tvdb, lang="en", cache=True):
    """
    Returns a series records that contains all information known about a
    particular series id.

    Online docs: api.thetvdb.com/swagger#!/Series/get_series_id.
    """
    if lang not in TVDB_LANGUAGE_CODES:
        raise MapiProviderException(
            "'lang' must be one of %s" % ",".join(TVDB_LANGUAGE_CODES)
        )
    try:
        url = "https://api.thetvdb.com/series/%d" % int(id_tvdb)
    except ValueError:
        raise MapiProviderException("id_tvdb must be numeric")
    headers = {"Accept-Language": lang, "Authorization": "Bearer %s" % token}
    status, content = request_json(url, headers=headers, cache=cache)
    if status == 401:
        raise MapiProviderException("invalid token")
    elif status == 404:
        raise MapiNotFoundException
    elif status != 200 or not content.get("data"):  # pragma: no cover
        raise MapiNetworkException("TVDb down or unavailable?")
    return content


def tvdb_series_id_episodes(token, id_tvdb, page=1, lang="en", cache=True):
    """
    All episodes for a given series.

    Note: Paginated with 100 results per page.
    Online docs: api.thetvdb.com/swagger#!/Series/get_series_id_episodes.
    """
    if lang not in TVDB_LANGUAGE_CODES:
        raise MapiProviderException(
            "'lang' must be one of %s" % ",".join(TVDB_LANGUAGE_CODES)
        )
    try:
        url = "https://api.thetvdb.com/series/%d/episodes" % int(id_tvdb)
    except ValueError:
        raise MapiProviderException("id_tvdb must be numeric")
    headers = {"Accept-Language": lang, "Authorization": "Bearer %s" % token}
    parameters = {"page": page}
    status, content = request_json(
        url, parameters, headers=headers, cache=cache
    )
    if status == 401:
        raise MapiProviderException("invalid token")
    elif status == 404:
        raise MapiNotFoundException
    elif status != 200 or not content.get("data"):  # pragma: no cover
        raise MapiNetworkException("TVDb down or unavailable?")
    return content


def tvdb_series_id_episodes_query(
        token, id_tvdb, episode=None, season=None, page=1, lang="en", cache=True
):
    """
    Allows the user to query against episodes for the given series.

    Note: Paginated with 100 results per page; omitted imdbId-- when would you
    ever need to query against both tvdb and imdb series ids?
    Online docs: api.thetvdb.com/swagger#!/Series/get_series_id_episodes_query.
    """
    if lang not in TVDB_LANGUAGE_CODES:
        raise MapiProviderException(
            "'lang' must be one of %s" % ",".join(TVDB_LANGUAGE_CODES)
        )
    try:
        url = "https://api.thetvdb.com/series/%d/episodes/query" % int(id_tvdb)
    except ValueError:
        raise MapiProviderException("id_tvdb must be numeric")
    headers = {"Accept-Language": lang, "Authorization": "Bearer %s" % token}
    parameters = {"airedSeason": season, "airedEpisode": episode, "page": page}
    status, content = request_json(
        url, parameters, headers=headers, cache=cache
    )
    if status == 401:
        raise MapiProviderException("invalid token")
    elif status == 404:
        raise MapiNotFoundException
    elif status != 200 or not content.get("data"):  # pragma: no cover
        raise MapiNetworkException("TVDb down or unavailable?")
    return content


def tvdb_search_series(
        token, series=None, id_imdb=None, id_zap2it=None, lang="en", cache=True
):
    """
    Allows the user to search for a series based on the following parameters.

    Online docs: https://api.thetvdb.com/swagger#!/Search/get_search_series
    Note: results a maximum of 100 entries per page, no option for pagination.
    """
    if lang not in TVDB_LANGUAGE_CODES:
        raise MapiProviderException(
            "'lang' must be one of %s" % ",".join(TVDB_LANGUAGE_CODES)
        )
    url = "https://api.thetvdb.com/search/series"
    parameters = {"name": series, "imdbId": id_imdb, "zap2itId": id_zap2it}
    headers = {"Accept-Language": lang, "Authorization": "Bearer %s" % token}
    status, content = request_json(
        url, parameters, headers=headers, cache=cache
    )
    if status == 401:
        raise MapiProviderException("invalid token")
    elif status == 405:
        raise MapiProviderException(
            "series, id_imdb, id_zap2it parameters are mutually exclusive"
        )
    elif status == 404:  # pragma: no cover
        raise MapiNotFoundException
    elif status != 200 or not content.get("data"):  # pragma: no cover
        raise MapiNetworkException("TVDb down or unavailable?")
    return content


def tvdbv4_login(api_key, pin):
    """
    Logs into TVDb using the provided api key.

    Note: You can register for a free TVDb key at thetvdb.com/?tab=apiregister
    Online docs: api.thetvdb.com/swagger#!/Authentication/post_login.
    """
    url = "https://api4.thetvdb.com/v4/login"
    body = {"apikey": api_key, "pin": pin}
    status, content = request_json(url, body=body, cache=False)
    if status == 401:
        raise MapiProviderException("invalid api key and pin")
    elif status != 200 or not content.get("data") or not content.get("data").get("token"):  # pragma: no cover
        raise MapiNetworkException("TVDb down or unavailable?")
    return content.get("data").get("token")


def tvdbv4_search(
        token, query, kind=None, year=None, lang="en", cache=True
):
    """
    Allows the user to search for a series, movies, people, or companies.

    Online docs: https://thetvdb.github.io/v4-api/#/Search/
    Note: results a maximum of 100 entries per page, no option for pagination.
    """
    if lang not in TVDB_LANGUAGE_CODES:
        raise MapiProviderException(
            "'lang' must be one of %s" % ",".join(TVDB_LANGUAGE_CODES)
        )
    url = "https://api4.thetvdb.com/v4/search"
    parameters = {"query": query}
    if kind:
        parameters["kind"] = kind
    if year:
        parameters["year"] = year
    headers = {"Accept-Language": lang, "Authorization": "Bearer %s" % token}
    status, content = request_json(
        url, parameters, headers=headers, cache=cache
    )
    if status == 401:
        raise MapiProviderException("invalid token")
    elif status == 405:
        raise MapiProviderException(
            "series, id_imdb, id_zap2it parameters are mutually exclusive"
        )
    elif status == 404:  # pragma: no cover
        raise MapiNotFoundException
    elif status != 200 or not content.get("data"):  # pragma: no cover
        raise MapiNetworkException("TVDb down or unavailable?")
    return content


def imdb_suggestion(query, year=None, cache=True):
    """
    Search for meta from IMDB.com.

    """
    if not query:
        raise MapiProviderException("query must be specified")
    # 将query中的特殊字符替换为_，以便于在url中使用
    query = re.sub(r"[^a-zA-Z0-9]", "_", query)
    query = query.replace("__", "_")
    if year:
        query = "%s_%s" % (query, year)

    url = "https://v3.sg.media-imdb.com/suggestion/x/%s.json" % (query)   # ?includeVideos=1&limit=10
    status, content = request_json(url, cache=cache)
    if status != 200 or not isinstance(content, dict):
        raise MapiNetworkException("IMDb down or unavailable?")
    return content

if __name__ == '__main__':
    ctn = imdb_suggestion('The Matrix', 1999)
    print(ctn)