# coding=utf-8

"""Provides a high-level interface for metadata media providers."""

import re
from abc import abstractmethod
from datetime import datetime as dt
from os import environ

from mapi import log
from mapi.compatibility import AbstractClass, ustr
from mapi.endpoints import *
from mapi.exceptions import (
    MapiException,
    MapiNotFoundException,
    MapiProviderException,
)
from mapi.metadata import *
from mapi.utils import year_expand

__all__ = [
    "API_ALL",
    "API_MOVIE",
    "API_TELEVISION",
    "OMDb",
    "Provider",
    "provider_factory",
    "TMDb",
    "TVDb",
]

API_TELEVISION = {"tvdb", "tvdbv4", "imdb", "omdb", "tmdb"}
API_MOVIE = {"tmdb", "omdb", "tvdbv4", "imdb"}
API_ALL = API_TELEVISION | API_MOVIE


class Provider(AbstractClass):
    """ABC for Providers, high-level interfaces for metadata media providers.
    """

    def __init__(self, **options):
        """Initializes the provider."""
        cls_name = self.__class__.__name__
        self._api_key = options.get(
            "api_key", environ.get("API_KEY_%s" % cls_name.upper())
        )
        self._pin = options.get(
            "pin", environ.get("PIN_%s" % cls_name.upper())
        )
        self._cache = options.get("cache", True)

    @abstractmethod
    def search(self, id_key=None, **parameters):
        pass

    @property
    def api_key(self):
        return self._api_key

    @property
    def pin(self):
        return self._pin

    @property
    def cache(self):
        return self._cache


def has_provider(provider):
    """Verifies that module has support for requested API provider."""
    return provider.lower() in API_ALL


def has_provider_support(provider, media_type):
    """Verifies if API provider has support for requested media type."""
    if provider.lower() not in API_ALL:
        return False
    provider_const = "API_" + media_type.upper()
    return provider in globals().get(provider_const, {})


def provider_factory(provider, **options):
    """Factory function for DB Provider concrete classes."""
    providers = {"tmdb": TMDb, "tvdb": TVDb, "omdb": OMDb}
    try:
        return providers[provider.lower()](**options)
    except KeyError:
        msg = "Attempted to initialize non-existing DB Provider"
        log.error(msg)
        raise MapiException(msg)


class OMDb(Provider):
    """Queries the OMDb API.
    """

    def __init__(self, **options):
        super(OMDb, self).__init__(**options)
        if not self.api_key:
            raise MapiProviderException("OMDb require API key")

    def search(self, id_key=None, **parameters):
        title = parameters.get("title") or parameters.get("query")
        year = parameters.get("year")
        id_imdb = id_key or parameters.get("id_imdb")
        kind = parameters.get("kind", "movie")

        if id_imdb:
            results = self._lookup_movie(id_imdb)
        elif title:
            results = self._search_movie(title, year, kind=kind)
        else:
            raise MapiNotFoundException
        for result in results:
            yield result

    def _lookup_movie(self, id_imdb, kind='movie'):
        response = omdb_title(self.api_key, id_imdb, media_type=kind, cache=self._cache)
        try:
            date = dt.strptime(response["Released"], "%d %b %Y").strftime(
                "%Y-%m-%d"
            )
        except (KeyError, ValueError):
            if response.get("Year") in (None, "N/A"):
                date = None
            else:
                date = "%s-01-01" % response["Year"]
        meta = MetadataMovie(
            title=response["Title"],
            date=date,
            synopsis=response["Plot"],
            id_imdb=id_imdb,
            year=response["Year"],
        )
        if meta["synopsis"] == "N/A":
            del meta["synopsis"]
        yield meta

    def _search_movie(self, title, year, kind='movie'):
        year_from, year_to = year_expand(year)
        found = False
        page = 1
        page_max = 10  # each page yields a maximum of 10 results
        while True:
            try:
                response = omdb_search(
                    api_key=self.api_key,
                    media_type="movie",
                    query=title,
                    page=page,
                    cache=self.cache,
                )
            except MapiNotFoundException:
                break
            for entry in response["Search"]:
                if year_from <= int(entry["Year"]) <= year_to:
                    for result in self._lookup_movie(entry["imdbID"]):
                        yield result
                    found = True
            if page >= page_max:
                break
            page += 1
        if not found:
            raise MapiNotFoundException


class TMDb(Provider):
    """Queries the TMDb API.
    """

    def __init__(self, **options):
        super(TMDb, self).__init__(**options)
        if not self.api_key:
            raise MapiProviderException("TMDb requires an API key")

    def search(self, id_key=None, **parameters):
        """Searches TMDb for movie metadata."""
        id_tmdb = id_key or parameters.get("id_tmdb")
        id_imdb = parameters.get("id_imdb")
        title = parameters.get("title") or parameters.get("query")
        year = parameters.get("year")
        kind = parameters.get("kind", "movie")

        if id_tmdb:
            results = self._search_id_tmdb(id_tmdb, kind=kind)
        elif id_imdb:
            results = self._search_id_imdb(id_imdb, kind=kind)
        elif title:
            results = self._search_title(title, year, kind)
        else:
            raise MapiNotFoundException
        for result in results:
            yield result

    def _search_id_imdb(self, id_imdb, kind='movie'):
        response = tmdb_find(
            self.api_key, "imdb_id", id_imdb, cache=self.cache
        )
        if not response["movie_results"] and not response["tv_results"]:
            raise MapiNotFoundException
        if kind == 'movie':
            response = response["movie_results"][0]
            meta = MetadataMovie(
                title=response["title"],
                date=response["release_date"],
                year=response["release_date"][:4],
                synopsis=response["overview"],
                id_tmdb=response["id"],
                id_imdb=response["imdb_id"],
                runtime=response.get("runtime", None),
                vote_average=response["vote_average"],
                original_language=response["original_language"],
                original_title=response["original_title"],
            )
        else:
            response["tv_results"][0]
            meta = MetadataTelevision(
                series=response["name"],
                title=response["name"],
                date=response["first_air_date"],
                year=response["first_air_date"][:4],
                synopsis=response["overview"],
                id_tmdb=response["id"],
                id_imdb=response["imdb_id"],
                runtime=response.get("episode_run_time", None),
                vote_average=response["vote_average"],
                original_language=response["original_language"],
                original_title=response["original_name"],
            )
        yield meta

    def _search_id_tmdb(self, id_tmdb, kind='movie'):
        assert id_tmdb
        response = tmdb_movies_or_series(self.api_key, id_tmdb, kind=kind, cache=self.cache)
        if kind == 'movie':
            meta = MetadataMovie(
                title=response["title"],
                date=response["release_date"],
                year=response["release_date"][:4],
                synopsis=response["overview"],
                media="movie",
                id_tmdb=response["id"],
                id_imdb=response.get("imdb_id", response.get('external_ids', {"imdb_id":None}).get("imdb_id", None)),
                runtime=response["runtime"],
                vote_average=response["vote_average"],
                original_language=response["original_language"],
                original_title=response["original_title"],
            )
        else:
            meta = MetadataTelevision(
                series=response["name"],
                title=response["name"],
                date=response["first_air_date"],
                synopsis=response["overview"],
                id_tmdb=response["id"],
                id_imdb=response.get('external_ids', {"imdb_id":None}).get("imdb_id", None),
                year=response["first_air_date"][:4],
                runtime=response.get("episode_run_time", None),
                vote_average=response["vote_average"],
                original_language=response["original_language"],
                original_title=response["original_name"],
            )
        yield meta

    def _search_title(self, title, year, kind='movie'):
        assert title
        found = False
        year_from, year_to = year_expand(year)
        page = 1
        page_max = 5  # each page yields a maximum of 20 results

        while True:
            response = tmdb_search(
                self.api_key, title, year, kind=kind, page=page, cache=self.cache
            )
            for entry in response["results"]:
                try:
                    if kind and kind == "movie":
                        meta = MetadataMovie(
                            title=entry["title"],
                            date=entry["release_date"],
                            synopsis=entry["overview"],
                            id_tmdb=ustr(entry["id"]),
                            id_imdb=entry.get("imdb_id", None),
                            year=entry["release_date"][:4],
                            runtime=entry.get("runtime", None),
                            vote_average=entry["vote_average"],
                            original_language=entry["original_language"],
                            original_title=entry["original_title"],
                        )
                    elif kind and kind.lower() in ["tv", "series"]:
                        meta = MetadataTelevision(
                            series=entry["name"],
                            date=entry["first_air_date"],
                            synopsis=entry["overview"],
                            id_tmdb=ustr(entry["id"]),
                            id_imdb=entry.get("imdb_id", None),
                            year=entry["first_air_date"][:4],
                            runtime=entry.get("episode_run_time", None),
                            vote_average=entry["vote_average"],
                            original_language=entry["original_language"],
                            original_title=entry["original_name"],
                        )
                    else:
                        raise MapiProviderException("Invalid media type: %s" % kind)
                except ValueError:
                    continue
                if year_from <= int(meta["year"]) <= year_to:
                    yield meta
                    found = True
            if page == response["total_pages"]:
                break
            elif page >= page_max:
                break
            page += 1
        if not found:
            raise MapiNotFoundException


class TVDb(Provider):
    """Queries the TVDb API.
    """

    def __init__(self, **options):
        super(TVDb, self).__init__(**options)
        if not self.api_key:
            raise MapiProviderException("TVDb requires an API key")
        self.token = "" if self.cache else self._login()

    def _login(self):
        return tvdb_login(self.api_key)

    def search(self, id_key=None, **parameters):
        """Searches TVDb for movie metadata.

        TODO: Consider making parameters for episode ids
        """
        title = parameters.get("title") or parameters.get("query") or parameters.get("series")
        episode = parameters.get("episode")
        id_tvdb = id_key or parameters.get("id_tvdb")
        id_imdb = parameters.get("id_imdb")
        season = parameters.get("season")
        series = parameters.get("series")
        date = parameters.get("date")
        date_fmt = r"(19|20)\d{2}(-(?:0[1-9]|1[012])(-(?:[012][1-9]|3[01]))?)?"

        try:
            if id_tvdb and date:
                results = self._search_tvdb_date(id_tvdb, date)
            elif id_tvdb:
                results = self._search_id_tvdb(id_tvdb, season, episode)
            elif id_imdb:
                results = self._search_id_imdb(id_imdb, season, episode)
            elif series and date:
                if not re.match(date_fmt, date):
                    raise MapiProviderException(
                        "Date format must be YYYY-MM-DD"
                    )
                results = self._search_series_date(series, date)
            elif series:
                results = self._search_series(series, season, episode)
            else:
                raise MapiNotFoundException
            for result in results:
                yield result
        except MapiProviderException:
            if not self.token:
                log.info(
                    "Result not cached; logging in and reattempting search"
                )
                self.token = self._login()
                for result in self.search(id_key, **parameters):
                    yield result
            else:
                raise

    def _search_id_imdb(self, id_imdb, season=None, episode=None):
        series_data = tvdb_search_series(
            self.token, id_imdb=id_imdb, cache=self.cache
        )
        id_tvdb = series_data["data"][0]["id"]
        return self._search_id_tvdb(id_tvdb, season, episode)

    def _search_id_tvdb(self, id_tvdb, season=None, episode=None):
        assert id_tvdb
        found = False
        series_data = tvdb_series_id(self.token, id_tvdb, cache=self.cache)
        page = 1
        while True:
            episode_data = tvdb_series_id_episodes_query(
                self.token,
                id_tvdb,
                episode,
                season,
                page=page,
                cache=self.cache,
            )
            for entry in episode_data["data"]:
                try:
                    yield MetadataTelevision(
                        series=series_data["data"]["seriesName"],
                        season=ustr(entry["airedSeason"]),
                        episode=ustr(entry["airedEpisodeNumber"]),
                        date=entry["firstAired"],
                        title=entry["episodeName"].split(";", 1)[0],
                        synopsis=(entry["overview"] or "")
                        .replace("\r\n", "")
                        .replace("  ", "")
                        .strip(),
                        media="television",
                        id_tvdb=ustr(id_tvdb),
                    )
                    found = True
                except (AttributeError, ValueError):
                    continue
            if page == episode_data["links"]["last"]:
                break
            page += 1
        if not found:
            raise MapiNotFoundException

    def _search_series(self, series, season, episode):
        assert series
        found = False
        series_data = tvdb_search_series(self.token, series, cache=self.cache)

        for series_id in [entry["id"] for entry in series_data["data"][:5]]:
            try:
                for data in self._search_id_tvdb(series_id, season, episode):
                    found = True
                    yield data
            except MapiNotFoundException:
                continue  # may not have requested episode or may be banned
        if not found:
            raise MapiNotFoundException

    def _search_tvdb_date(self, id_tvdb, date):
        found = False
        for meta in self._search_id_tvdb(id_tvdb):
            if meta["date"] and meta["date"].startswith(date):
                found = True
                yield meta
        if not found:
            raise MapiNotFoundException

    def _search_series_date(self, series, date):
        assert series and date
        series_data = tvdb_search_series(self.token, series, cache=self.cache)
        tvdb_ids = [entry["id"] for entry in series_data["data"]][:5]
        found = False
        for tvdb_id in tvdb_ids:
            try:
                for result in self._search_tvdb_date(tvdb_id, date):
                    yield result
                found = True
            except MapiNotFoundException:
                continue
        if not found:
            raise MapiNotFoundException


class TVDbV4(Provider):
    """Queries the TVDb API V4. https://api4.thetvdb.com/v4
    """

    def __init__(self, **options):
        super(TVDbV4, self).__init__(**options)
        if not self.api_key:
            raise MapiProviderException("TVDbV4 requires an API key")
        if not self.pin:
            raise MapiProviderException("TVDbV4 requires a PIN")
        self.token = "" if self.cache else self._login()  # The token has one month validation length.

    def _login(self):
        return tvdbv4_login(self.api_key, self.pin)

    def search(self, id_key=None, **parameters):
        """Searches TVDb for movie metadata.
        使用imdb_id/tvdb_id/或者title / title+year方式查询影片信息

        """
        query = parameters.get("query") or parameters.get("title")
        kind = parameters.get("kind")
        year = parameters.get("year")
        episode = parameters.get("episode")
        id_tvdb = id_key or parameters.get("id_tvdb")
        id_imdb = parameters.get("id_imdb")
        season = parameters.get("season")
        series = parameters.get("series")
        date = parameters.get("date")
        date_fmt = r"(19|20)\d{2}(-(?:0[1-9]|1[012])(-(?:[012][1-9]|3[01]))?)?"

        try:
            if id_tvdb and date:
                results = self._search_tvdb_date(id_tvdb, date)  # TODO: 暂未实现
            elif id_tvdb:
                results = self._search_id_tvdb(id_tvdb, season, episode)  # TODO: 暂未实现
            elif id_imdb:
                results = self._search_id_imdb(id_imdb, season, episode)  # TODO: 暂未实现
            elif series and date:
                if not re.match(date_fmt, date):
                    raise MapiProviderException(
                        "Date format must be YYYY-MM-DD"
                    )
                results = self._search_series_date(series, date)  # TODO: 暂未实现
            elif series:
                results = self._search_series(series, season, episode)  # TODO: 暂未实现
            elif query:
                results = self._search(query, kind, year)
            else:
                raise MapiNotFoundException
            for result in results:
                yield result
        except MapiProviderException:
            if not self.token:
                log.info(
                    "Result not cached; logging in and reattempting search"
                )
                self.token = self._login()
                for result in self.search(id_key, **parameters):  # 可能会嵌套
                    yield result
            else:
                raise

    def _search_id_imdb(self, id_imdb, season=None, episode=None):
        series_data = tvdb_search_series(
            self.token, id_imdb=id_imdb, cache=self.cache
        )
        id_tvdb = series_data["data"][0]["id"]
        return self._search_id_tvdb(id_tvdb, season, episode)

    def _search_id_tvdb(self, id_tvdb, season=None, episode=None):
        assert id_tvdb
        found = False
        series_data = tvdb_series_id(self.token, id_tvdb, cache=self.cache)
        page = 1
        while True:
            episode_data = tvdb_series_id_episodes_query(
                self.token,
                id_tvdb,
                episode,
                season,
                page=page,
                cache=self.cache,
            )
            for entry in episode_data["data"]:
                try:
                    yield MetadataTelevision(
                        series=series_data["data"]["seriesName"],
                        season=ustr(entry["airedSeason"]),
                        episode=ustr(entry["airedEpisodeNumber"]),
                        date=entry["firstAired"],
                        title=entry["episodeName"].split(";", 1)[0],
                        synopsis=(entry["overview"] or "")
                        .replace("\r\n", "")
                        .replace("  ", "")
                        .strip(),
                        media="television",
                        id_tvdb=ustr(id_tvdb),
                    )
                    found = True
                except (AttributeError, ValueError):
                    continue
            if page == episode_data["links"]["last"]:
                break
            page += 1
        if not found:
            raise MapiNotFoundException

    def _search_series(self, series, season, episode):
        assert series
        found = False
        series_data = tvdb_search_series(self.token, series, cache=self.cache)

        for series_id in [entry["id"] for entry in series_data["data"][:5]]:
            try:
                for data in self._search_id_tvdb(series_id, season, episode):
                    found = True
                    yield data
            except MapiNotFoundException:
                continue  # may not have requested episode or may be banned
        if not found:
            raise MapiNotFoundException

    def _search(self, query, kind=None, year=None, season=None, episode=None):
        assert query
        found = False
        meta_data = tvdbv4_search(self.token, query, kind=kind, year=year, cache=self.cache)

        for data in meta_data["data"]:  # TODO: Only search series now, no season & episode yet.
            try:
                found = True
                id_imdb = next((item['id'] for item in data['remote_ids'] if 'remote_ids' in data and item['sourceName'] == 'IMDB'), None)
                id_tmdb = next((item['id'] for item in data['remote_ids'] if 'remote_ids' in data and item['sourceName'] == 'TheMovieDB.com'), None)
                if kind == 'series':
                    meta = MetadataTelevision(
                        title=data["name"],
                        series=data["name"],
                        # season=ustr(data["airedSeason"]),
                        # episode=ustr(data["airedEpisodeNumber"]),
                        # title=data["episodeName"].split(";", 1)[0],
                        id_tvdb=ustr(data["tvdb_id"]),
                        id_imdb=id_imdb,
                        id_tmdb=id_tmdb,
                        year=data.get("year", data["first_air_time"][:4]),
                    )
                    yield meta
                elif kind == 'movie':
                    meta = MetadataMovie(
                        title=data["name"],
                        id_tvdb=data["tvdb_id"],
                        id_imdb=id_imdb,
                        id_tmdb=id_tmdb,
                        year=data.get("year", data["first_air_time"][:4]),
                    )
                    yield meta
                else:
                    raise MapiProviderException(f"kind {kind} not support")
            except MapiNotFoundException:
                raise  # may not have requested episode or may be banned
        if not found:
            raise MapiNotFoundException

    def _search_tvdb_date(self, id_tvdb, date):
        found = False
        for meta in self._search_id_tvdb(id_tvdb):
            if meta["date"] and meta["date"].startswith(date):
                found = True
                yield meta
        if not found:
            raise MapiNotFoundException

    def _search_series_date(self, series, date):
        assert series and date
        series_data = tvdb_search_series(self.token, series, cache=self.cache)
        tvdb_ids = [entry["id"] for entry in series_data["data"]][:5]
        found = False
        for tvdb_id in tvdb_ids:
            try:
                for result in self._search_tvdb_date(tvdb_id, date):
                    yield result
                found = True
            except MapiNotFoundException:
                continue
        if not found:
            raise MapiNotFoundException


class IMDb(Provider):
    """Queries from IMDb.com.
    """

    def __init__(self, **options):
        super(IMDb, self).__init__(**options)

    def search(self, id_key=None, **parameters):
        title = parameters.get("query") or parameters.get("title")
        year = parameters.get("year")
        kind = parameters.get("kind")
        id_imdb = id_key or parameters.get("id_imdb")

        if id_imdb:
            results = self._lookup_movie(id_imdb)
        elif title:
            results = self._search_movie(title, year=year, kind=kind)
        else:
            raise MapiNotFoundException
        for result in results:
            yield result

    def _lookup_movie(self, id_imdb):
        response = omdb_title(self.api_key, id_imdb, cache=self._cache)
        try:
            date = dt.strptime(response["Released"], "%d %b %Y").strftime(
                "%Y-%m-%d"
            )
        except (KeyError, ValueError):
            if response.get("Year") in (None, "N/A"):
                date = None
            else:
                date = "%s-01-01" % response["Year"]
        meta = MetadataMovie(
            title=response["Title"],
            date=date,
            synopsis=response["Plot"],
            id_imdb=id_imdb,
        )
        if meta["synopsis"] == "N/A":
            del meta["synopsis"]
        yield meta

    def _search_movie(self, title, year=None, kind='movie'):
        found = False
        try:
            response = imdb_suggestion(
                query=title,
                year=year,
                cache=self.cache,
            )
            for entry in response["d"]:
                found = True
                if kind == 'movie':
                    if entry["qid"].lower() in ['movie', 'short', 'tv_movie', 'video_movie']:
                        meta = MetadataMovie(
                            title=entry["l"],
                            id_imdb=entry["id"],
                            year=entry["y"],
                        )
                        yield meta
                elif kind == 'series':
                    if entry["qid"].lower() in ['tvseries', 'tvminiseries', 'tvshort', 'tvspecial']:
                        meta = MetadataTelevision(
                            title=entry["l"],
                            id_imdb=entry["id"],
                            year=entry["y"],
                        )
                        yield meta
                else:
                    raise MapiProviderException(f"kind {kind} not support")
        except MapiNotFoundException:
            raise
        if not found:
            raise MapiNotFoundException
