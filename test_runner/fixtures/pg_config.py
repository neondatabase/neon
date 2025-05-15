from __future__ import annotations

import shlex
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, cast, final

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import IO

    from fixtures.neon_fixtures import PgBin


@final
class PgConfigKey(StrEnum):
    BINDIR = "BINDIR"
    DOCDIR = "DOCDIR"
    HTMLDIR = "HTMLDIR"
    INCLUDEDIR = "INCLUDEDIR"
    PKGINCLUDEDIR = "PKGINCLUDEDIR"
    INCLUDEDIR_SERVER = "INCLUDEDIR-SERVER"
    LIBDIR = "LIBDIR"
    PKGLIBDIR = "PKGLIBDIR"
    LOCALEDIR = "LOCALEDIR"
    MANDIR = "MANDIR"
    SHAREDIR = "SHAREDIR"
    SYSCONFDIR = "SYSCONFDIR"
    PGXS = "PGXS"
    CONFIGURE = "CONFIGURE"
    CC = "CC"
    CPPFLAGS = "CPPFLAGS"
    CFLAGS = "CFLAGS"
    CFLAGS_SL = "CFLAGS_SL"
    LDFLAGS = "LDFLAGS"
    LDFLAGS_EX = "LDFLAGS_EX"
    LDFLAGS_SL = "LDFLAGS_SL"
    LIBS = "LIBS"
    VERSION = "VERSION"


if TYPE_CHECKING:
    # TODO: This could become a TypedDict if Python ever allows StrEnums to be
    # keys.
    PgConfig = dict[PgConfigKey, str | Path | list[str]]


def __get_pg_config(pg_bin: PgBin) -> PgConfig:
    """Get pg_config values by invoking the command"""

    cmd = pg_bin.run_nonblocking(["pg_config"])
    cmd.wait()
    if cmd.returncode != 0:
        pytest.exit("")
    assert cmd.stdout

    stdout = cast("IO[str]", cmd.stdout)

    # Parse the output into a dictionary
    values: PgConfig = {}
    for line in stdout.readlines():
        if "=" in line:
            key, value = line.split("=", 1)
            value = value.strip()
            match PgConfigKey(key.strip()):
                case (
                    (
                        PgConfigKey.CC
                        | PgConfigKey.CPPFLAGS
                        | PgConfigKey.CFLAGS
                        | PgConfigKey.CFLAGS_SL
                        | PgConfigKey.LDFLAGS
                        | PgConfigKey.LDFLAGS_EX
                        | PgConfigKey.LDFLAGS_SL
                        | PgConfigKey.LIBS
                    ) as k
                ):
                    values[k] = shlex.split(value)
                case (
                    (
                        PgConfigKey.BINDIR
                        | PgConfigKey.DOCDIR
                        | PgConfigKey.HTMLDIR
                        | PgConfigKey.INCLUDEDIR
                        | PgConfigKey.PKGINCLUDEDIR
                        | PgConfigKey.INCLUDEDIR_SERVER
                        | PgConfigKey.LIBDIR
                        | PgConfigKey.PKGLIBDIR
                        | PgConfigKey.LOCALEDIR
                        | PgConfigKey.MANDIR
                        | PgConfigKey.SHAREDIR
                        | PgConfigKey.SYSCONFDIR
                        | PgConfigKey.PGXS
                    ) as k
                ):
                    values[k] = Path(value)
                case _ as k:
                    values[k] = value

    return values


@pytest.fixture(scope="function")
def pg_config(pg_bin: PgBin) -> Iterator[PgConfig]:
    """Dictionary of all pg_config values from the system"""

    yield __get_pg_config(pg_bin)


@pytest.fixture(scope="function")
def pg_config_bindir(pg_config: PgConfig) -> Iterator[Path]:
    """BINDIR value from pg_config"""
    yield cast("Path", pg_config[PgConfigKey.BINDIR])


@pytest.fixture(scope="function")
def pg_config_docdir(pg_config: PgConfig) -> Iterator[Path]:
    """DOCDIR value from pg_config"""
    yield cast("Path", pg_config[PgConfigKey.DOCDIR])


@pytest.fixture(scope="function")
def pg_config_htmldir(pg_config: PgConfig) -> Iterator[Path]:
    """HTMLDIR value from pg_config"""
    yield cast("Path", pg_config[PgConfigKey.HTMLDIR])


@pytest.fixture(scope="function")
def pg_config_includedir(
    pg_config: dict[PgConfigKey, str | Path | list[str]],
) -> Iterator[Path]:
    """INCLUDEDIR value from pg_config"""
    yield cast("Path", pg_config[PgConfigKey.INCLUDEDIR])


@pytest.fixture(scope="function")
def pg_config_pkgincludedir(pg_config: PgConfig) -> Iterator[Path]:
    """PKGINCLUDEDIR value from pg_config"""
    yield cast("Path", pg_config[PgConfigKey.PKGINCLUDEDIR])


@pytest.fixture(scope="function")
def pg_config_includedir_server(pg_config: PgConfig) -> Iterator[Path]:
    """INCLUDEDIR-SERVER value from pg_config"""
    yield cast("Path", pg_config[PgConfigKey.INCLUDEDIR_SERVER])


@pytest.fixture(scope="function")
def pg_config_libdir(pg_config: PgConfig) -> Iterator[Path]:
    """LIBDIR value from pg_config"""
    yield cast("Path", pg_config[PgConfigKey.LIBDIR])


@pytest.fixture(scope="function")
def pg_config_pkglibdir(pg_config: PgConfig) -> Iterator[Path]:
    """PKGLIBDIR value from pg_config"""
    yield cast("Path", pg_config[PgConfigKey.PKGLIBDIR])


@pytest.fixture(scope="function")
def pg_config_localedir(pg_config: PgConfig) -> Iterator[Path]:
    """LOCALEDIR value from pg_config"""
    yield cast("Path", pg_config[PgConfigKey.LOCALEDIR])


@pytest.fixture(scope="function")
def pg_config_mandir(pg_config: PgConfig) -> Iterator[Path]:
    """MANDIR value from pg_config"""
    yield cast("Path", pg_config[PgConfigKey.MANDIR])


@pytest.fixture(scope="function")
def pg_config_sharedir(pg_config: PgConfig) -> Iterator[Path]:
    """SHAREDIR value from pg_config"""
    yield cast("Path", pg_config[PgConfigKey.SHAREDIR])


@pytest.fixture(scope="function")
def pg_config_sysconfdir(pg_config: PgConfig) -> Iterator[Path]:
    """SYSCONFDIR value from pg_config"""
    yield cast("Path", pg_config[PgConfigKey.SYSCONFDIR])


@pytest.fixture(scope="function")
def pg_config_pgxs(pg_config: PgConfig) -> Iterator[Path]:
    """PGXS value from pg_config"""
    yield cast("Path", pg_config[PgConfigKey.PGXS])


@pytest.fixture(scope="function")
def pg_config_configure(pg_config: PgConfig) -> Iterator[str]:
    """CONFIGURE value from pg_config"""
    yield cast("str", pg_config[PgConfigKey.CONFIGURE])


@pytest.fixture(scope="function")
def pg_config_cc(pg_config: PgConfig) -> Iterator[list[str]]:
    """CC value from pg_config"""
    yield cast("list[str]", pg_config[PgConfigKey.CC])


@pytest.fixture(scope="function")
def pg_config_cppflags(pg_config: PgConfig) -> Iterator[list[str]]:
    """CPPFLAGS value from pg_config"""
    yield cast("list[str]", pg_config[PgConfigKey.CPPFLAGS])


@pytest.fixture(scope="function")
def pg_config_cflags(pg_config: PgConfig) -> Iterator[list[str]]:
    """CFLAGS value from pg_config"""
    yield cast("list[str]", pg_config[PgConfigKey.CFLAGS])


@pytest.fixture(scope="function")
def pg_config_cflags_sl(pg_config: PgConfig) -> Iterator[list[str]]:
    """CFLAGS_SL value from pg_config"""
    yield cast("list[str]", pg_config[PgConfigKey.CFLAGS_SL])


@pytest.fixture(scope="function")
def pg_config_ldflags(pg_config: PgConfig) -> Iterator[list[str]]:
    """LDFLAGS value from pg_config"""
    yield cast("list[str]", pg_config[PgConfigKey.LDFLAGS])


@pytest.fixture(scope="function")
def pg_config_ldflags_ex(pg_config: PgConfig) -> Iterator[list[str]]:
    """LDFLAGS_EX value from pg_config"""
    yield cast("list[str]", pg_config[PgConfigKey.LDFLAGS_EX])


@pytest.fixture(scope="function")
def pg_config_ldflags_sl(pg_config: PgConfig) -> Iterator[list[str]]:
    """LDFLAGS_SL value from pg_config"""
    yield cast("list[str]", pg_config[PgConfigKey.LDFLAGS_SL])


@pytest.fixture(scope="function")
def pg_config_libs(pg_config: PgConfig) -> Iterator[list[str]]:
    """LIBS value from pg_config"""
    yield cast("list[str]", pg_config[PgConfigKey.LIBS])


@pytest.fixture(scope="function")
def pg_config_version(pg_config: PgConfig) -> Iterator[str]:
    """VERSION value from pg_config"""
    yield cast("str", pg_config[PgConfigKey.VERSION])
