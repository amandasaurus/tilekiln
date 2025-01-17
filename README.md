# Tilekiln

## Background

Tilekiln is a set of command-line utilities to generate and serve Mapbox Vector Tiles (MVTs).

Generation relies on the standard method of a PostgreSQL + PostGIS server as a data source, and ST_AsMVT to serialize the MVTs.

The target use-case is vector tiles for OpenStreetMap Carto on openstreetmap.org, a worldwide complex basemap under high load.

Minutely updates are supported with an appropriately updating database.

## Requirements

Tilekiln requires a PostGIS database with data loaded to generate vector tiles.

[OpenStreetMap Carto's](https://github.com/gravitystorm/openstreetmap-carto/blob/master/INSTALL.md#openstreetmap-data) directions are a good starting place for loading OpenStreetMap data into a PostGIS database, but any PostGIS data source in EPSG 3857 will work.

- PostgreSQL 10+
- PostGIS 3.1+
- Python 3.10+

## Usage
Tilekiln commands can be broken into two sets, commands which involve serving tiles, and CLI commands. Command-line options can be found with `--help`.

### CLI commands
These commands will do something, then exit.

#### `config`
Commands to work with and check config files

#### `generate`
Commands for tile generation

#### `sql`
Prints the SQL for a tile

#### `storage`
Commands working with tile storage

### Serving commands
These commands start a HTTP server to serve content.

#### `dev`
Starts a server to live-render tiles with no caching, intended for development. It presents a tilejson at `/<id>/tilejson.json`, and for convience `/tilejson.json` redirects to it.

#### `live`
Like `serve`, but fall back to live generation if a tile is missing from storage.

It presents a tilejson at `/<id>/tilejson.json`.

#### `serve`
Serves tiles from tile storage. This is highly scalable and the preferred mode for production.

It presents a tilejson at `/<id>/tilejson.json`. In the future it will allow serving multiple tilesets.

## Quick-start
These instructions give you a setup based on osm2pgsql-themepark and their shortbread setup. They assume you have PostgreSQL with PostGIS and Python 3.10+ with venv set up, and a recent version of osm2pgsql.

### Install and setup

```sh
git clone https://github.com/osm2pgsql-dev/osm2pgsql-themepark.git
python3 -m venv tilekiln
tilekiln/bin/pip install tilekiln
createdb flex
psql -d flex -c 'CREATE EXTENSION postgis;'
createdb tiles
```

### Loading data
We have to produce a tilekiln config from the osm2pgsql-themepark config. This requires uncommenting a line in the config.

```sh
sed -i -E -e "s/--.*(themepark:plugin\('tilekiln'\):write_config\('tk'\))/\1/" ./osm2pgsql-themepark/config/shortbread_gen.lua
LUA_PATH="./osm2pgsql-themepark/lua/?.lua;;" osm2pgsql -d flex -O flex -S ./osm2pgsql-themepark/config/shortbread_gen.lua osm-data.pbf --cache 7000
LUA_PATH="./osm2pgsql-themepark/lua/?.lua;;" osm2pgsql-gen -d flex -S ./osm2pgsql-themepark/config/shortbread_gen.lua -j 8
mkdir -p downloads
osm2pgsql-themepark/themes/external/download-and-import.sh ./downloads flex oceans ocean
```

### Serve some tiles

```sh
tilekiln/bin/tilekiln dev
```

Use the tilejson URL `http://127.0.0.1:8000/tilejson.json` to load tiles into your preferred tile viewer such as QGIS.

## History
The tilekiln configuration syntax is based on studies and experience with other vector tile and map generation configurations. In particular, it is heavily inspired by Tilezen's use of Jinja2 templates and TileJSON for necessary metadata.

## License

### Code

Copyright © 2022-2024 Paul Norman <osm@paulnorman.ca>

The code is licensed terms of the GNU General Public License as
published by the Free Software Foundation, either version 3 of
the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

### Documentation

The text of the documentation and configuration format specification is licensed under a [Creative Commons Attribution 4.0 International License](https://creativecommons.org/licenses/by/4.0/). However, the use of the specification in products and code is entirely free: there are no royalties, restrictions, or requirements.

### Sample configuration

The sample configuration files are released under the CC0 Public
Domain Dedication, version 1.0, as published by Creative Commons.
To the extent possible under law, the author(s) have dedicated all
copyright and related and neighboring rights to the Software to
the public domain worldwide. The Software is distributed WITHOUT
ANY WARRANTY.

If you did not receive a copy of the CC0 Public Domain Dedication
along with the Software, see
<http://creativecommons.org/publicdomain/zero/1.0/>
