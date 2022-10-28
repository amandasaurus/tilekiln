import yaml
import json
from tilekiln.definition import Definition


class Config:
    def __init__(self, yaml_string, filesystem):
        '''Create a config from a yaml string
           Creates a config from the yaml string. Any SQL files referenced must be in the
           filesystem.
        '''
        config = yaml.safe_load(yaml_string)
        self.name = config.get("metadata").get("name")
        self.description = config.get("metadata").get("description")
        self.attribution = config.get("metadata").get("attribution")
        self.version = config.get("metadata").get("version")
        self.bounds = config.get("metadata").get("bounds")
        self.center = config.get("metadata").get("center")

        # TODO: Make private and expose needed operations through proper functions
        self.layers = []
        for id, l in config.get("vector_layers", {}).items():
            self.layers.append(LayerConfig(id, l, filesystem))

        self.minzoom = None
        self.maxzoom = None
        if self.layers:
            self.minzoom = min([layer.minzoom for layer in self.layers])
            self.maxzoom = max([layer.maxzoom for layer in self.layers])

    def tilejson(self, url):
        '''Returns a TileJSON'''

        # Todo: test with no attribution
        result = {"tilejson": "3.0.0",
                  "tiles": [url + "/{z}/{x}/{y}.mvt"],
                  "attribution": self.attribution,
                  "bounds": self.bounds,
                  "center": self.center,
                  "description": self.description,
                  "maxzoom": self.maxzoom,
                  "minzoom": self.minzoom,
                  "name": self.name,
                  "scheme": "xyz"}
        # TODO: vector_layers

        return json.dumps({k: v for k, v in result.items() if v is not None}, sort_keys=True, indent=4)


class LayerConfig:
    def __init__(self, id, layer_yaml, filesystem):
        '''Create a layer config
           Creates a layer config from the config yaml for a layer. Any SQL files referenced must
           be in the filesystem.
        '''
        self.id = id
        self.description = layer_yaml.get("description")
        self.fields = layer_yaml.get("fields")
        self.definitions = []
        self.geometry_type = set(layer_yaml.get("geometry_type", []))

        self.__definitions = set()
        for definition in layer_yaml.get("sql", []):
            self.__definitions.add(Definition(id, definition, filesystem))

        self.minzoom = min({d.minzoom for d in self.__definitions})
        self.maxzoom = max({d.maxzoom for d in self.__definitions})

    def render_sql(self, tile):
        '''Returns the SQL for a layer, given a tile, or None if it is outside the zoom range
           of the definitions
        '''
        if tile.zoom > self.maxzoom or tile.zoom < self.minzoom:
            return None

        for d in self.__definitions:
            if tile.zoom <= d.maxzoom and tile.zoom >= d.minzoom:
                return d.render_sql(tile)

        return None
