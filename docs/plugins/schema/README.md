# Machine schemas

- `descriptor.schema.json` mirrors `PluginDescriptor.as_dict()`.
- `manifest.schema.json` mirrors the serialized `PluginManifest` fields.
- Config schemas are supplied by each plugin and must be a closed flat scalar
  object. The loader validates them with `trading_infra.plugin_protocol`.
