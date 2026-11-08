"""Generic exit-rule catalog.

The infrastructure asks by plugin id and receives a spec string; it never
imports a concrete strategy. An application supplies the rule map and the
feature keys its plugins store. Unknown ids are absent, not silently served by
another strategy.
"""


class ExitRuleRegistry:
    def __init__(self, rules, feature_keys):
        self.rules = dict(rules)
        self.feature_keys = {key: tuple(value) for key, value in dict(feature_keys).items()}

    def feature_keys_for(self, plugin_id):
        return tuple(self.feature_keys.get(plugin_id) or ())

    def specs_for(self, plugin_ids):
        return {plugin_id: self.rules[plugin_id] for plugin_id in plugin_ids
                if plugin_id in self.rules}
