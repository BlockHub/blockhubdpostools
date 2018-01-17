import yaml


schemes = {
    'base': yaml.load(open('yamls/arkdbschema.yaml')),
    'ark': yaml.load(open('yamls/arkdbschema.yaml')),
    'oxycoin': yaml.load(open('yamls/oxycoindbschema.yaml')),
}