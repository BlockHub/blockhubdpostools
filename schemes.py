import yaml

schemes = {
    'base': yaml.load(open('basedbschema.yaml')),
    'ark': yaml.load(open('arkdbschema.yaml')),
    'oxycoin': yaml.load(open('oxycoindbschema.yaml')),
}