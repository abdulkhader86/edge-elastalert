import datetime
from elastalert.util import new_elasticsearch, build_es_conn_config
from elastalert.alerts import Alerter, BasicMatchString


def str2(match_obj):
    match_items = match_obj.match.items()
    match_items.sort(key=lambda x: x[0])
    text = ""
    for key, value in match_items:
        if key.startswith('_id'):
            value_str = unicode(value.strip())
            text += '%s\n' % (value_str)
    return text


# noinspection PyPackageRequirements
class ESAlerter(Alerter):
    required_options = frozenset(['es_index'])

    def __init__(self, *args):
        super(ESAlerter, self).__init__(*args)
        self.es_index = self.rule.get('es_index', '')

    # Alert is called
    def alert(self, matches):
        rule_es_conn_config = build_es_conn_config(self.rule)
        es = new_elasticsearch(rule_es_conn_config)

        for match in matches:
            match_obj = BasicMatchString(self.rule, match)
            es.index(self.es_index,
                              'alerts',
                              {"user": match_obj.rule['user'],
                               "rule_name": match_obj.rule['name'],
                               '@timestamp': datetime.datetime.utcnow(),
                               "info": str2(match_obj)})

    def get_info(self):
        return {'type': 'ES Alerter'}
