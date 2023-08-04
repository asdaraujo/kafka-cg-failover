import os
import re
import sys
import time

import yaml
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition, OffsetAndMetadata
from krbticket import KrbConfig, KrbCommand
from optparse import OptionParser
from random import randint
import concurrent.futures
import logging

logging.basicConfig(level=logging.INFO)
for logger in ['kafka.conn', 'kafka.cluster', 'kafka.coordinator']:
    logging.getLogger(logger).setLevel(logging.WARNING)
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

MAX_WORKERS = 10


class KafkaClient(object):
    def __init__(self, config):
        self.config = config
        self._krb_cache_file = f'/tmp/{randint(1, 10000000)}'
        self._admin_client = None
        self._consumer = None

    def __del__(self):
        if os.path.exists(self._krb_cache_file):
            os.unlink(self._krb_cache_file)

    def validate(self):
        # Check Kafka credentials
        if (self.kerberos_principal and not self.kerberos_keytab) or \
                (not self.kerberos_principal and self.kerberos_keytab):
            raise RuntimeError('The following properties must always be specified together: '
                               'kerberos_principal, kerberos_keytab (cluster [{}]).'.format(self.alias))
        if (self.ldap_username and not self.ldap_password) or \
                (not self.ldap_username and self.ldap_password):
            raise RuntimeError('The following properties must always be specified together: '
                               'ldap_username, ldap_password (cluster [{}]).'.format(self.alias))
        if self.ldap_username and self.kerberos_principal:
            raise RuntimeError('The following properties are mutually exclusive: '
                               'ldap_username, kerberos_principal (cluster [{}]).'.format(self.alias))

        # Check service connectivity
        self.test_kafka_connectivity()

    def client_config(self):
        client_config = {
            'bootstrap_servers': self.bootstrap_servers,
            'ssl_cafile': self.tls_ca_cert,
            'api_version_auto_timeout_ms': 30000,
        }
        if self.ldap_username:
            client_config.update({
                'security_protocol': 'SASL_SSL' if self.use_tls else 'SASL_PLAINTEXT',
                'sasl_mechanism': 'PLAIN',
                'sasl_plain_username': self.ldap_username,
                'sasl_plain_password': self.ldap_password,
            })
        elif self.kerberos_principal:
            client_config.update({
                'security_protocol': 'SASL_SSL' if self.use_tls else 'SASL_PLAINTEXT',
                'sasl_mechanism': 'GSSAPI',
            })
        else:
            client_config.update({
                'security_protocol': 'PLAINTEXT',
            })

        if self.client_configs:
            client_config.update(self.client_configs)

        return client_config

    def _ensure_kinit(self):
        if not os.path.exists(self._krb_cache_file):
            if self.krb5_conf:
                os.environ['KRB5_CONFIG'] = self.krb5_conf

            if self.kerberos_principal:
                os.environ['KRB5CCNAME'] = self._krb_cache_file
                krb_config = KrbConfig(principal=self.kerberos_principal, keytab=self.kerberos_keytab)
                KrbCommand.kinit(krb_config)

    def describe_topics(self):
        return self.admin_client.describe_topics()

    def list_consumer_groups(self, broker_ids=None):
        all_groups = self.admin_client.list_consumer_groups(broker_ids)
        return [t[0] for t in all_groups if t[1] != 'connect']

    def list_consumer_group_offsets(self, group_id):
        return self.admin_client.list_consumer_group_offsets(group_id)

    def _fetch_one(self, tp, offset):
        consumer = self.create_consumer()
        end_offsets = consumer.end_offsets([tp])
        consumer.assign([tp])
        consumer.seek(tp, max(0, min(offset - 1, end_offsets[tp] - 1)))
        retries = 6
        while retries > 0:
            results = consumer.poll(timeout_ms=60000, max_records=1, update_offsets=False)
            if results is not None and results.get(tp, None):
                return results[tp][0]
            retries -= 1
            LOG.debug(f'Retry polling {tp}')
            time.sleep(1)

    def get_timestamps(self, offsets):
        timestamps = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_tp = {executor.submit(self._fetch_one, tp, om.offset): tp for tp, om in offsets.items()}
            cnt = 0
            for future in concurrent.futures.as_completed(future_to_tp):
                cnt += 1
                sys.stderr.write('\rINFO:{}:Partitions completed: {}/{} ({}%)'.format(
                    __name__, cnt, len(future_to_tp), int(100 * cnt / len(future_to_tp))))
                tp = future_to_tp[future]
                try:
                    record = future.result()
                    ts = record.timestamp
                except Exception as exc:
                    raise RuntimeError(f'Failed to fetch offset {offsets[tp].offset} for {tp} in cluster {self.alias}')
                else:
                    timestamps[tp] = ts
            sys.stderr.write('\n')
        return timestamps

    def get_offsets_for_timestamps(self, timestamps):
        consumer = self.create_consumer()
        offsets_and_ts = consumer.offsets_for_times(timestamps)
        missing_partitions = [tp for tp, o in offsets_and_ts.items() if o is None]
        end_offsets = consumer.end_offsets(missing_partitions)
        offsets = {tp: end_offsets[tp] if o is None else o.offset for tp, o in offsets_and_ts.items()}
        return offsets

    def commit_offsets(self, group_id, offsets):
        return self.create_consumer(group_id).commit({tp: OffsetAndMetadata(o, '') for tp, o in offsets.items()})

    def test_kafka_connectivity(self):
        try:
            self.describe_topics()
            LOG.info(f'{self.alias}: Connected successfully!')
        except Exception as exc:
            raise RuntimeError('Cannot list topics from cluster [{}]. Exception: {}'.format(self.alias, exc))

    def _get_property(self, name, default=None, required=False):
        value = self.config.get(name, default)
        if value is None and required:
            msg = f'Property [{name}] is missing'
            alias = self.config.get('alias', None)
            if alias:
                msg += f' for Kafka service [{alias}]'
            raise RuntimeError(msg)
        return value

    @property
    def admin_client(self):
        if not self._admin_client:
            self._ensure_kinit()
            self._admin_client = KafkaAdminClient(**self.client_config())
        return self._admin_client

    def create_consumer(self, group_id=None):
        self._ensure_kinit()
        return KafkaConsumer(group_id=group_id, enable_auto_commit=False, **self.client_config())

    @property
    def alias(self):
        return self._get_property('alias', required=True)

    @property
    def bootstrap_servers(self):
        return self._get_property('bootstrap_servers', required=True)

    @property
    def use_tls(self):
        return self._get_property('use_tls',
                                  '9093' in self.bootstrap_servers,
                                  '9093' not in self.bootstrap_servers and '9092' not in self.bootstrap_servers)

    @property
    def ldap_username(self):
        return self.config.get('ldap_username', None)

    @property
    def ldap_password(self):
        return self.config.get('ldap_password', None)

    @property
    def kerberos_principal(self):
        return self.config.get('kerberos_principal', None)

    @property
    def kerberos_keytab(self):
        return self.config.get('kerberos_keytab', None)

    @property
    def krb5_conf(self):
        return self.config.get('krb5_conf', None)

    @property
    def tls_ca_cert(self):
        return self.config.get('tls_ca_cert', None)

    @property
    def client_configs(self):
        return self.config.get('client_configs', {})


def show_offsets(group_id, offsets):
    header_fmt = '{:15s} {:15s} {:10s} {:15s}\n'
    fmt = '{:15s} {:15s} {:<10d} {:<15d}\n'
    header = header_fmt.format('GROUP', 'TOPIC', 'PARTITION', 'CURRENT-OFFSET')
    sys.stderr.write('\n')
    sys.stderr.write(header)
    for tp, offset in sorted(offsets.items(), key=lambda o: o[0]):
        sys.stderr.write(fmt.format(group_id, tp.topic, tp.partition, offset))
    sys.stderr.write('\n')


class ConsumerGroupFailover(object):
    def __init__(self, config_file, groups_regex, source, target, no_prefix, no_dry_run):
        self.config_file = config_file
        self.groups_regex = groups_regex
        self.source = source
        self.target = target
        self.no_prefix = no_prefix
        self.no_dry_run = no_dry_run
        self.kafka_clients = {}
        self._config = None

    @property
    def config(self):
        if not self._config:
            self._config = yaml.load(open(self.config_file, 'r'), Loader=yaml.Loader)
        return self._config

    def validate_config(self):
        for ks in self.config['kafka_services']:
            kafka_client = KafkaClient(ks)
            kafka_client.validate()
            # Check alias uniqueness
            if kafka_client.alias in self.kafka_clients:
                raise RuntimeError('Cluster aliases must be unique.'
                                   ' The alias [{}] is duplicated'.format(kafka_client.alias))
            self.kafka_clients[kafka_client.alias] = kafka_client

    @property
    def source_client(self):
        return self.kafka_clients[self.source]

    @property
    def target_client(self):
        return self.kafka_clients[self.target]

    def get_source_groups(self):
        all_groups = self.source_client.list_consumer_groups()
        return [g for g in all_groups if re.match(self.groups_regex, g)]

    def get_source_offsets(self, group_id):
        return self.source_client.list_consumer_group_offsets(group_id)

    def translate_prefix(self, topic):
        if self.no_prefix:
            return topic
        if topic.startswith(f'{self.target}.'):
            return topic.replace(f'{self.target}.', '', 1)
        else:
            return f'{self.source}.{topic}'

    def translate_prefixes(self, timestamps):
        return {TopicPartition(self.translate_prefix(tp.topic), tp.partition): x for tp, x in timestamps.items()}

    def translate(self):
        groups = self.get_source_groups()
        if not groups:
            LOG.info(f'No consumer groups were matched by pattern "{self.groups_regex}" in cluster {self.source}')
            return
        for grp in sorted(self.get_source_groups()):
            if not self.no_dry_run:
                LOG.info('#################################')
                LOG.info('##      THIS IS A DRY RUN      ##')
                LOG.info('#################################')
            LOG.info(f'Translating consumer group {grp}')
            offsets = self.get_source_offsets(grp)
            LOG.info(f'Fetching timestamps for the offsets of the following source topics: {", ".join(set([tp.topic for tp in offsets.keys()]))}')
            timestamps = self.source_client.get_timestamps(offsets)
            translated_prefixes = self.translate_prefixes(timestamps)
            LOG.info(f'Target topics: {", ".join(set([tp.topic for tp in translated_prefixes.keys()]))}')
            translated_offsets = self.target_client.get_offsets_for_timestamps(translated_prefixes)
            show_offsets(grp, translated_offsets)
            if self.no_dry_run:
                LOG.info(f'Committing translated offsets to consumer group {grp} on cluster {self.target}')
                self.target_client.commit_offsets(grp, translated_offsets)
            else:
                LOG.info('#################################')
                LOG.info('##      THIS IS A DRY RUN      ##')
                LOG.info('##  NO CHANGES WERE COMMITTED  ##')
                LOG.info('#################################')

    def main(self):
        self.validate_config()
        self.translate()


def validate_arguments(options):
    # Check for required arguments
    required_arguments = ['config', 'source', 'target', ('groups_regex', '--groups-regex')]
    missing_arguments = []
    for arg in required_arguments:
        if isinstance(arg, str):
            req_arg = arg
            cmd_option = f'--{arg}'
        else:
            req_arg = arg[0]
            cmd_option = arg[1]
        if getattr(options, req_arg, None) is None:
            missing_arguments.append(cmd_option)
    if missing_arguments:
        raise RuntimeError(f'Missing arguments: {", ".join(missing_arguments)}')


if __name__ == '__main__':
    PARSER = OptionParser()
    PARSER.add_option('-c', '--config', dest='config',
                      help='Configuration file (YAML format)', metavar='FILE')
    PARSER.add_option('-g', '--groups-regex', dest='groups_regex',
                      help='Regex matching the names of groups to translate', metavar='REGEX')
    PARSER.add_option('-s', '--source', dest='source',
                      help='Source cluster', metavar='ALIAS')
    PARSER.add_option('-t', '--target', dest='target',
                      help='Target cluster', metavar='ALIAS')
    PARSER.add_option('-n', '--no-prefix', dest='no_prefix', action='store_true',
                      help='Do not use topic prefixes')
    PARSER.add_option('-d', '--no-dry-run', dest='no_dry_run', action='store_true',
                      help='Commit offsets on target cluster')
    (options, args) = PARSER.parse_args()
    validate_arguments(options)

    sc = ConsumerGroupFailover(options.config, options.groups_regex, options.source, options.target, options.no_prefix,
                               options.no_dry_run)
    sc.main()
