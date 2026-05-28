import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / 'src' / 'DatabaseLibrary' / 'connection_manager.py'


class _DummyBuiltIn(object):
    pass


class _DummyConnectionCache(object):
    def __init__(self, error_message):
        self.error_message = error_message

    def register(self, obj_dict, alias=None):
        self.obj_dict = obj_dict

    def switch(self, alias=None):
        return self.obj_dict


class _DummyLogger(object):
    def info(self, *args, **kwargs):
        return None


def _load_connection_manager_module():
    robot_module = types.ModuleType('robot')
    robot_utils = types.ModuleType('robot.utils')
    robot_utils.ConnectionCache = _DummyConnectionCache
    robot_utils_asserts = types.ModuleType('robot.utils.asserts')
    robot_utils_asserts.fail = lambda *args, **kwargs: None
    robot_libraries = types.ModuleType('robot.libraries')
    robot_built_in = types.ModuleType('robot.libraries.BuiltIn')
    robot_built_in.BuiltIn = _DummyBuiltIn
    robot_api = types.ModuleType('robot.api')
    robot_api.logger = _DummyLogger()
    robot_module.utils = robot_utils

    stubbed_modules = {
        'robot': robot_module,
        'robot.api': robot_api,
        'robot.libraries': robot_libraries,
        'robot.libraries.BuiltIn': robot_built_in,
        'robot.utils': robot_utils,
        'robot.utils.asserts': robot_utils_asserts,
    }

    with mock.patch.dict(sys.modules, stubbed_modules):
        spec = importlib.util.spec_from_file_location(
            'database_library_connection_manager_test_module', str(MODULE_PATH))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


CONNECTION_MANAGER_MODULE = _load_connection_manager_module()


class _FakePsycopg2Module(object):
    def __init__(self):
        self.calls = []

    def connect(self, database=None, user=None, password=None, host=None, port=None,
                sslmode=None, sslrootcert=None):
        self.calls.append({
            'database': database,
            'user': user,
            'password': password,
            'host': host,
            'port': port,
            'sslmode': sslmode,
            'sslrootcert': sslrootcert,
        })
        return object()


class _FakePyMySQLModule(object):
    def __init__(self):
        self.calls = []

    def connect(self, db=None, user=None, passwd=None, host=None, port=None, charset=None,
                ssl=None, ssl_disabled=None, ssl_verify_cert=None, ssl_verify_identity=None):
        self.calls.append({
            'db': db,
            'user': user,
            'passwd': passwd,
            'host': host,
            'port': port,
            'charset': charset,
            'ssl': ssl,
            'ssl_disabled': ssl_disabled,
            'ssl_verify_cert': ssl_verify_cert,
            'ssl_verify_identity': ssl_verify_identity,
        })
        return object()


class ConnectionManagerTlsTests(unittest.TestCase):
    def setUp(self):
        self.module = CONNECTION_MANAGER_MODULE
        self.manager = self.module.ConnectionManager.__new__(self.module.ConnectionManager)
        # Tests assert connect kwargs, so caching is stubbed out.
        self.manager._push_cache = mock.Mock()

    def test_psycopg2_uses_verify_full_for_rds_hosts(self):
        fake_module = _FakePsycopg2Module()

        with mock.patch.object(self.module.importlib, 'import_module', return_value=fake_module):
            self.manager._connect_to_database(
                alias='pg',
                dbapiModuleName='psycopg2',
                dbName='app',
                dbUsername='user',
                dbPassword='secret',
                dbHost='app.cluster-123.eu-west-1.rds.amazonaws.com',
                dbPort=5432,
                dbCharset=None)

        self.assertEqual(len(fake_module.calls), 1)
        self.assertEqual(fake_module.calls[0]['sslmode'], 'verify-full')
        self.assertEqual(
            fake_module.calls[0]['sslrootcert'],
            self.module.RDS_GLOBAL_BUNDLE_PATH)

    def test_psycopg2_uses_verify_ca_for_custom_aliases(self):
        fake_module = _FakePsycopg2Module()

        with mock.patch.object(self.module.importlib, 'import_module', return_value=fake_module):
            self.manager._connect_to_database(
                alias='pg',
                dbapiModuleName='psycopg2',
                dbName='app',
                dbUsername='user',
                dbPassword='secret',
                dbHost='pgsql-staging.internal',
                dbPort=5432,
                dbCharset=None)

        self.assertEqual(len(fake_module.calls), 1)
        self.assertEqual(fake_module.calls[0]['sslmode'], 'verify-ca')
        self.assertEqual(
            fake_module.calls[0]['sslrootcert'],
            self.module.RDS_GLOBAL_BUNDLE_PATH)

    def test_pymysql_enables_verified_tls_for_rds_hosts(self):
        fake_module = _FakePyMySQLModule()

        with mock.patch.object(self.module.importlib, 'import_module', return_value=fake_module):
            self.manager._connect_to_database(
                alias='mysql',
                dbapiModuleName='pymysql',
                dbName='app',
                dbUsername='user',
                dbPassword='secret',
                dbHost='app.cluster-123.eu-west-1.rds.amazonaws.com',
                dbPort=3306,
                dbCharset='utf8mb4')

        self.assertEqual(len(fake_module.calls), 1)
        self.assertEqual(
            fake_module.calls[0]['ssl']['ca'],
            self.module.RDS_GLOBAL_BUNDLE_PATH)
        self.assertEqual(
            fake_module.calls[0]['ssl']['cert_reqs'],
            self.module.ssl.CERT_REQUIRED)
        self.assertFalse(fake_module.calls[0]['ssl_disabled'])
        self.assertTrue(fake_module.calls[0]['ssl_verify_cert'])
        self.assertTrue(fake_module.calls[0]['ssl_verify_identity'])

    def test_pymysql_accepts_explicit_tls_keyword_arguments(self):
        fake_module = _FakePyMySQLModule()

        with mock.patch.object(self.module.importlib, 'import_module', return_value=fake_module):
            self.manager._connect_to_database(
                alias='mysql',
                dbapiModuleName='pymysql',
                dbName='app',
                dbUsername='user',
                dbPassword='secret',
                dbHost='mysql-staging.internal',
                dbPort=3306,
                dbCharset='utf8mb4',
                ssl_ca='/tmp/custom-ca.pem',
                ssl_verify_cert=True,
                ssl_verify_identity=False)

        self.assertEqual(len(fake_module.calls), 1)
        self.assertEqual(
            fake_module.calls[0]['ssl']['ca'],
            '/tmp/custom-ca.pem')
        self.assertEqual(
            fake_module.calls[0]['ssl']['cert_reqs'],
            self.module.ssl.CERT_REQUIRED)
        self.assertFalse(fake_module.calls[0]['ssl_disabled'])
        self.assertTrue(fake_module.calls[0]['ssl_verify_cert'])
        self.assertFalse(fake_module.calls[0]['ssl_verify_identity'])

    def test_psycopg2_accepts_explicit_tls_keyword_arguments(self):
        fake_module = _FakePsycopg2Module()

        with mock.patch.object(self.module.importlib, 'import_module', return_value=fake_module):
            self.manager._connect_to_database(
                alias='pg',
                dbapiModuleName='psycopg2',
                dbName='app',
                dbUsername='user',
                dbPassword='secret',
                dbHost='pgsql-staging.internal',
                dbPort=5432,
                dbCharset=None,
                sslmode='verify-full',
                sslrootcert='/tmp/custom-ca.pem')

        self.assertEqual(len(fake_module.calls), 1)
        self.assertEqual(fake_module.calls[0]['sslmode'], 'verify-full')
        self.assertEqual(fake_module.calls[0]['sslrootcert'], '/tmp/custom-ca.pem')

    def test_custom_params_override_insecure_postgresql_tls_flags(self):
        fake_module = _FakePsycopg2Module()

        with mock.patch.object(self.module.importlib, 'import_module', return_value=fake_module):
            self.manager.connect_to_database_using_custom_params(
                alias='custom-pg',
                dbapiModuleName='psycopg2',
                db_connect_string="database='app', user='user', password='secret', host='pgsql-staging.internal', port=5432, sslmode='disable', sslrootcert='/tmp/other.pem'")

        self.assertEqual(len(fake_module.calls), 1)
        self.assertEqual(fake_module.calls[0]['sslmode'], 'verify-ca')
        self.assertEqual(
            fake_module.calls[0]['sslrootcert'],
            self.module.RDS_GLOBAL_BUNDLE_PATH)
        self.manager._push_cache.assert_called_once()
        self.assertEqual(self.manager._push_cache.call_args[0][0], 'custom-pg')


if __name__ == '__main__':
    unittest.main()
