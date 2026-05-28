#  Copyright (c) 2010 Franz Allan Valencia See
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import importlib
import inspect
import re
import ssl
import robot
from robot.libraries.BuiltIn import BuiltIn
from robot.utils.asserts import fail
from urllib.parse import urlparse

try:
    import ConfigParser
except:
    import configparser as ConfigParser

from robot.api import logger

RDS_GLOBAL_BUNDLE_PATH = '/etc/ssl/certs/rds-global-bundle.pem'
RDS_HOST_SUFFIX = '.rds.amazonaws.com'


class ConnectionManager(object):
    """
    Connection Manager handles the connection & disconnection to the database.
    """

    def __init__(self):
        """
        Initializes _dbconnection to None.
        Added cache mode for multi connection use.
        Added to all method a new field, alias (Name of connection)
        """
        self._cache = robot.utils.ConnectionCache('No connection created')
        self.builtin = BuiltIn()

    def _push_cache(self, alias=None, connection=None, db_api_module_name=None):
        """
         Overlay _cache.register using dictionary
         Create a dictionary that contains the dbconnection and the api_module used
         and push it into the cache
        """
        logger.info('Connection Name: %s | Db Module: %s ' %
                    (alias, db_api_module_name))
        obj_dict = {'connection': connection, 'module': db_api_module_name}
        self._cache.register(obj_dict, alias=alias)

    def _get_cache(self, alias=None):
        """
         Overlay _cache.switch using dictionary
         Get from cache the dictionary contain dbconnection and api_module
         and return them
        """
        obj_dict = self._cache.switch(alias)
        dbconnection = obj_dict['connection']
        db_api_module_name = obj_dict['module']

        return dbconnection, db_api_module_name

    def _is_rds_hostname(self, db_host):
        return bool(db_host) and db_host.endswith(RDS_HOST_SUFFIX)

    def _get_rds_postgresql_ssl_mode(self, db_host):
        if self._is_rds_hostname(db_host):
            return 'verify-full'
        # Custom aliases to RDS will not match the AWS-issued hostname,
        # so verified TLS falls back to CA validation for those hosts.
        return 'verify-ca'

    def _get_connect_signature(self, db_api_2):
        try:
            return inspect.signature(db_api_2.connect)
        except (TypeError, ValueError):
            return None

    def _get_connection_host(self, db_api_2, connection_args, connection_kwargs):
        db_host = connection_kwargs.get('host') or connection_kwargs.get('hostname')
        if db_host:
            return db_host
        dsn = connection_kwargs.get('dsn')
        if dsn:
            return self._get_host_from_dsn(dsn)
        if connection_args:
            db_host = self._get_host_from_dsn(connection_args[0])
            if db_host:
                return db_host
        connect_signature = self._get_connect_signature(db_api_2)
        if connect_signature is None:
            return None
        try:
            bound_args = connect_signature.bind_partial(*connection_args, **connection_kwargs)
        except TypeError:
            return None
        return bound_args.arguments.get('host') or bound_args.arguments.get('hostname')

    def _get_host_from_dsn(self, dsn):
        if not isinstance(dsn, str):
            return None
        if '://' in dsn:
            data_connection = urlparse(dsn)
            if data_connection.hostname:
                return data_connection.hostname
        match = re.search(r'(?:^|\s)(?:host|hostname)\s*=\s*([^\s]+)', dsn)
        if match:
            return match.group(1)
        return None

    def _normalize_bool(self, value, default):
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in ['1', 'true', 'yes', 'on']

    def _get_mysql_tls_config(self, dbapiModuleName, db_api_2, dbHost,
                              ssl_ca=None, ssl_verify_cert=None, ssl_verify_identity=None):
        verify_cert = self._normalize_bool(ssl_verify_cert, True)
        verify_identity = self._normalize_bool(
            ssl_verify_identity, self._is_rds_hostname(dbHost))
        ssl_ca = ssl_ca or RDS_GLOBAL_BUNDLE_PATH
        if dbapiModuleName == "pymysql":
            # PyMySQL can enforce CA validation and hostname checks together
            # when the connection uses the canonical RDS endpoint name.
            tls_config = {
                'ssl': {
                    'ca': ssl_ca,
                    'cert_reqs': ssl.CERT_REQUIRED if verify_cert else ssl.CERT_NONE,
                }
            }
            connect_signature = self._get_connect_signature(db_api_2)
            if connect_signature is not None:
                if 'ssl_disabled' in connect_signature.parameters:
                    tls_config['ssl_disabled'] = False
                if 'ssl_verify_cert' in connect_signature.parameters:
                    tls_config['ssl_verify_cert'] = verify_cert
                if 'ssl_verify_identity' in connect_signature.parameters:
                    tls_config['ssl_verify_identity'] = verify_identity
                else:
                    # Older PyMySQL versions do not expose hostname checks here,
                    # so verified TLS falls back to CA validation for those drivers.
                    pass
            return tls_config
        # MySQLdb does not expose hostname verification through this API, so
        # the verified TLS fallback is CA validation against the RDS bundle.
        return {'ssl': {'ca': ssl_ca}}

    def _apply_tls_settings(self, dbapiModuleName, db_api_2, connection_args, connection_kwargs):
        dbHost = self._get_connection_host(db_api_2, connection_args, connection_kwargs)
        if dbapiModuleName == "psycopg2":
            # Force verified TLS for PostgreSQL with the AWS RDS CA bundle.
            connection_kwargs['sslmode'] = self._get_rds_postgresql_ssl_mode(dbHost)
            connection_kwargs['sslrootcert'] = RDS_GLOBAL_BUNDLE_PATH
        elif dbapiModuleName in ["MySQLdb", "pymysql"]:
            # Force verified TLS for MySQL-family drivers with the AWS RDS CA bundle.
            connection_kwargs.update(
                self._get_mysql_tls_config(dbapiModuleName, db_api_2, dbHost))
        return connection_kwargs

    def connect_to_database(self, dbapiModuleName=None, dbName=None, dbUsername=None, dbPassword=None, dbHost=None,
                            dbPort=None, dbCharset=None, dbConfigFile="./resources/db.cfg", url=None, alias=None,
                            ssl_ca=None, ssl_verify_cert=None, ssl_verify_identity=None,
                            sslmode=None, sslrootcert=None):
        """
        Loads the DB API 2.0 module given `dbapiModuleName` then uses it to
        connect to the database using `dbName`, `dbUsername`, and `dbPassword`.

        Optionally, you can specify a `dbConfigFile` wherein it will load the
        default property values for `dbapiModuleName`, `dbName` `dbUsername`
        and `dbPassword` (note: specifying `dbapiModuleName`, `dbName`
        `dbUsername` or `dbPassword` directly will override the properties of
        the same key in `dbConfigFile`). If no `dbConfigFile` is specified, it
        defaults to `./resources/db.cfg`.

        The `dbConfigFile` is useful if you don't want to check into your SCM
        your database credentials.

        Added new field alias 
        Added _cache.register to register given connection with alias

        Example db.cfg file
        | [default]
        | alias=aliasuwant
        | dbapiModuleName=pymysqlforexample
        | dbName=yourdbname
        | dbUsername=yourusername
        | dbPassword=yourpassword
        | dbHost=yourhost
        | dbPort=yourport

        Example usage:
        | # explicitly specifies all db property values |
        | Connect To Database | alias | psycopg2 | my_db | postgres | s3cr3t | tiger.foobar.com | 5432 |

        | # loads all property values from default.cfg |
        | Connect To Database | dbConfigFile=default.cfg |

        | # loads all property values from ./resources/db.cfg |
        | Connect To Database |

        | # uses explicit `dbapiModuleName` and `dbName` but uses the `dbUsername` and `dbPassword` in 'default.cfg' |
        | Connect To Database | alias | psycopg2 | my_db_test | dbConfigFile=default.cfg |

        | # uses explicit `dbapiModuleName` and `dbName` but uses the `dbUsername` and `dbPassword` in './resources/db.cfg' |
        | Connect To Database | alias | psycopg2 | my_db_test |
        """

        logger.info('Creating Db Connection using : alias=%s,url=%s dbapiModuleName=%s, dbName=%s, \
        dbUsername=%s, dbPassword=%s, dbHost=%s, dbPort=%s, dbCharset=%s, \
        dbConfigFile=%s ' % (alias, url, dbapiModuleName, dbName, dbUsername, dbPassword, dbHost, dbPort,
                             dbCharset, dbConfigFile))

        config = ConfigParser.ConfigParser()
        config.read([dbConfigFile])

        if not (url is None):
            dataConnection = urlparse(url)

        dbapiModuleName = dbapiModuleName or config.get(
            'default', 'dbapiModuleName')
        dbName = dbName or dataConnection.path[1:] or config.get(
            'default', 'dbName')
        dbUsername = dbUsername or dataConnection.username or config.get(
            'default', 'dbUsername')
        dbPassword = dbPassword if dbPassword is not None else \
            dataConnection.password if dataConnection.password is not None else \
            config.get('default', 'dbPassword')
        dbHost = dbHost or dataConnection.hostname or config.get(
            'default', 'dbHost') or 'localhost'
        dbPort = int(dbPort or dataConnection.port or config.get(
            'default', 'dbPort'))

        return self._connect_to_database(
            alias,
            dbapiModuleName,
            dbName,
            dbUsername,
            dbPassword,
            dbHost,
            dbPort,
            dbCharset,
            dbConfigFile,
            ssl_ca=ssl_ca,
            ssl_verify_cert=ssl_verify_cert,
            ssl_verify_identity=ssl_verify_identity,
            sslmode=sslmode,
            sslrootcert=sslrootcert)

    def _connect_to_database(self, alias, dbapiModuleName, dbName, dbUsername, dbPassword, dbHost, dbPort, dbCharset,
                             dbConfigFile="./resources/db.cfg", ssl_ca=None, ssl_verify_cert=None,
                             ssl_verify_identity=None, sslmode=None, sslrootcert=None):

        try:

            if dbapiModuleName == "excel" or dbapiModuleName == "excelrw":
                db_api_module_name = "pyodbc"
                db_api_2 = importlib.import_module("pyodbc")
            else:
                db_api_module_name = dbapiModuleName
                db_api_2 = importlib.import_module(dbapiModuleName)

            if dbapiModuleName in ["MySQLdb", "pymysql"]:
                dbPort = dbPort or 3306
                mysql_connection_args = {
                    'db': dbName,
                    'user': dbUsername,
                    'passwd': dbPassword,
                    'host': dbHost,
                    'port': dbPort,
                    'charset': dbCharset,
                }
                # Always require verified TLS when creating MySQL connections.
                mysql_connection_args.update(
                    self._get_mysql_tls_config(
                        dbapiModuleName, db_api_2, dbHost,
                        ssl_ca=ssl_ca,
                        ssl_verify_cert=ssl_verify_cert,
                        ssl_verify_identity=ssl_verify_identity))
                logger.info('Connecting using : %s.connect(db=%s, user=%s, passwd=%s, host=%s, port=%s, charset=%s, ssl=verified) ' %
                            (dbapiModuleName, dbName, dbUsername, dbPassword, dbHost, dbPort, dbCharset))
                dbconnection = db_api_2.connect(**mysql_connection_args)
            elif dbapiModuleName in ["psycopg2"]:
                dbPort = dbPort or 5432
                postgresql_ssl_mode = sslmode or self._get_rds_postgresql_ssl_mode(dbHost)
                postgresql_ssl_root_cert = sslrootcert or RDS_GLOBAL_BUNDLE_PATH
                logger.info('Connecting using : %s.connect(database=%s, user=%s, password=%s, host=%s, port=%s, sslmode=%s, sslrootcert=%s) ' %
                            (dbapiModuleName, dbName, dbUsername, dbPassword, dbHost, dbPort,
                             postgresql_ssl_mode, postgresql_ssl_root_cert))
                dbconnection = db_api_2.connect(
                    database=dbName,
                    user=dbUsername,
                    password=dbPassword,
                    host=dbHost,
                    port=dbPort,
                    sslmode=postgresql_ssl_mode,
                    sslrootcert=postgresql_ssl_root_cert)
            elif dbapiModuleName in ["pyodbc", "pypyodbc"]:
                dbPort = dbPort or 1433
                logger.info('Connecting using : %s.connect(DRIVER={SQL Server};SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s)' %
                            (dbapiModuleName, dbHost, dbPort, dbName, dbUsername, dbPassword))
                dbconnection = db_api_2.connect('DRIVER={SQL Server};SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s' %
                                                (dbHost, dbPort, dbName, dbUsername, dbPassword))
            elif dbapiModuleName in ["excel"]:
                logger.info(
                    'Connecting using : %s.connect(DRIVER={Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)};DBQ=%s;ReadOnly=1;'
                    'Extended Properties="Excel 8.0;HDR=YES";)' % (dbapiModuleName, dbName))
                dbconnection = db_api_2.connect(
                    'DRIVER={Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)};DBQ=%s;ReadOnly=1;Extended Properties="Excel 8.0;HDR=YES";)' % (
                        dbName), autocommit=True)
            elif dbapiModuleName in ["excelrw"]:
                logger.info(
                    'Connecting using : %s.connect(DRIVER={Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)};DBQ=%s;ReadOnly=0;'
                    'Extended Properties="Excel 8.0;HDR=YES";)' % (dbapiModuleName, dbName))
                dbconnection = db_api_2.connect(
                    'DRIVER={Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)};DBQ=%s;ReadOnly=0;Extended Properties="Excel 8.0;HDR=YES";)' % (
                        dbName), autocommit=True)
            elif dbapiModuleName in ["ibm_db", "ibm_db_dbi"]:
                dbPort = dbPort or 50000
                logger.info('Connecting using : %s.connect(DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;UID=%s;PWD=%s;) ' %
                            (dbapiModuleName, dbName, dbHost, dbPort, dbUsername, dbPassword))
                dbconnection = db_api_2.connect('DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;UID=%s;PWD=%s;' %
                                                (dbName, dbHost, dbPort, dbUsername, dbPassword), '', '')
            elif dbapiModuleName in ["cx_Oracle"]:
                dbPort = dbPort or 1521
                oracle_dsn = db_api_2.makedsn(
                    host=dbHost, port=dbPort, service_name=dbName)
                logger.info('Connecting using: %s.connect(user=%s, password=%s, dsn=%s) ' % (
                    dbapiModuleName, dbUsername, dbPassword, oracle_dsn))
                dbconnection = db_api_2.connect(
                    user=dbUsername, password=dbPassword, dsn=oracle_dsn)
            else:
                logger.info('Connecting using : %s.connect(database=%s, user=%s, password=%s, host=%s, port=%s) ' %
                            (dbapiModuleName, dbName, dbUsername, dbPassword, dbHost, dbPort))
                dbconnection = db_api_2.connect(
                    database=dbName, user=dbUsername, password=dbPassword, host=dbHost, port=dbPort)

            self._push_cache(alias, dbconnection, db_api_module_name)

        except Exception as Err:
            err_msg = ('DbConnection : %s : %s' % (alias, Err))
            raise AssertionError(err_msg)

    def connect_to_database_using_custom_params(self, dbapiModuleName=None, db_connect_string='', alias=None):

        logger.info('Creating Db Connection using : alias=%s, dbapiModuleName=%s, db_connect_string=%s' %
                    (alias, dbapiModuleName, db_connect_string))

        return self._connect_to_database_using_custom_params(
            alias=alias,
            dbapiModuleName=dbapiModuleName,
            db_connect_string=db_connect_string)

    def _connect_to_database_using_custom_params(self, alias=None, dbapiModuleName=None, db_connect_string=''):
        """
        Loads the DB API 2.0 module given `dbapiModuleName` then uses it to
        connect to the database using the map string `db_custom_param_string`.

        Added field alias

        Example usage:
        | # for psycopg2 |
        | Connect To Database Using Custom Params | alias | psycopg2 | database='my_db_test', user='postgres', password='s3cr3t', host='tiger.foobar.com', port=5432 |

        | # for JayDeBeApi |
        | Connect To Database Using Custom Params | alias | JayDeBeApi | 'oracle.jdbc.driver.OracleDriver', 'my_db_test', 'system', 's3cr3t' |
        """

        db_api_2 = importlib.import_module(dbapiModuleName)

        def secure_connect(*connection_args, **connection_kwargs):
            connection_kwargs = self._apply_tls_settings(
                dbapiModuleName, db_api_2, connection_args, connection_kwargs)
            return db_api_2.connect(*connection_args, **connection_kwargs)

        db_connect_string = 'secure_connect(%s)' % db_connect_string

        logger.info('Executing : Connect To Database Using Custom Params : %s.connect(%s) ' % (
            dbapiModuleName, db_connect_string))
        dbconnection = eval(db_connect_string, {'db_api_2': db_api_2, 'secure_connect': secure_connect})

        self._push_cache(alias, dbconnection, dbapiModuleName)

    def disconnect_from_database(self, alias=None):
        """
        Disconnects from the database.
        Added field alias

        For example:
        | Disconnect From Database | alias | # disconnects from current connection to the database |
        """
        logger.info('Executing : Disconnect From Database')
        connection, module_api = self._get_cache(alias)
        connection.close()

    def set_auto_commit(self, autoCommit=True, alias=None):
        """
        Turn the autocommit on the database connection ON or OFF. 

        The default behaviour on a newly created database connection is to automatically start a 
        transaction, which means that database actions that won't work if there is an active 
        transaction will fail. Common examples of these actions are creating or deleting a database 
        or database snapshot. By turning on auto commit on the database connection these actions 
        can be performed.

        Added field alias

        Example:
        | # Default behaviour, sets auto commit to true
        | Set Auto Commit |  alias
        | # Explicitly set the desired state
        | Set Auto Commit | alias | False
        """
        logger.info('Executing : Set Auto Commit')
        connection, module_api = self._get_cache(alias)
        connection.autocommit = autoCommit
        self._push_cache(alias, connection, module_api)
