# -*- coding: utf-8 -*-

ADMIN_MAIL = 'xx@ucloudlink.com'
TEST_MAIL = 'xx@ucloudlink.com'
LOCALHOST = '127.0.0.1'

mail_config = {
    'default': {
        'smtp_server': 'smtp.office365.com',
        'port': 587,
        'sender': 'xxx',
        'from': 'mail system',
        'password': 'xxx',
        'encrypt': True
    }
}

DB_SERVER_CONFIG = {
    'GSVC_MGO': {
        'host': 'xx',
        'port': 27017,
        'user': 'xx',
        'password': 'xx',
        'db_name': 'xx',
        'db_type': 'mongo'
    },
    "OSS_MGO": {
        'host': 'xx',
        'port': 27018,
        'user': 'xx',
        'password': 'xx',
        'db_name': 'xx',
        'db_type': 'mongo'
    },
    "PERF_MGO": {
        'host': 'xx',
        'port': 27019,
        'user': 'xx',
        'password': 'xx',
        'db_name': 'xx',
        'db_type': 'mongo'
    },

    "RMS_SQL": {
        'host': 'xx',
        'port': 55162,
        'user': 'xx',
        'password': 'xx',
        'db_name': 'xx',
        'db_type': 'mysql'
    },

    "CSS_SQL": {
        'host': 'xx',
        'port': 3306,
        'user': 'xx',
        'password': 'xx',
        'db_name': 'xx',
        'db_type': 'mysql'
    },

    "GSVC_SQL": {
        'host': 'xx',
        'port': 55192,
        'db_name': 'xx',
        'user': 'xx',
        'password': 'xx',
        'db_type': 'mysql'
    },
    
    "GSVC_SQL_ADMIN": {
        'host': 'xx',
        'port': 55162,
        'user': 'xx',
        'password': 'xx',
        'db_name': 'xx',
        'db_type': 'mysql'
    }
}

DB_LOCAL_CONFIG = {
    'GSVC_MGO': {
        'host': LOCALHOST,
        'port': 27017,
        'user': 'xx',
        'password': 'xx',
        'db_name': 'xx',
        'db_type': 'mongo'
    },
    'GSVC_MGO_ADMIN': {
        'host': LOCALHOST,
        'port': 27017,
        'user': 'xx',
        'password': 'xx',
        'db_name': 'xx',
        'db_type': 'mongo'
    },
    "OSS_MGO": {
        'host': LOCALHOST,
        'port': 27019,
        'user': 'xx',
        'password': 'xx',
        'db_name': 'xx',
        'db_type': 'mongo'
    },
    "PERF_MGO": {
        'host': LOCALHOST,
        'port': 27018,
        'user': 'xx',
        'password': 'xx',
        'db_name': 'xx',
        'db_type': 'mongo'
    },

    "RMS_SQL": {
        'host': 'xx',
        'port': 55162,
        'user': 'xx',
        'password': 'xx',
        'db_name': 'xx',
        'db_type': 'mysql'
    },

    "CSS_SQL": {
        'host': LOCALHOST,
        'port': 55161,
        'user': 'xx',
        'password': 'xx',
        'db_name': 'xx',
        'db_type': 'mysql'
    },

    "GSVC_SQL": {
        'host': 'xx',
        'port': 55162,
        'db_name': 'xx',
        'user': 'xx',
        'password': 'xx',
        'db_type': 'mysql'
    },

    "GSVC_SQL_ADMIN": {
        'host': 'xx',
        'port': 55162,
        'user': 'xx',
        'password': 'xx',
        'db_name': 'xx',
        'db_type': 'mysql'
    }
}
# db_config = DB_SERVER_CONFIG
db_config = DB_LOCAL_CONFIG
