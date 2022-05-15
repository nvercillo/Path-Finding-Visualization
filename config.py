
# this is the config file for dev only!!!
SQLALCHEMY_DATABASE_URI = "postgres://user:postgres@localhost:5432/doctor_reviews"
SQLALCHEMY_TRACK_MODIFICATIONS = False


# class Config(object):
#     DEBUG = False
#     TESTING = False
#     CSRF_ENABLED = True
#     SECRET_KEY = 'this-really-needs-to-be-changed'
#     SQLALCHEMY_DATABASE_URI = os.environ['DATABASE_URL']


# class ProductionConfig(Config):
#     DEBUG = False


# class StagingConfig(Config):
#     DEVELOPMENT = True
#     DEBUG = True


# class DevelopmentConfig(Config):
#     DEVELOPMENT = True
#     DEBUG = True


# class TestingConfig(Config):
#     TESTING = True