import logging
import os
import pytest

os.environ['TESTING'] = 'True'

logger = logging.getLogger(__name__)

from milvus_app import celery_app, db
import milvus_app.models

@pytest.fixture(scope='function')
def setup_function(request):
    def teardown_function():
        pass
    request.addfinalizer(teardown_function)
    db.drop_all()
    db.create_all()
