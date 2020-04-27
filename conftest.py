"""
    pytest fixtures that can be reused across tests.  the filename is required to be conftest.py
"""
import logging
import pytest
from pyspark.sql import SparkSession

# ensure env vars are set correctly
import findspark
findspark.init()

def quiet_py4j():
    # turns down spark logging for the test context
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)

@pytest.fixture(scope="session")
def spark_session(request):
    """
        Fixture for pytest that creates the Spark Context.
        Enables you to pytest your Pyspark funcs
    """
    spark_session = SparkSession.builder \
                        .master('local') \
                        .appName('fching-spark') \
                        .getOrCreate()
    
    request.addfinalizer(lambda: spark_session.stop())
    quiet_py4j()

    return spark_session
