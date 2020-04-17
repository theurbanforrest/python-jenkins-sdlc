from fching.aloha_world.aloha_world_functions import aloha_world

def test_aloha_world(spark_session):
    result = aloha_world()
    expected = 'aloha world'

    assert result == expected