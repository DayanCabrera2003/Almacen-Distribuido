from almacen.cluster.channels import ChannelPool


def test_returns_the_same_channel_for_the_same_address():
    pool = ChannelPool()
    try:
        first = pool.channel("127.0.0.1:50051")
        second = pool.channel("127.0.0.1:50051")
        assert first is second
    finally:
        pool.close()


def test_returns_distinct_channels_for_distinct_addresses():
    pool = ChannelPool()
    try:
        assert pool.channel("127.0.0.1:50051") is not pool.channel("127.0.0.1:50052")
    finally:
        pool.close()


def test_close_is_idempotent():
    pool = ChannelPool()
    pool.channel("127.0.0.1:50051")
    pool.close()
    pool.close()  # must not raise


def test_channel_after_close_is_a_fresh_channel():
    pool = ChannelPool()
    first = pool.channel("127.0.0.1:50051")
    pool.close()
    second = pool.channel("127.0.0.1:50051")
    try:
        assert second is not first
    finally:
        pool.close()
