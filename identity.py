import config

LEGACY_GATEWAY_ID = 'gateway_legacy'
UNKNOWN_GATEWAY_ID = 'gateway_unknown'

NODE_GATEWAYS = {
    '1': 'gateway_node_1',
    '44204': 'gateway_node_2',
}


def canonical_gateway_id(sensor_id, gateway_id):
    """Map configured nodes to stable identities, including the lab receiver's fallback ID."""
    sensor_key = str(sensor_id) if sensor_id is not None else None
    if (config.LAB_SENSOR_ID and sensor_key == str(int(config.LAB_SENSOR_ID))
            and (sensor_key not in NODE_GATEWAYS or gateway_id == config.LAB_GATEWAY_ID)
            and gateway_id in (None, '', config.LAB_GATEWAY_ID, config.DEFAULT_GATEWAY_ID, LEGACY_GATEWAY_ID)):
        return config.LAB_GATEWAY_ID
    if gateway_id in (None, '', LEGACY_GATEWAY_ID) and sensor_key in NODE_GATEWAYS:
        return NODE_GATEWAYS[sensor_key]
    return gateway_id


def gateway_query_values(sensor_id, gateway_id):
    """Include that node's old rows when its canonical gateway is requested."""
    if not gateway_id:
        return ()
    sensor_key = str(sensor_id) if sensor_id is not None else None
    if (config.LAB_SENSOR_ID and sensor_key == str(int(config.LAB_SENSOR_ID))
            and (sensor_key not in NODE_GATEWAYS or gateway_id == config.LAB_GATEWAY_ID)
            and gateway_id == config.LAB_GATEWAY_ID):
        # Include rows written before gateway_unknown was normalized to LAB_GATEWAY_ID.
        return tuple(dict.fromkeys((gateway_id, config.LAB_GATEWAY_ID, UNKNOWN_GATEWAY_ID, LEGACY_GATEWAY_ID)))
    if sensor_key in NODE_GATEWAYS and gateway_id == NODE_GATEWAYS[sensor_key]:
        return (gateway_id, LEGACY_GATEWAY_ID)
    return (gateway_id,)
