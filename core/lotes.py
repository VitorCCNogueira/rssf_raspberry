import math
import re
import config
from identity import canonical_gateway_id
from monitoring import point_profile

MAX_IDADE_MIN = 15 * 30 + 16 * 60              # gateway -> Pi (MQTT)
MAX_RETENCAO_MIN = 16 * 60                     # gateway -> Pi (MQTT)
MAX_IDADE_SYNC_MIN = MAX_IDADE_MIN + 2 * 24 * 60  # Pi -> nuvem (sync): lote pode esperar até 3 dias no Pi

class LoteInvalido(Exception): 
    pass

def is_valid_gateway_id(value):
    if not value or not isinstance(value, str):
        return False
    value = value.strip()
    return 0 < len(value) <= 64 and bool(re.match(r'^[\w-]+$', value))



def is_valid_number(value):
    return type(value) is not bool and isinstance(value, (int, float)) and math.isfinite(value)


def preparar_lote(data, max_idade=MAX_IDADE_MIN, max_retencao=MAX_RETENCAO_MIN):

    if not isinstance(data,dict):
        raise LoteInvalido('payload deve ser um objeto JSON')

    sender = data.get('senderAddress')
    if type(sender) is bool or not isinstance(sender, int) or not (1 <= sender <= 65534):
        raise LoteInvalido('senderAddress inválido')

    raw_gateway = data.get('gateway_id')
    if raw_gateway is not None and not is_valid_gateway_id(raw_gateway):
        raise LoteInvalido('gateway_id inválido')
    gateway_id = canonical_gateway_id(sender, raw_gateway.strip() if raw_gateway else None)
    gateway_id = gateway_id or config.DEFAULT_GATEWAY_ID
    laboratory = point_profile(sender, gateway_id)['mode'] == 'laboratory'

    for index in range(1, 7):
        temperature = data.get(f'temp_ds{index}')
        if temperature is not None and not is_valid_number(temperature):
            raise LoteInvalido(f'temp_ds{index} inválido')

    rssi = data.get('rssi')
    if rssi is not None and not is_valid_number(rssi):
        raise LoteInvalido('rssi inválido')

    if 'medicoes' in data:
        if not laboratory or raw_gateway != config.LAB_GATEWAY_ID:
            raise LoteInvalido('Lotes exigem um ponto de laboratorio configurado')
        medicoes = data['medicoes']
        if not isinstance(medicoes, list) or not 1 <= len(medicoes) <= 16:
            raise LoteInvalido('medicoes deve conter de 1 a 16 leituras')
        if any(f'temp_ds{i}' in data for i in range(1, 7)):
                raise LoteInvalido('Nao misture temperaturas avulsas e medicoes')
        ages = set()
        for medicao in medicoes:
            if not isinstance(medicao, dict):
                raise LoteInvalido('Medicao invalida')
            age = medicao.get('idade_min')
            if type(age) is not int or not 0 <= age <= max_idade or age in ages:
                raise LoteInvalido(f'idade_min deve ser inteiro unico entre 0 e {max_idade}')
            ages.add(age)
            for index in range(1, 7):
                key = f'temp_ds{index}'
                if key not in medicao or (medicao[key] is not None and not is_valid_number(medicao[key])):
                    raise LoteInvalido(f'{key} ausente ou invalido no lote')
        if max_retencao is not None and min(ages) > max_retencao:
            raise LoteInvalido('Lote mais antigo que a retencao maxima do gateway')
        # The lab has no probes. Never turn firmware error values into temperatures.
        for reading in medicoes:
            for index in range(1, 7):
                reading[f'temp_ds{index}'] = None
    else:
        if laboratory:
            for index in range(1, 7):
                data[f'temp_ds{index}'] = None
    return gateway_id