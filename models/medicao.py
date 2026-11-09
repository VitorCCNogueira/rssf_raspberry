# models/medicao.py
from datetime import datetime, timedelta, timezone

import sqlite3
from config import DB_CONFIG

def _iso(time):
    return time.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


class MedicaoModel:
    SENSOR_COLUMNS = ('ds1', 'ds2', 'ds3', 'ds4', 'ds5', 'ds6')

    @staticmethod
    def salvar(data, gateway_id, mac = None, lote_id= None):
        # Ordem: DATA_HORA, gateway_id, id(sensor_id), s1, s2, s3, s4, s5, s6, rssi, mac, lote_id
        values = (
            _iso(datetime.now(timezone.utc)),
            gateway_id,
            data.get('senderAddress'),
            data.get('temp_ds1'), data.get('temp_ds2'),
            data.get('temp_ds3'), data.get('temp_ds4'),
            data.get('temp_ds5'), data.get('temp_ds6'),
            data.get('rssi'), mac, lote_id
        )

        conn = None
        cursor = None
        try:
            conn = sqlite3.connect(DB_CONFIG['path'],timeout=DB_CONFIG['timeout'])
            cursor = conn.cursor()

            sql = """INSERT INTO medicoes
                     (data_hora, gateway_id, sensor_id, temp_ds1, temp_ds2, temp_ds3, temp_ds4, temp_ds5, temp_ds6, rssi, mac, lote_id)
                     VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

            cursor.execute(sql, values)
            conn.commit()
            return True
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                if cursor:
                    cursor.close()
                conn.close()

    @staticmethod
    def salvar_lote(data, gateway_id, mac = None, lote_id= None):
        conn = None
        cursor = None
        try:
            conn = sqlite3.connect(DB_CONFIG['path'],timeout=DB_CONFIG['timeout'])
            cursor = conn.cursor()
            received_at = datetime.now(timezone.utc)
            values = [
                (_iso(received_at - timedelta(minutes=reading['idade_min'])),
                 gateway_id, data['senderAddress'],
                 *(reading[f'temp_ds{i}'] for i in range(1, 7)), data.get('rssi'),
                 mac, lote_id)
                 for reading in data['medicoes']
            ]
            cursor.executemany(
                """INSERT INTO medicoes
                   (data_hora, gateway_id, sensor_id, temp_ds1, temp_ds2,
                    temp_ds3, temp_ds4, temp_ds5, temp_ds6, rssi, mac, lote_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                values,
            )
            conn.commit()
            return True
        except Exception:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                if cursor:
                    cursor.close()
                conn.close()

    @staticmethod
    def received(mac,lote_id):
        intervalo_lim = _iso(datetime.now(timezone.utc) - timedelta(minutes=DB_CONFIG['dedup_min']))
        conn = sqlite3.connect(DB_CONFIG['path'],timeout=DB_CONFIG['timeout'])
        try:
            row = conn.execute(
                """SELECT 1 FROM medicoes
                WHERE mac = ? AND lote_id = ? AND data_hora >= ?
                LIMIT 1""",
                (mac,lote_id, intervalo_lim)).fetchone()
            return row is not None

        finally:
            conn.close()
            
    @staticmethod
    def pendentes():
        conn = sqlite3.connect(DB_CONFIG['path'],timeout=DB_CONFIG['timeout'])
        conn.row_factory = sqlite3.Row
        try:
            return conn.execute(
                """SELECT data_hora, gateway_id, sensor_id, temp_ds1, temp_ds2,
                    temp_ds3, temp_ds4, temp_ds5, temp_ds6, rssi, mac, lote_id
                    FROM medicoes
                    WHERE enviado_em IS NULL AND mac IS NOT NULL AND lote_id IS NOT NULL
                    ORDER BY mac, lote_id, data_hora""").fetchall()
            
        finally:
                conn.close()

    @staticmethod
    def marcar_enviado(mac,lote_id, valor):
        conn = sqlite3.connect(DB_CONFIG['path'],timeout=DB_CONFIG['timeout'])
        try:
            conn.execute(
                """UPDATE medicoes SET enviado_em = ?
                WHERE mac = ? AND lote_id = ? AND enviado_em IS NULL""", (valor, mac, lote_id))
            conn.commit()
        finally:
            conn.close()
    
        

