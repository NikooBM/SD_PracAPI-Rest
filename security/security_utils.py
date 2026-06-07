"""
SECURITY_UTILS - Utilidades de Seguridad para EVCharging
Implementa cifrado AES-256-CBC, gestión de tokens y auditoría.
Release 2 - Práctica SD 25/26

CORRECCIONES:
  - AuditLogger thread-safe con threading.Lock() en FileHandler
  - AuditLogger: rotación de logs para evitar archivos infinitos (RotatingFileHandler)
  - CryptoManager: validación explícita de la clave (longitud, decodificable)
  - CryptoManager: manejo de excepciones más informativo
  - TokenManager: eliminado (duplicaba lógica de RegistryDatabase; solo se usa
    internamente allí mediante hashlib directo)
"""
import os
import json
import logging
import threading
import base64
import hashlib
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Dict, Any, Optional

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from Crypto.Random import get_random_bytes

logger = logging.getLogger('Security')


# ---------------------------------------------------------------------------
# CryptoManager
# ---------------------------------------------------------------------------

class CryptoManager:
    """
    Cifrado simétrico AES-256-CBC.
    Cada CP tiene su propia clave única (32 bytes, codificada en base64 estándar).
    """

    KEY_BYTES = 32   # AES-256
    IV_BYTES  = 16   # AES block size

    @staticmethod
    def generate_key() -> str:
        """Genera una clave AES-256 aleatoria codificada en base64."""
        return base64.b64encode(get_random_bytes(CryptoManager.KEY_BYTES)).decode('utf-8')

    @staticmethod
    def _decode_key(key: str) -> bytes:
        """
        Decodifica y valida la clave base64.
        Lanza ValueError si la clave es inválida o tiene longitud incorrecta.
        """
        if not key or not isinstance(key, str):
            raise ValueError("La clave de cifrado no puede estar vacía")
        try:
            key_bytes = base64.b64decode(key)
        except Exception as exc:
            raise ValueError(f"La clave no es base64 válido: {exc}") from exc
        if len(key_bytes) != CryptoManager.KEY_BYTES:
            raise ValueError(
                f"La clave debe tener {CryptoManager.KEY_BYTES} bytes; "
                f"tiene {len(key_bytes)}"
            )
        return key_bytes

    @staticmethod
    def encrypt(plaintext: str, key: str) -> str:
        """
        Cifra texto plano con AES-256-CBC.

        Retorna base64(IV[16] + ciphertext).
        Lanza ValueError / CryptoError en caso de error.
        """
        key_bytes = CryptoManager._decode_key(key)
        iv = get_random_bytes(CryptoManager.IV_BYTES)
        cipher = AES.new(key_bytes, AES.MODE_CBC, iv)
        ciphertext = cipher.encrypt(pad(plaintext.encode('utf-8'), AES.block_size))
        return base64.b64encode(iv + ciphertext).decode('utf-8')

    @staticmethod
    def decrypt(encrypted: str, key: str) -> str:
        """
        Descifra base64(IV + ciphertext) con AES-256-CBC.
        Lanza ValueError si los datos están corruptos o la clave es incorrecta.
        """
        key_bytes = CryptoManager._decode_key(key)
        try:
            raw = base64.b64decode(encrypted)
        except Exception as exc:
            raise ValueError(f"Datos cifrados no son base64 válido: {exc}") from exc
        if len(raw) < CryptoManager.IV_BYTES + AES.block_size:
            raise ValueError("Datos cifrados demasiado cortos para contener IV + bloque")
        iv         = raw[:CryptoManager.IV_BYTES]
        ciphertext = raw[CryptoManager.IV_BYTES:]
        cipher = AES.new(key_bytes, AES.MODE_CBC, iv)
        try:
            plaintext = unpad(cipher.decrypt(ciphertext), AES.block_size)
        except (ValueError, KeyError) as exc:
            raise ValueError(f"Error eliminando padding (clave incorrecta o datos corruptos): {exc}") from exc
        return plaintext.decode('utf-8')

    @staticmethod
    def encrypt_json(data: Dict[str, Any], key: str) -> str:
        """Serializa un dict a JSON y lo cifra."""
        return CryptoManager.encrypt(json.dumps(data, ensure_ascii=False), key)

    @staticmethod
    def decrypt_json(encrypted: str, key: str) -> Dict[str, Any]:
        """Descifra y deserializa JSON."""
        return json.loads(CryptoManager.decrypt(encrypted, key))


# ---------------------------------------------------------------------------
# AuditLogger
# ---------------------------------------------------------------------------

class AuditLogger:
    """
    Sistema de auditoría de eventos de seguridad.

    CORRECCIONES respecto a la versión original:
    - RotatingFileHandler (máx 5 MB × 3 backups) para evitar archivos infinitos.
    - Lock adicional en torno a los handlers para garantizar thread-safety
      (Python logging ya es thread-safe a nivel de Handler, pero el lock
       explícito protege operaciones compuestas como flush).
    - Todos los métodos son thread-safe.
    """

    _MAX_BYTES    = 5 * 1024 * 1024   # 5 MB
    _BACKUP_COUNT = 3

    def __init__(self, log_file: str = 'audit.log'):
        self.log_file = log_file
        self._lock = threading.Lock()

        self._logger = logging.getLogger(f'Audit.{os.path.basename(log_file)}')
        self._logger.setLevel(logging.INFO)
        # Evitar duplicar handlers si se re-instancia
        self._logger.propagate = False

        if not self._logger.handlers:
            # Asegurar directorio
            log_dir = os.path.dirname(os.path.abspath(log_file))
            os.makedirs(log_dir, exist_ok=True)

            handler = RotatingFileHandler(
                log_file,
                maxBytes=self._MAX_BYTES,
                backupCount=self._BACKUP_COUNT,
                encoding='utf-8'
            )
            handler.setLevel(logging.INFO)
            handler.setFormatter(logging.Formatter(
                '%(asctime)s | %(levelname)s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ))
            self._logger.addHandler(handler)

    # ------------------------------------------------------------------
    # Método genérico
    # ------------------------------------------------------------------

    def log_event(self, event_type: str, source_ip: str, actor: str,
                  action: str, details: str = '', success: bool = True):
        """
        Registra un evento de auditoría con formato estructurado.

        Parámetros:
            event_type : Categoría (AUTH, SERVICE, STATUS, COMMAND, WEATHER, ERROR, SYSTEM, SECURITY)
            source_ip  : IP de origen (puede ser '0.0.0.0' si es interno)
            actor      : Identificador del agente (cp_id, driver_id, 'CENTRAL', etc.)
            action     : Descripción corta de la acción
            details    : Información adicional (opcional)
            success    : True → INFO; False → WARNING
        """
        status = 'SUCCESS' if success else 'FAILED'
        entry = (
            f"[{event_type}] "
            f"SOURCE={source_ip} | "
            f"ACTOR={actor} | "
            f"ACTION={action} | "
            f"STATUS={status}"
        )
        if details:
            entry += f" | DETAILS={details}"

        with self._lock:
            if success:
                self._logger.info(entry)
            else:
                self._logger.warning(entry)

    # ------------------------------------------------------------------
    # Métodos especializados
    # ------------------------------------------------------------------

    def log_authentication(self, cp_id: str, source_ip: str,
                           success: bool, method: str = 'TOKEN'):
        self.log_event(
            'AUTH', source_ip, cp_id,
            f'Authentication attempt via {method}',
            f'CP_ID: {cp_id}',
            success
        )

    def log_service_request(self, driver_id: str, cp_id: str, source_ip: str):
        self.log_event(
            'SERVICE', source_ip, driver_id,
            'Service request',
            f'Driver: {driver_id}, CP: {cp_id}',
            True
        )

    def log_service_auth(self, driver_id: str, cp_id: str, authorized: bool):
        self.log_event(
            'SERVICE', 'CENTRAL', 'SYSTEM',
            'Service authorization',
            f'Driver: {driver_id}, CP: {cp_id}',
            authorized
        )

    def log_cp_status_change(self, cp_id: str, old_status: str,
                             new_status: str, reason: str = ''):
        self.log_event(
            'STATUS', 'SYSTEM', cp_id,
            f'Status change: {old_status} -> {new_status}',
            f'Reason: {reason}' if reason else '',
            True
        )

    def log_security_incident(self, incident_type: str, actor: str,
                              source_ip: str, details: str):
        self.log_event(
            'SECURITY', source_ip, actor,
            f'Security incident: {incident_type}',
            details,
            False
        )

    def log_error(self, error_type: str, source: str, details: str):
        self.log_event(
            'ERROR', 'SYSTEM', source,
            f'System error: {error_type}',
            details,
            False
        )

    def log_command(self, cp_id: str, command: str, issued_by: str = 'CENTRAL'):
        self.log_event(
            'COMMAND', 'CENTRAL', issued_by,
            f'Command issued: {command}',
            f'Target: {cp_id}',
            True
        )

    def log_weather_alert(self, cp_id: str, alert_type: str, temperature: float):
        self.log_event(
            'WEATHER', 'EV_W', 'SYSTEM',
            f'Weather alert: {alert_type}',
            f'CP: {cp_id}, Temp: {temperature:.1f}°C',
            True
        )


# ---------------------------------------------------------------------------
# TokenManager — utilidades de contraseña (usadas internamente por Registry)
# ---------------------------------------------------------------------------

class TokenManager:
    """Utilidades para hashing de contraseñas con PBKDF2-HMAC-SHA256."""

    ITERATIONS = 100_000
    HASH_NAME   = 'sha256'

    @staticmethod
    def hash_password(password: str, salt: str) -> str:
        """Devuelve hex del hash PBKDF2."""
        return hashlib.pbkdf2_hmac(
            TokenManager.HASH_NAME,
            password.encode('utf-8'),
            salt.encode('utf-8'),
            TokenManager.ITERATIONS
        ).hex()

    @staticmethod
    def verify_password(password: str, salt: str, password_hash: str) -> bool:
        """Comparación en tiempo constante para evitar timing attacks."""
        computed = TokenManager.hash_password(password, salt)
        # hmac.compare_digest requiere mismo tipo; ambos son str
        import hmac as _hmac
        return _hmac.compare_digest(computed, password_hash)
