"""
SECURITY_UTILS - Utilidades de Seguridad para EVCharging
Implementa cifrado AES-256, gestión de tokens y auditoría
Release 2 - Práctica SD 25/26
"""
import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from Crypto.Random import get_random_bytes
import base64
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Security')

class CryptoManager:
    """
    Gestión de cifrado simétrico AES-256-CBC
    Cada CP tiene su propia clave de cifrado única
    """
    
    @staticmethod
    def generate_key() -> str:
        """Generar una clave AES-256 (32 bytes = 256 bits)"""
        return base64.b64encode(get_random_bytes(32)).decode('utf-8')
    
    @staticmethod
    def encrypt(plaintext: str, key: str) -> str:
        """
        Cifrar texto con AES-256-CBC
        
        Args:
            plaintext: Texto en claro
            key: Clave de cifrado (base64)
        
        Returns:
            Texto cifrado en formato: IV + ciphertext (base64)
        """
        try:
            # Decodificar la clave
            key_bytes = base64.b64decode(key)
            
            # Generar IV aleatorio
            iv = get_random_bytes(16)
            
            # Crear cipher
            cipher = AES.new(key_bytes, AES.MODE_CBC, iv)
            
            # Cifrar con padding
            ciphertext = cipher.encrypt(pad(plaintext.encode('utf-8'), AES.block_size))
            
            # Concatenar IV + ciphertext y codificar en base64
            encrypted = base64.b64encode(iv + ciphertext).decode('utf-8')
            
            return encrypted
        except Exception as e:
            logger.error(f"❌ Error cifrando: {e}")
            raise
    
    @staticmethod
    def decrypt(encrypted: str, key: str) -> str:
        """
        Descifrar texto con AES-256-CBC
        
        Args:
            encrypted: Texto cifrado (base64)
            key: Clave de descifrado (base64)
        
        Returns:
            Texto en claro
        """
        try:
            # Decodificar la clave
            key_bytes = base64.b64decode(key)
            
            # Decodificar el mensaje cifrado
            encrypted_data = base64.b64decode(encrypted)
            
            # Extraer IV (primeros 16 bytes)
            iv = encrypted_data[:16]
            ciphertext = encrypted_data[16:]
            
            # Crear cipher
            cipher = AES.new(key_bytes, AES.MODE_CBC, iv)
            
            # Descifrar y quitar padding
            plaintext = unpad(cipher.decrypt(ciphertext), AES.block_size)
            
            return plaintext.decode('utf-8')
        except Exception as e:
            logger.error(f"❌ Error descifrando: {e}")
            raise
    
    @staticmethod
    def encrypt_json(data: Dict[str, Any], key: str) -> str:
        """Cifrar un diccionario JSON"""
        json_str = json.dumps(data)
        return CryptoManager.encrypt(json_str, key)
    
    @staticmethod
    def decrypt_json(encrypted: str, key: str) -> Dict[str, Any]:
        """Descifrar y parsear JSON"""
        json_str = CryptoManager.decrypt(encrypted, key)
        return json.loads(json_str)


class AuditLogger:
    """
    Sistema de Auditoría para eventos de seguridad
    Registra todos los eventos importantes del sistema
    """
    
    def __init__(self, log_file: str = 'audit.log'):
        self.log_file = log_file
        
        # Configurar logger específico para auditoría
        self.logger = logging.getLogger('Audit')
        self.logger.setLevel(logging.INFO)
        
        # Handler para archivo
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Formato detallado
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
    
    def log_event(self, event_type: str, source_ip: str, actor: str, 
                  action: str, details: str = "", success: bool = True):
        """
        Registrar un evento de auditoría
        
        Args:
            event_type: Tipo de evento (AUTH, SERVICE, ERROR, SECURITY, etc.)
            source_ip: IP de origen del evento
            actor: Quién genera el evento (CP_ID, Driver_ID, User)
            action: Acción realizada
            details: Detalles adicionales
            success: Si la acción fue exitosa
        """
        status = "SUCCESS" if success else "FAILED"
        
        log_entry = (
            f"[{event_type}] "
            f"SOURCE={source_ip} | "
            f"ACTOR={actor} | "
            f"ACTION={action} | "
            f"STATUS={status} | "
            f"DETAILS={details}"
        )
        
        if success:
            self.logger.info(log_entry)
        else:
            self.logger.warning(log_entry)
    
    def log_authentication(self, cp_id: str, source_ip: str, success: bool, 
                          method: str = "TOKEN"):
        """Registrar intento de autenticación"""
        action = f"Authentication attempt via {method}"
        details = f"CP_ID: {cp_id}"
        self.log_event('AUTH', source_ip, cp_id, action, details, success)
    
    def log_service_request(self, driver_id: str, cp_id: str, source_ip: str):
        """Registrar solicitud de servicio"""
        action = "Service request"
        details = f"Driver: {driver_id}, CP: {cp_id}"
        self.log_event('SERVICE', source_ip, driver_id, action, details, True)
    
    def log_service_auth(self, driver_id: str, cp_id: str, authorized: bool):
        """Registrar autorización de servicio"""
        action = "Service authorization"
        details = f"Driver: {driver_id}, CP: {cp_id}"
        self.log_event('SERVICE', 'CENTRAL', 'SYSTEM', action, details, authorized)
    
    def log_cp_status_change(self, cp_id: str, old_status: str, new_status: str, 
                            reason: str = ""):
        """Registrar cambio de estado de CP"""
        action = f"Status change: {old_status} -> {new_status}"
        details = f"Reason: {reason}" if reason else ""
        self.log_event('STATUS', 'SYSTEM', cp_id, action, details, True)
    
    def log_security_incident(self, incident_type: str, actor: str, 
                             source_ip: str, details: str):
        """Registrar incidente de seguridad"""
        action = f"Security incident: {incident_type}"
        self.log_event('SECURITY', source_ip, actor, action, details, False)
    
    def log_error(self, error_type: str, source: str, details: str):
        """Registrar error del sistema"""
        action = f"System error: {error_type}"
        self.log_event('ERROR', 'SYSTEM', source, action, details, False)
    
    def log_command(self, cp_id: str, command: str, issued_by: str = "CENTRAL"):
        """Registrar comando enviado a CP"""
        action = f"Command issued: {command}"
        details = f"Target: {cp_id}"
        self.log_event('COMMAND', 'CENTRAL', issued_by, action, details, True)
    
    def log_weather_alert(self, cp_id: str, alert_type: str, temperature: float):
        """Registrar alerta climática"""
        action = f"Weather alert: {alert_type}"
        details = f"CP: {cp_id}, Temp: {temperature}°C"
        self.log_event('WEATHER', 'EV_W', 'SYSTEM', action, details, True)


class TokenManager:
    """
    Gestión simple de tokens para autenticación
    (Simplificado - en producción usar JWT con más validaciones)
    """
    
    @staticmethod
    def hash_password(password: str, salt: str) -> str:
        """Hashear contraseña con salt"""
        return hashlib.pbkdf2_hmac('sha256', password.encode(), 
                                   salt.encode(), 100000).hex()
    
    @staticmethod
    def verify_password(password: str, salt: str, password_hash: str) -> bool:
        """Verificar contraseña"""
        computed_hash = TokenManager.hash_password(password, salt)
        return computed_hash == password_hash
