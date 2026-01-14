"""
EV_REGISTRY - Sistema de Registro Seguro
Implementa API REST con HTTPS real, autenticación JWT y gestión de credenciales
Release 2 - Práctica SD 25/26
"""
import os
import json
import sqlite3
import secrets
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional
from flask import Flask, request, jsonify
from flask_cors import CORS
import jwt

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger('Registry')

app = Flask(__name__)
CORS(app)

# Configuración de seguridad
SECRET_KEY = os.getenv('JWT_SECRET_KEY', secrets.token_hex(32))
TOKEN_EXPIRATION_HOURS = 24

# Import para AES encryption
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from security.security_utils import CryptoManager

class RegistryDatabase:
    """Base de datos para el Registry"""
    def __init__(self, db_path='registry.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()
    
    def _init_schema(self):
        cursor = self.conn.cursor()
        
        # Tabla de CPs registrados
        cursor.execute('''CREATE TABLE IF NOT EXISTS charging_points (
            cp_id TEXT PRIMARY KEY,
            location TEXT NOT NULL,
            price REAL NOT NULL,
            registration_date INTEGER NOT NULL,
            last_auth INTEGER,
            status TEXT DEFAULT 'REGISTERED',
            created_at INTEGER DEFAULT (strftime('%s', 'now'))
        )''')
        
        # Tabla de credenciales (hasheadas)
        cursor.execute('''CREATE TABLE IF NOT EXISTS credentials (
            cp_id TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            salt TEXT NOT NULL,
            encryption_key TEXT NOT NULL,
            created_at INTEGER DEFAULT (strftime('%s', 'now')),
            FOREIGN KEY (cp_id) REFERENCES charging_points(cp_id) ON DELETE CASCADE
        )''')
        
        # Tabla de tokens activos
        cursor.execute('''CREATE TABLE IF NOT EXISTS active_tokens (
            cp_id TEXT PRIMARY KEY,
            token TEXT NOT NULL,
            issued_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            FOREIGN KEY (cp_id) REFERENCES charging_points(cp_id) ON DELETE CASCADE
        )''')
        
        self.conn.commit()
        logger.info("✅ Base de datos inicializada")
    
    def register_cp(self, cp_id: str, location: str, price: float) -> Dict:
        """Registrar un nuevo CP y generar credenciales"""
        cursor = self.conn.cursor()
        
        # Verificar si ya existe
        cursor.execute('SELECT cp_id FROM charging_points WHERE cp_id = ?', (cp_id,))
        if cursor.fetchone():
            return {'error': 'CP_ID ya registrado'}
        
        # Generar credenciales
        password = secrets.token_urlsafe(16)
        salt = secrets.token_hex(16)
        password_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), 
                                            salt.encode(), 100000).hex()
        
        # Generar clave de cifrado simétrico (AES-256)
        encryption_key = CryptoManager.generate_key()
        
        # Registrar CP
        cursor.execute('''INSERT INTO charging_points 
            (cp_id, location, price, registration_date, status)
            VALUES (?, ?, ?, ?, 'REGISTERED')''',
            (cp_id, location, price, int(datetime.now().timestamp())))
        
        # Guardar credenciales
        cursor.execute('''INSERT INTO credentials 
            (cp_id, password_hash, salt, encryption_key)
            VALUES (?, ?, ?, ?)''',
            (cp_id, password_hash, salt, encryption_key))
        
        self.conn.commit()
        
        logger.info(f"✅ CP {cp_id} registrado exitosamente")
        
        return {
            'cp_id': cp_id,
            'password': password,
            'message': '⚠️ GUARDE ESTA CONTRASEÑA - No se mostrará nuevamente'
        }
    
    def unregister_cp(self, cp_id: str) -> Dict:
        """Dar de baja un CP"""
        cursor = self.conn.cursor()
        
        cursor.execute('SELECT cp_id FROM charging_points WHERE cp_id = ?', (cp_id,))
        if not cursor.fetchone():
            return {'error': 'CP_ID no encontrado'}
        
        cursor.execute('DELETE FROM charging_points WHERE cp_id = ?', (cp_id,))
        self.conn.commit()
        
        logger.info(f"❌ CP {cp_id} dado de baja")
        return {'message': f'CP {cp_id} dado de baja exitosamente'}
    
    def authenticate_cp(self, cp_id: str, password: str) -> Optional[Dict]:
        """Autenticar un CP y devolver token + encryption_key"""
        cursor = self.conn.cursor()
        
        # Obtener credenciales
        cursor.execute('''SELECT c.password_hash, c.salt, c.encryption_key 
            FROM credentials c
            INNER JOIN charging_points cp ON c.cp_id = cp.cp_id
            WHERE c.cp_id = ? AND cp.status = 'REGISTERED' ''',
            (cp_id,))
        
        row = cursor.fetchone()
        if not row:
            logger.warning(f"⚠️ Intento de autenticación fallido: {cp_id}")
            return None
        
        # Verificar contraseña
        password_hash = hashlib.pbkdf2_hmac('sha256', password.encode(),
                                            row['salt'].encode(), 100000).hex()
        
        if password_hash != row['password_hash']:
            logger.warning(f"⚠️ Contraseña incorrecta para {cp_id}")
            return None
        
        # Generar JWT token
        payload = {
            'cp_id': cp_id,
            'iat': datetime.utcnow(),
            'exp': datetime.utcnow() + timedelta(hours=TOKEN_EXPIRATION_HOURS)
        }
        token = jwt.encode(payload, SECRET_KEY, algorithm='HS256')
        
        # Guardar token activo
        cursor.execute('''INSERT OR REPLACE INTO active_tokens 
            (cp_id, token, issued_at, expires_at)
            VALUES (?, ?, ?, ?)''',
            (cp_id, token, int(datetime.now().timestamp()),
             int((datetime.now() + timedelta(hours=TOKEN_EXPIRATION_HOURS)).timestamp())))
        
        # Actualizar última autenticación
        cursor.execute('UPDATE charging_points SET last_auth = ? WHERE cp_id = ?',
                      (int(datetime.now().timestamp()), cp_id))
        
        self.conn.commit()
        
        logger.info(f"✅ CP {cp_id} autenticado exitosamente")
        
        return {
            'cp_id': cp_id,
            'token': token,
            'encryption_key': row['encryption_key'],
            'expires_in': TOKEN_EXPIRATION_HOURS * 3600
        }
    
    def verify_token(self, token: str) -> Optional[str]:
        """Verificar si un token es válido y devolver cp_id"""
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            cp_id = payload.get('cp_id')
            
            # Verificar que el token esté en la base de datos
            cursor = self.conn.cursor()
            cursor.execute('SELECT cp_id FROM active_tokens WHERE cp_id = ? AND token = ?',
                          (cp_id, token))
            
            if cursor.fetchone():
                return cp_id
            return None
        except jwt.ExpiredSignatureError:
            logger.warning("⚠️ Token expirado")
            return None
        except jwt.InvalidTokenError:
            logger.warning("⚠️ Token inválido")
            return None
    
    def get_all_cps(self) -> list:
        """Obtener todos los CPs registrados"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT cp_id, location, price, registration_date, last_auth, status FROM charging_points')
        return [dict(row) for row in cursor.fetchall()]
    
    def revoke_token(self, cp_id: str):
        """Revocar el token de un CP (forzar re-autenticación)"""
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM active_tokens WHERE cp_id = ?', (cp_id,))
        self.conn.commit()
        logger.info(f"🔒 Token de {cp_id} revocado")

# Instancia global de la base de datos
db = RegistryDatabase()

# API REST ENDPOINTS

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de salud"""
    return jsonify({'status': 'healthy', 'service': 'EV_Registry'}), 200

@app.route('/api/v1/register', methods=['POST'])
def register_endpoint():
    """
    POST /api/v1/register
    Body: { "cp_id": "CP001", "location": "Madrid Centro", "price": 0.50 }
    Returns: { "cp_id": "CP001", "password": "xxx", "message": "..." }
    """
    try:
        data = request.get_json()
        
        if not data or 'cp_id' not in data or 'location' not in data or 'price' not in data:
            return jsonify({'error': 'Faltan campos requeridos: cp_id, location, price'}), 400
        
        result = db.register_cp(data['cp_id'], data['location'], float(data['price']))
        
        if 'error' in result:
            return jsonify(result), 409  # Conflict
        
        return jsonify(result), 201
    except Exception as e:
        logger.error(f"❌ Error en registro: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/unregister/<cp_id>', methods=['DELETE'])
def unregister_endpoint(cp_id: str):
    """
    DELETE /api/v1/unregister/<cp_id>
    Returns: { "message": "..." }
    """
    try:
        result = db.unregister_cp(cp_id)
        
        if 'error' in result:
            return jsonify(result), 404
        
        return jsonify(result), 200
    except Exception as e:
        logger.error(f"❌ Error en baja: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/authenticate', methods=['POST'])
def authenticate_endpoint():
    """
    POST /api/v1/authenticate
    Body: { "cp_id": "CP001", "password": "xxx" }
    Returns: { "cp_id": "CP001", "token": "JWT...", "encryption_key": "xxx", "expires_in": 86400 }
    """
    try:
        data = request.get_json()
        
        if not data or 'cp_id' not in data or 'password' not in data:
            return jsonify({'error': 'Faltan campos: cp_id, password'}), 400
        
        result = db.authenticate_cp(data['cp_id'], data['password'])
        
        if not result:
            return jsonify({'error': 'Autenticación fallida'}), 401
        
        return jsonify(result), 200
    except Exception as e:
        logger.error(f"❌ Error en autenticación: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/verify', methods=['POST'])
def verify_token_endpoint():
    """
    POST /api/v1/verify
    Body: { "token": "JWT..." }
    Returns: { "valid": true, "cp_id": "CP001" }
    """
    try:
        data = request.get_json()
        
        if not data or 'token' not in data:
            return jsonify({'error': 'Falta campo: token'}), 400
        
        cp_id = db.verify_token(data['token'])
        
        if cp_id:
            return jsonify({'valid': True, 'cp_id': cp_id}), 200
        else:
            return jsonify({'valid': False}), 401
    except Exception as e:
        logger.error(f"❌ Error en verificación: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/cps', methods=['GET'])
def list_cps_endpoint():
    """
    GET /api/v1/cps
    Returns: [ { "cp_id": "CP001", "location": "...", ... }, ... ]
    """
    try:
        cps = db.get_all_cps()
        return jsonify(cps), 200
    except Exception as e:
        logger.error(f"❌ Error listando CPs: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/revoke/<cp_id>', methods=['POST'])
def revoke_token_endpoint(cp_id: str):
    """
    POST /api/v1/revoke/<cp_id>
    Revoca el token de un CP (para forzar re-autenticación)
    """
    try:
        db.revoke_token(cp_id)
        return jsonify({'message': f'Token de {cp_id} revocado'}), 200
    except Exception as e:
        logger.error(f"❌ Error revocando token: {e}")
        return jsonify({'error': str(e)}), 500

# ============================================================================
# MAIN - Arranque con HTTPS usando adhoc SSL
# ============================================================================

if __name__ == '__main__':
    logger.info("="*60)
    logger.info("EV_REGISTRY - Sistema de Registro Seguro")
    logger.info("Release 2 - Práctica SD 25/26")
    logger.info("="*60)
    
    port = int(os.getenv('REGISTRY_PORT', 8443))
    
    logger.info(f"🚀 Registry escuchando en https://0.0.0.0:{port}")
    logger.info("📋 Endpoints disponibles:")
    logger.info("   POST   /api/v1/register      - Registrar CP")
    logger.info("   DELETE /api/v1/unregister/:id - Dar de baja CP")
    logger.info("   POST   /api/v1/authenticate  - Autenticar CP")
    logger.info("   POST   /api/v1/verify        - Verificar token")
    logger.info("   GET    /api/v1/cps           - Listar CPs")
    logger.info("   POST   /api/v1/revoke/:id    - Revocar token")
    logger.info("="*60)
    logger.info("⚠️  Usando certificado autofirmado (adhoc)")
    logger.info("="*60)
    
    # Arrancar servidor HTTPS con certificado adhoc de Flask
    # Para producción, usar certificados reales
    app.run(host='0.0.0.0', port=port, ssl_context='adhoc', debug=False)