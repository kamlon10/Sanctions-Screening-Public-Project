# -*- coding: utf-8 -*-
import sqlite3
import logging
import re
import io
import csv
from flask import Flask, jsonify, render_template, request, Response
from flask_cors import CORS
from thefuzz import fuzz

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__, template_folder='.')
CORS(app)

DB_FILE = "sanctions_lists.db"

def conectar_db():
    """Conecta a la base de datos SQLite."""
    try:
        # check_same_thread=False es necesario porque Flask puede manejar peticiones en diferentes hilos.
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error al conectar a la base de datos SQLite: {e}")
        return None

def normalize_string(s):
    """Normaliza un string para la comparación difusa."""
    if not s: return ""
    s = s.lower()
    s = re.sub(r'[^\w\s]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def get_full_entity_details(cursor, uid):
    """Obtiene todos los detalles de una entidad a partir de su UID."""
    cursor.execute("SELECT * FROM Entidades WHERE uid = ?", (uid,))
    entidad_data = cursor.fetchone()
    if not entidad_data: return None

    entidad_completa = dict(entidad_data)
    
    cursor.execute("SELECT * FROM Alias WHERE entidad_uid = ?", (uid,))
    entidad_completa['aliases'] = [dict(row) for row in cursor.fetchall()]
    cursor.execute("SELECT * FROM Direcciones WHERE entidad_uid = ?", (uid,))
    entidad_completa['direcciones'] = [dict(row) for row in cursor.fetchall()]
    cursor.execute("SELECT * FROM Programas WHERE entidad_uid = ?", (uid,))
    entidad_completa['programas'] = [row['programa'] for row in cursor.fetchall()]
    cursor.execute("SELECT * FROM Identificadores WHERE entidad_uid = ?", (uid,))
    entidad_completa['identificadores'] = [dict(row) for row in cursor.fetchall()]
    cursor.execute("SELECT * FROM CaracteristicasAdicionales WHERE entidad_uid = ?", (uid,))
    entidad_completa['caracteristicas'] = [dict(row) for row in cursor.fetchall()]
    
    return entidad_completa

def perform_database_search(search_params):
    """Función central que ejecuta la lógica de búsqueda y devuelve los resultados."""
    conn = conectar_db()
    if not conn:
        raise ConnectionError("No se pudo conectar a la base de datos")

    try:
        cursor = conn.cursor()
        uids_from_name_search = None
        scores_map = {}

        query_name = search_params.get('name')
        exclude_aliases = search_params.get('exclude_aliases', False)

        if query_name:
            if search_params.get('is_exact_search'):
                sql = "SELECT DISTINCT uid FROM Entidades WHERE nombre_principal = ?"
                sql_params = [query_name]
                if not exclude_aliases:
                    sql = "SELECT DISTINCT e.uid FROM Entidades e LEFT JOIN Alias a ON e.uid = a.entidad_uid WHERE e.nombre_principal = ? OR a.nombre_alias = ?"
                    sql_params = [query_name, query_name]
                cursor.execute(sql, sql_params)
                uids_from_name_search = [row['uid'] for row in cursor.fetchall()]
            else: # Fuzzy Search
                if exclude_aliases:
                    cursor.execute("SELECT uid, nombre_principal FROM Entidades")
                else:
                    cursor.execute("SELECT e.uid, e.nombre_principal, a.nombre_alias FROM Entidades e LEFT JOIN Alias a ON e.uid = a.entidad_uid")
                
                all_names_from_db = cursor.fetchall()
                names_by_uid = {}
                for row in all_names_from_db:
                    uid = row['uid']
                    if uid not in names_by_uid: names_by_uid[uid] = []
                    if row['nombre_principal'] and row['nombre_principal'] not in names_by_uid[uid]: names_by_uid[uid].append(row['nombre_principal'])
                    if not exclude_aliases and 'nombre_alias' in row.keys() and row['nombre_alias'] and row['nombre_alias'] not in names_by_uid[uid]:
                        names_by_uid[uid].append(row['nombre_alias'])

                normalized_query = normalize_string(query_name)
                matches = []
                for uid, names in names_by_uid.items():
                    best_match_in_entity = {'score': 0, 'name': ''}
                    for name in names:
                        score = fuzz.token_sort_ratio(normalized_query, normalize_string(name))
                        if score > best_match_in_entity['score']:
                            best_match_in_entity['score'] = score
                            best_match_in_entity['name'] = name
                    
                    if best_match_in_entity['score'] >= search_params.get('threshold', 80):
                        matches.append({'uid': uid, 'score': best_match_in_entity['score'], 'matched_on': best_match_in_entity['name']})
                
                matches.sort(key=lambda x: x['score'], reverse=True)
                uids_from_name_search = [match['uid'] for match in matches]
                scores_map = {m['uid']: {'score': m['score'], 'matched_on': m['matched_on']} for m in matches}

        base_query = "SELECT DISTINCT e.uid FROM Entidades e"
        joins, conditions, params = set(), [], []

        if uids_from_name_search is not None:
            if not uids_from_name_search: return []
            placeholders = ','.join('?' for _ in uids_from_name_search)
            conditions.append(f"e.uid IN ({placeholders})")
            params.extend(uids_from_name_search)

        if search_params.get('dob'):
            joins.add("LEFT JOIN CaracteristicasAdicionales ca_dob ON e.uid = ca_dob.entidad_uid")
            conditions.append("(ca_dob.tipo_caracteristica LIKE '%Date of Birth%' AND ca_dob.valor_caracteristica LIKE ?)")
            params.append(f"%{search_params.get('dob')}%")
        
        if search_params.get('nationality'):
            joins.add("LEFT JOIN CaracteristicasAdicionales ca_nat ON e.uid = ca_nat.entidad_uid")
            conditions.append("(ca_nat.tipo_caracteristica LIKE '%Nationality%' AND ca_nat.valor_caracteristica LIKE ?)")
            params.append(f"%{search_params.get('nationality')}%")

        if search_params.get('gov_id'):
            joins.add("LEFT JOIN Identificadores i ON e.uid = i.entidad_uid")
            conditions.append("i.numero_identificador LIKE ?")
            params.append(f"%{search_params.get('gov_id')}%")

        final_query = base_query
        if joins: final_query += " " + " ".join(list(joins))
        if conditions: final_query += " WHERE " + " AND ".join(conditions)
        
        cursor.execute(final_query, params)
        final_uids = [row['uid'] for row in cursor.fetchall()]

        if uids_from_name_search is not None:
            final_uids_ordered = [uid for uid in uids_from_name_search if uid in final_uids]
        else:
            final_uids_ordered = final_uids

        entidades_encontradas = []
        for uid in final_uids_ordered[:50]:
            entidad_completa = get_full_entity_details(cursor, uid)
            if entidad_completa:
                if uid in scores_map:
                    entidad_completa.update(scores_map[uid])
                entidades_encontradas.append(entidad_completa)
        
        return entidades_encontradas

    finally:
        if conn: conn.close()

@app.route('/')
def index():
    """Sirve el archivo frontend principal."""
    return render_template('verificador_final.html')

@app.route('/search')
def search_sanctions():
    """Endpoint que maneja las búsquedas para la UI."""
    search_params = {
        'name': request.args.get('name', '').strip(),
        'dob': request.args.get('dob', '').strip(),
        'nationality': request.args.get('nationality', '').strip(),
        'gov_id': request.args.get('gov_id', '').strip(),
        'threshold': int(request.args.get('threshold', 80)),
        'is_exact_search': request.args.get('exact', 'false').lower() == 'true',
        'exclude_aliases': request.args.get('exclude_aliases', 'false').lower() == 'true'
    }

    if not any([search_params['name'], search_params['dob'], search_params['nationality'], search_params['gov_id']]):
        return jsonify({"error": "Se requiere al menos un parámetro de búsqueda"}), 400

    try:
        resultados = perform_database_search(search_params)
        logging.info(f"Se encontraron {len(resultados)} resultados para la búsqueda UI.")
        return jsonify({"resultados": resultados})
    except Exception as e:
        logging.error(f"Error inesperado durante la búsqueda: {e}", exc_info=True)
        return jsonify({"error": "Error interno al realizar la búsqueda"}), 500

@app.route('/export')
def export_results():
    """Endpoint que maneja la exportación a CSV."""
    search_params = {
        'name': request.args.get('name', '').strip(),
        'dob': request.args.get('dob', '').strip(),
        'nationality': request.args.get('nationality', '').strip(),
        'gov_id': request.args.get('gov_id', '').strip(),
        'threshold': int(request.args.get('threshold', 80)),
        'is_exact_search': request.args.get('exact', 'false').lower() == 'true',
        'exclude_aliases': request.args.get('exclude_aliases', 'false').lower() == 'true'
    }

    if not any([search_params['name'], search_params['dob'], search_params['nationality'], search_params['gov_id']]):
        return jsonify({"error": "Se requiere al menos un criterio de búsqueda para exportar."}), 400

    try:
        entidades_encontradas = perform_database_search(search_params)
        logging.info(f"Exportando {len(entidades_encontradas)} resultados a CSV.")
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        headers = ["UID", "Nombre Principal", "Tipo", "Fuente", "Programas", "Alias", "Direcciones", "IDs", "Info Adicional"]
        writer.writerow(headers)
        
        for entidad in entidades_encontradas:
            # --- CORRECCIÓN AQUÍ ---
            # Se asegura de que todos los elementos sean strings antes de unirlos.
            programas_str = " | ".join(filter(None, entidad.get('programas', [])))
            aliases_str = " | ".join([f"{a.get('nombre_alias') or ''} ({a.get('tipo_alias') or 'Alias'})" for a in entidad.get('aliases', [])])
            direcciones_str = " | ".join([d.get('direccion_completa') or '' for d in entidad.get('direcciones', [])])
            ids_str = " | ".join([f"{i.get('tipo_identificador') or ''}: {i.get('numero_identificador') or ''}" for i in entidad.get('identificadores', [])])
            caracteristicas_str = " | ".join([f"{c.get('tipo_caracteristica') or ''}: {c.get('valor_caracteristica') or ''}" for c in entidad.get('caracteristicas', [])])
            
            row = [
                entidad.get('uid'), entidad.get('nombre_principal'), entidad.get('tipo'), entidad.get('fuente_lista'),
                programas_str, aliases_str, direcciones_str, ids_str, caracteristicas_str
            ]
            writer.writerow(row)
            
        output.seek(0)
        
        return Response(output, mimetype="text/csv", headers={"Content-Disposition":"attachment;filename=resultados_sanciones.csv"})

    except Exception as e:
        logging.error(f"Error inesperado durante la exportación: {e}", exc_info=True)
        return jsonify({"error": "Error interno al generar el archivo CSV."}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)
