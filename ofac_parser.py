# -*- coding: utf-8 -*-
import requests
import xml.etree.ElementTree as ET
import os
import logging
import psycopg2
from psycopg2.extras import execute_values
import sqlite3 # <--- AÑADIDO: Import para SQLite

# Intenta importar dotenv para desarrollo local, pero no falles si no está (para GitHub Actions)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ModuleNotFoundError:
    pass

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_namespace_uri(element):
    if element is not None and '}' in element.tag:
        return element.tag.split('}')[0][1:]
    return None

# --- TODAS LAS FUNCIONES DE PARSING (analizar_ofac_xml_sdn_enhanced, analizar_onu_xml, etc.) PERMANECEN EXACTAMENTE IGUAL ---
# --- FUNCIÓN DE PARSING PARA OFAC SDN_ENHANCED.XML (basada en el XSD "ofacEnhancedXml") ---
def analizar_ofac_xml_sdn_enhanced(ruta_archivo_xml):
    lista_completa_entidades = []
    logging.info(f"Iniciando análisis del archivo XML OFAC SDN Enhanced (schema 'ofacEnhancedXml'): {ruta_archivo_xml}")
    try:
        tree = ET.parse(ruta_archivo_xml)
        root = tree.getroot() 

        expected_ns_uri = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/ENHANCED_XML"
        actual_ns_uri = get_namespace_uri(root)
        ns_uri = actual_ns_uri if actual_ns_uri and actual_ns_uri == expected_ns_uri else expected_ns_uri
        
        if ns_uri:
            ET.register_namespace('', ns_uri) 
            logging.info(f"OFAC SDN Enhanced: Usando namespace URI '{ns_uri}'.")
        else:
            logging.warning("OFAC SDN Enhanced: No se detectó un namespace URI esperado. Se procederá sin él, lo que podría causar errores si el XML lo requiere.")

        def build_tag(base_name):
            return f"{{{ns_uri}}}{base_name}" if ns_uri else base_name

        def find_node_text(parent, tag_path_list): 
            current_node = parent
            for i, part_name in enumerate(tag_path_list):
                node_to_find = build_tag(part_name)
                found_node = current_node.find(node_to_find)
                if found_node is None: return None
                if i == len(tag_path_list) - 1:
                    return found_node.text.strip() if found_node.text else None
                current_node = found_node
            return None 

        def find_node(parent, tag_path_list):
            current_node = parent
            for part_name in tag_path_list:
                node_to_find = build_tag(part_name)
                current_node = current_node.find(node_to_find)
                if current_node is None: return None
            return current_node

        def findall_nodes(parent, tag_path_list):
            current_node = parent
            for i, part_name in enumerate(tag_path_list):
                node_to_find = build_tag(part_name)
                if i == len(tag_path_list) - 1:
                    return current_node.findall(node_to_find)
                current_node = current_node.find(node_to_find)
                if current_node is None: return []
            return []
        
        reference_values_map = {}
        reference_values_node = find_node(root, ["referenceValues"])
        if reference_values_node is not None:
            for ref_val_node in findall_nodes(reference_values_node, ["referenceValue"]):
                ref_id = ref_val_node.get("refId")
                value = find_node_text(ref_val_node, ["value"]) 
                if ref_id and value:
                    reference_values_map[ref_id] = value
        logging.info(f"OFAC SDN Enhanced: {len(reference_values_map)} valores de referencia cacheados.")

        sanctions_entries_container_base_tag = "entities" 
        sanction_entry_base_tag = "entity"      

        entities_container_node = find_node(root, [sanctions_entries_container_base_tag])
        if entities_container_node is None:
            logging.error(f"OFAC SDN Enhanced: No se encontró el contenedor <{sanctions_entries_container_base_tag}>. Verifica la estructura del XML.")
            return []

        entity_elements = findall_nodes(entities_container_node, [sanction_entry_base_tag])
        if not entity_elements:
            logging.error(f"OFAC SDN Enhanced: No se encontraron elementos <{sanction_entry_base_tag}> para procesar.")
            return []
            
        logging.info(f"OFAC SDN Enhanced: Procesando {len(entity_elements)} nodos <{sanction_entry_base_tag}> encontrados.")

        for entry_node in entity_elements: 
            entidad = {'fuente_lista': 'OFAC'}
            aliases, direcciones, identificadores, caracteristicas, programas = [], [], [], [], []
            
            general_info_node = find_node(entry_node, ["generalInfo"])
            if general_info_node:
                entidad['uid'] = find_node_text(general_info_node, ["identityId"])
                entity_type_ref_node = find_node(general_info_node, ["entityType"])
                if entity_type_ref_node is not None:
                    entity_type_ref_id = entity_type_ref_node.get("refId")
                    entidad['tipo'] = reference_values_map.get(entity_type_ref_id, entity_type_ref_node.text)
                
                remarks_text = find_node_text(general_info_node, ["remarks"])
                if remarks_text: caracteristicas.append({'tipo_caracteristica': 'Remarks', 'valor_caracteristica': remarks_text})
                title_text = find_node_text(general_info_node, ["title"])
                if title_text: caracteristicas.append({'tipo_caracteristica': 'Title', 'valor_caracteristica': title_text})

            sanctions_programs_node = find_node(entry_node, ["sanctionsPrograms"])
            if sanctions_programs_node:
                for prog_node in findall_nodes(sanctions_programs_node, ["sanctionsProgram"]):
                    program_ref_id = prog_node.get("refId")
                    program_name = reference_values_map.get(program_ref_id, prog_node.text)
                    if program_name: programas.append(program_name)
            entidad['programas'] = list(set(programas))

            names_node = find_node(entry_node, ["names"])
            if names_node:
                for name_node in findall_nodes(names_node, ["name"]):
                    is_primary_name = find_node_text(name_node, ["isPrimary"]) == "true"
                    alias_type_ref_node = find_node(name_node, ["aliasType"])
                    alias_type_str = "AKA" 
                    if alias_type_ref_node is not None:
                         alias_type_ref_id = alias_type_ref_node.get("refId")
                         alias_type_str = reference_values_map.get(alias_type_ref_id, alias_type_ref_node.text or "AKA")

                    translations_node = find_node(name_node, ["translations"])
                    if translations_node:
                        for trans_node in findall_nodes(translations_node, ["translation"]):
                            is_primary_translation = find_node_text(trans_node, ["isPrimary"]) == "true"
                            full_name = find_node_text(trans_node, ["formattedFullName"])
                            script_ref_node = find_node(trans_node, ["script"])
                            script_str = None
                            if script_ref_node is not None:
                                script_ref_id = script_ref_node.get("refId")
                                script_str = reference_values_map.get(script_ref_id, script_ref_node.text)
                            
                            if full_name:
                                if is_primary_name and is_primary_translation and not entidad.get('nombre_principal'):
                                    entidad['nombre_principal'] = full_name
                                elif not is_primary_name or (is_primary_name and not is_primary_translation and entidad.get('nombre_principal') != full_name):
                                    aliases.append({'nombre_alias': full_name, 'tipo_alias': alias_type_str, 'idioma_escritura': script_str if script_str and script_str.lower() != 'latin' else None})
                if not entidad.get('nombre_principal') and aliases:
                    entidad['nombre_principal'] = aliases[0]['nombre_alias']
            entidad['aliases'] = aliases
            
            addresses_node = find_node(entry_node, ["addresses"])
            if addresses_node:
                for addr_node in findall_nodes(addresses_node, ["address"]):
                    addr_calle1, addr_ciudad, addr_pais, addr_cp, addr_region = None, None, None, None, None
                    addr_parts_collected_for_full = []
                    
                    country_ref_node_addr = find_node(addr_node, ["country"])
                    if country_ref_node_addr is not None:
                        addr_pais = reference_values_map.get(country_ref_node_addr.get("refId"), country_ref_node_addr.text)

                    translations_addr_node = find_node(addr_node, ["translations"])
                    if translations_addr_node:
                        trans_addr_node = find_node(translations_addr_node, ["translation"]) 
                        if trans_addr_node:
                            address_parts_container_node = find_node(trans_addr_node, ["addressParts"]) 
                            if address_parts_container_node:
                                for part_node in findall_nodes(address_parts_container_node, ["addressPart"]): 
                                    part_type_ref_node = find_node(part_node, ["type"])
                                    part_value = find_node_text(part_node, ["value"])
                                    if part_type_ref_node and part_value:
                                        part_type_ref_id = part_type_ref_node.get("refId")
                                        part_type_str = reference_values_map.get(part_type_ref_id, part_type_ref_node.text)
                                        addr_parts_collected_for_full.append(part_value) 
                                        if part_type_str:
                                            pt_lower = part_type_str.lower()
                                            if "address1" in pt_lower: addr_calle1 = part_value
                                            elif "city" in pt_lower: addr_ciudad = part_value
                                            elif "postal code" in pt_lower: addr_cp = part_value
                                            elif "state/province" in pt_lower: addr_region = part_value
                                            elif "country" in pt_lower and not addr_pais : addr_pais = part_value 
                    
                    direccion_completa = ", ".join(filter(None, addr_parts_collected_for_full)) if addr_parts_collected_for_full else None
                    if not addr_calle1 and addr_parts_collected_for_full: addr_calle1 = addr_parts_collected_for_full[0] # Fallback
                    if direccion_completa or addr_pais or addr_ciudad:
                         direcciones.append({
                            'calle1': addr_calle1, 'ciudad': addr_ciudad, 'pais': addr_pais, 
                            'codigo_postal': addr_cp, 'direccion_completa': direccion_completa, 
                            'region': addr_region, 'lugar': None, 'po_box': None
                        })
            entidad['direcciones'] = direcciones

            identity_docs_node = find_node(entry_node, ["identityDocuments"])
            if identity_docs_node:
                for id_doc_node in findall_nodes(identity_docs_node, ["identityDocument"]):
                    doc_type_ref_node = find_node(id_doc_node, ["type"])
                    doc_type_str = "Desconocido"
                    if doc_type_ref_node is not None:
                        doc_type_ref_id = doc_type_ref_node.get("refId")
                        doc_type_str = reference_values_map.get(doc_type_ref_id, doc_type_ref_node.text)
                    
                    doc_number = find_node_text(id_doc_node, ["documentNumber"])
                    issuing_country_ref_node = find_node(id_doc_node, ["issuingCountry"])
                    pais_emisor_doc = None
                    if issuing_country_ref_node is not None:
                        issuing_country_ref_id = issuing_country_ref_node.get("refId")
                        pais_emisor_doc = reference_values_map.get(issuing_country_ref_id, issuing_country_ref_node.text)
                    
                    comentarios_doc = find_node_text(id_doc_node, ["comments"])
                    if doc_number:
                        identificadores.append({'tipo_identificador': doc_type_str, 'numero_identificador': doc_number, 'pais_emisor': pais_emisor_doc, 'comentarios': comentarios_doc})
            entidad['identificadores'] = identificadores

            features_node = find_node(entry_node, ["features"])
            if features_node:
                for feature_node in findall_nodes(features_node, ["feature"]):
                    feature_type_node = find_node(feature_node, ["type"]) 
                    tipo_carac_str = "Desconocido"
                    valor_carac_str = find_node_text(feature_node, ["value"]) 

                    if feature_type_node is not None:
                        feature_type_id_attr = feature_type_node.get("featureTypeId")
                        tipo_carac_str = reference_values_map.get(feature_type_id_attr, feature_type_node.text)
                        
                        if not valor_carac_str: 
                            value_date_node = find_node(feature_node, ["valueDate"])
                            if value_date_node:
                                from_date_begin = find_node_text(value_date_node, ["fromDateBegin"])
                                if from_date_begin: valor_carac_str = from_date_begin
                    
                    if tipo_carac_str and valor_carac_str:
                        caracteristicas.append({'tipo_caracteristica': tipo_carac_str, 'valor_caracteristica': valor_carac_str})
            entidad['caracteristicas'] = list({frozenset(item.items()): item for item in caracteristicas}.values())
            
            if entidad.get('uid') or entidad.get('nombre_principal'):
                lista_completa_entidades.append(entidad)
            else:
                entity_id_attr = entry_node.get("id")
                logging.warning(f"OFAC SDN Enhanced: Entidad (XML ID: {entity_id_attr}) sin UID de <identityId> ni nombre_principal. Saltada.")
        
        logging.info(f"OFAC SDN Enhanced: Análisis XML completado. Se extrajeron {len(lista_completa_entidades)} entidades.")

    except ET.ParseError as e:
        logging.error(f"Error de parsing XML en archivo OFAC SDN Enhanced {ruta_archivo_xml}: {e}")
    except Exception as e:
        logging.error(f"Error inesperado al analizar OFAC SDN Enhanced {ruta_archivo_xml}: {e}", exc_info=True)
    return lista_completa_entidades

def analizar_onu_xml(ruta_archivo_xml):
    # ... (el código de esta función no cambia)
    lista_completa_entidades = []
    logging.info(f"Iniciando análisis del archivo XML de ONU: {ruta_archivo_xml}")
    try:
        tree = ET.parse(ruta_archivo_xml)
        root = tree.getroot() 
        individuals_container = root.find("INDIVIDUALS")
        if individuals_container is not None:
            for ind_node in individuals_container.findall("INDIVIDUAL"):
                entidad = {'fuente_lista': 'ONU', 'tipo': 'Individual'}; aliases, direcciones, identificadores, caracteristicas, programas = [], [], [], [], []
                data_id_node_text = ind_node.findtext("DATAID"); ref_num_node_uid_text = ind_node.findtext("REFERENCE_NUMBER") 
                entidad['uid'] = f"UN-{data_id_node_text}" if data_id_node_text else (f"UN-REF-{ref_num_node_uid_text}" if ref_num_node_uid_text else None)
                first_name = ind_node.findtext("FIRST_NAME", default="").strip(); second_name = ind_node.findtext("SECOND_NAME", default="").strip(); third_name = ind_node.findtext("THIRD_NAME", default="").strip()
                nombre_completo_parts = [name for name in [first_name, second_name, third_name] if name]; entidad['nombre_principal'] = " ".join(nombre_completo_parts) if nombre_completo_parts else None
                for alias_node in ind_node.findall("INDIVIDUAL_ALIAS"):
                    alias_name = alias_node.findtext("ALIAS_NAME", default="").strip(); quality = alias_node.findtext("QUALITY", default="").strip()
                    if alias_name: aliases.append({'nombre_alias': alias_name, 'tipo_alias': quality if quality else 'Alias', 'idioma_escritura': None})
                nombre_original_script = ind_node.findtext("NAME_ORIGINAL_SCRIPT", default="").strip()
                if nombre_original_script: aliases.append({'nombre_alias': nombre_original_script, 'tipo_alias': 'Nombre en Escritura Original', 'idioma_escritura': 'Original Script'})
                entidad['aliases'] = aliases
                for addr_node in ind_node.findall("INDIVIDUAL_ADDRESS"):
                    country = addr_node.findtext("COUNTRY", default="").strip(); city = addr_node.findtext("CITY", default="").strip(); street = addr_node.findtext("STREET", default="").strip(); note = addr_node.findtext("NOTE", default="").strip()
                    dir_parts = [street, city, country, note]; dir_completa = ", ".join(filter(None, dir_parts))
                    if dir_completa: direcciones.append({'calle1': street, 'ciudad': city, 'pais': country, 'codigo_postal': None, 'direccion_completa': dir_completa, 'region': None, 'lugar': None, 'po_box': None})
                entidad['direcciones'] = direcciones
                title_val = ind_node.findtext("TITLE/VALUE", default="").strip();_ = caracteristicas.append({'tipo_caracteristica': 'Title', 'valor_caracteristica': title_val}) if title_val else None
                desig_node = ind_node.find("DESIGNATION");_ = [caracteristicas.append({'tipo_caracteristica': 'Designation', 'valor_caracteristica': val_node.text.strip()}) for val_node in desig_node.findall("VALUE") if val_node.text] if desig_node is not None else None
                nat_val = ind_node.findtext("NATIONALITY/VALUE", default="").strip();_ = caracteristicas.append({'tipo_caracteristica': 'Nationality', 'valor_caracteristica': nat_val}) if nat_val else None
                dob_node = ind_node.find("INDIVIDUAL_DATE_OF_BIRTH")
                if dob_node is not None and dob_node.findtext("YEAR"): 
                    dob_type = dob_node.findtext("TYPE_OF_DATE", default="").strip()
                    dob_day = dob_node.findtext("DAY", default="").strip(); dob_month = dob_node.findtext("MONTH", default="").strip(); dob_year = dob_node.findtext("YEAR", default="").strip()
                    dob_val_parts = [dob_year, dob_month, dob_day] 
                    dob_val = "-".join(filter(None, dob_val_parts))
                    if dob_val: caracteristicas.append({'tipo_caracteristica': f'Date of Birth ({dob_type})'.strip(), 'valor_caracteristica': dob_val})
                for pob_node in ind_node.findall("INDIVIDUAL_PLACE_OF_BIRTH"): pob_city = pob_node.findtext("CITY", default="").strip(); pob_prov = pob_node.findtext("STATE_PROVINCE", default="").strip(); pob_country = pob_node.findtext("COUNTRY", default="").strip(); pob_parts = [pob_city, pob_prov, pob_country]; pob_val = ", ".join(filter(None, pob_parts));_ = caracteristicas.append({'tipo_caracteristica': 'Place of Birth', 'valor_caracteristica': pob_val}) if pob_val else None
                comments = ind_node.findtext("COMMENTS1", default="").strip();_ = caracteristicas.append({'tipo_caracteristica': 'Comments', 'valor_caracteristica': comments}) if comments else None
                listed_on = ind_node.findtext("LISTED_ON", default="").strip();_ = caracteristicas.append({'tipo_caracteristica': 'Listed On', 'valor_caracteristica': listed_on}) if listed_on else None
                entidad['caracteristicas'] = caracteristicas
                un_list_type = ind_node.findtext("UN_LIST_TYPE", default="").strip();_ = programas.append(un_list_type) if un_list_type else None
                if ref_num_node_uid_text: programas.append(f"UN Ref: {ref_num_node_uid_text.strip()}")
                entidad['programas'] = list(set(programas)); entidad['identificadores'] = identificadores
                if entidad.get('uid') or entidad.get('nombre_principal'): lista_completa_entidades.append(entidad)
        entities_container = root.find("ENTITIES")
        if entities_container is not None:
            for ent_node in entities_container.findall("ENTITY"):
                entidad_obj = {'fuente_lista': 'ONU', 'tipo': 'Entity'}; aliases_ent, direcciones_ent, identificadores_ent, caracteristicas_ent, programas_ent = [], [], [], [], []
                data_id_ent_text = ent_node.findtext("DATAID"); ref_num_ent_uid_text = ent_node.findtext("REFERENCE_NUMBER")
                entidad_obj['uid'] = f"UN-{data_id_ent_text}" if data_id_ent_text else (f"UN-REF-{ref_num_ent_uid_text}" if ref_num_ent_uid_text else None)
                entidad_obj['nombre_principal'] = ent_node.findtext("FIRST_NAME", default="").strip()
                for alias_node_ent in ent_node.findall("ENTITY_ALIAS"): alias_name_ent = alias_node_ent.findtext("ALIAS_NAME", default="").strip(); quality_ent = alias_node_ent.findtext("QUALITY", default="").strip();_ = aliases_ent.append({'nombre_alias': alias_name_ent, 'tipo_alias': quality_ent if quality_ent else 'Alias', 'idioma_escritura': None}) if alias_name_ent else None
                entidad_obj['aliases'] = aliases_ent
                for addr_node_ent in ent_node.findall("ENTITY_ADDRESS"): country_ent = addr_node_ent.findtext("COUNTRY", default="").strip(); city_ent = addr_node_ent.findtext("CITY", default="").strip(); street_ent = addr_node_ent.findtext("STREET", default="").strip(); note_ent = addr_node_ent.findtext("NOTE", default="").strip(); dir_parts_ent = [street_ent, city_ent, country_ent, note_ent]; dir_completa_ent = ", ".join(filter(None, dir_parts_ent));_ = direcciones_ent.append({'calle1': street_ent, 'ciudad': city_ent, 'pais': country_ent, 'codigo_postal': None, 'direccion_completa': dir_completa_ent, 'region': None, 'lugar': None, 'po_box': None}) if dir_completa_ent else None
                entidad_obj['direcciones'] = direcciones_ent
                comments_ent = ent_node.findtext("COMMENTS1", default="").strip();_ = caracteristicas_ent.append({'tipo_caracteristica': 'Comments', 'valor_caracteristica': comments_ent}) if comments_ent else None
                listed_on_ent = ent_node.findtext("LISTED_ON", default="").strip();_ = caracteristicas_ent.append({'tipo_caracteristica': 'Listed On', 'valor_caracteristica': listed_on_ent}) if listed_on_ent else None
                entidad_obj['caracteristicas'] = caracteristicas_ent
                un_list_type_ent = ent_node.findtext("UN_LIST_TYPE", default="").strip();_ = programas_ent.append(un_list_type_ent) if un_list_type_ent else None
                if ref_num_ent_uid_text: programas_ent.append(f"UN Ref: {ref_num_ent_uid_text.strip()}")
                entidad_obj['programas'] = list(set(programas_ent)); entidad_obj['identificadores'] = identificadores_ent
                if entidad_obj.get('uid') or entidad_obj.get('nombre_principal'): lista_completa_entidades.append(entidad_obj)
        logging.info(f"ONU: Análisis XML completado. Se extrajeron {len(lista_completa_entidades)} entidades.")
    except ET.ParseError as e: logging.error(f"Error de parsing XML en archivo ONU {ruta_archivo_xml}: {e}")
    except Exception as e: logging.error(f"Error inesperado al analizar ONU {ruta_archivo_xml}: {e}", exc_info=True)
    return lista_completa_entidades

def analizar_ue_xml(ruta_archivo_xml):
    # ... (el código de esta función no cambia)
    lista_completa_entidades = []
    logging.info(f"Iniciando análisis del archivo XML de UE: {ruta_archivo_xml}")
    try:
        tree = ET.parse(ruta_archivo_xml)
        root = tree.getroot() 
        ns_uri = get_namespace_uri(root)
        ns = {'eu': ns_uri} if ns_uri else {}
        def get_tag(base_tag): return f"{{{ns_uri}}}{base_tag}" if ns_uri else base_tag
        entity_tag = get_tag("sanctionEntity")
        for se_node in root.findall(entity_tag): 
            entidad = {'fuente_lista': 'UE'}; aliases, direcciones, identificadores, caracteristicas, programas = [], [], [], [], []
            logical_id = se_node.get("logicalId"); eu_ref = se_node.get("euReferenceNumber"); un_id = se_node.get("unitedNationId")
            entidad['uid'] = f"EU-{logical_id}" if logical_id else (f"EU-REF-{eu_ref}" if eu_ref else (f"EU-UNID-{un_id}" if un_id else f"EU-TEMP-{len(lista_completa_entidades)+1}"))
            if eu_ref: identificadores.append({'tipo_identificador': 'EU Reference Number', 'numero_identificador': eu_ref, 'pais_emisor': 'EU'});
            if un_id: identificadores.append({'tipo_identificador': 'UN ID (from EU list)', 'numero_identificador': un_id, 'pais_emisor': 'UN'})
            subject_type_node = se_node.find(get_tag("subjectType"))
            if subject_type_node is not None: code = subject_type_node.get("code"); entidad['tipo'] = "Individual" if code == "person" else ("Entity" if code in ["enterprise", "organisation", "legalEntity"] else (code.capitalize() if code else "Desconocido"))
            else: entidad['tipo'] = "Desconocido"
            regulation_node = se_node.find(get_tag("regulation"))
            if regulation_node is not None: programme = regulation_node.get("programme", "").strip();_ = programas.append(programme) if programme else None
            entidad['programas'] = list(set(programas))
            remark_node = se_node.find(get_tag("remark"))
            if remark_node is not None and remark_node.text: caracteristicas.append({'tipo_caracteristica': 'Remark', 'valor_caracteristica': remark_node.text.strip()})
            for cit_node in se_node.findall(get_tag("citizenship")): country_desc = cit_node.get("countryDescription", "").strip();_ = caracteristicas.append({'tipo_caracteristica': 'Nationality/Citizenship', 'valor_caracteristica': country_desc}) if country_desc else None
            for bd_node in se_node.findall(get_tag("birthdate")):
                birth_date_attr = bd_node.get("birthdate", "").strip(); year = bd_node.get("year","").strip(); month = bd_node.get("monthOfYear","").strip(); day = bd_node.get("dayOfMonth","").strip(); city = bd_node.get("city", "").strip(); country_desc_bd = bd_node.get("countryDescription", "").strip(); place_attr = bd_node.get("place", "").strip()
                dob_str = birth_date_attr; 
                if not dob_str and year: dob_str = year; dob_str = f"{year}-{month.zfill(2)}" if month else dob_str; dob_str = f"{year}-{month.zfill(2)}-{day.zfill(2)}" if day and month else dob_str
                if dob_str: caracteristicas.append({'tipo_caracteristica': 'Date of Birth', 'valor_caracteristica': dob_str})
                pob_parts = [part for part in [city, place_attr, country_desc_bd] if part];_ = caracteristicas.append({'tipo_caracteristica': 'Place of Birth', 'valor_caracteristica': ", ".join(pob_parts)}) if pob_parts else None
            for addr_node in se_node.findall(get_tag("address")):
                street = addr_node.get("street", "").strip(); city_addr = addr_node.get("city", "").strip(); zip_code = addr_node.get("zipCode", "").strip(); country_desc_addr = addr_node.get("countryDescription", "").strip(); region = addr_node.get("region", "").strip(); place = addr_node.get("place", "").strip(); po_box = addr_node.get("poBox", "").strip()
                dir_parts = [part for part in [street, po_box, city_addr, zip_code, place, region, country_desc_addr] if part]; direccion_completa = ", ".join(dir_parts)
                if direccion_completa: direcciones.append({'calle1': street or None, 'ciudad': city_addr or None, 'pais': country_desc_addr or None, 'codigo_postal': zip_code or None, 'direccion_completa': direccion_completa, 'region': region or None, 'lugar': place or None, 'po_box': po_box or None})
            entidad['direcciones'] = direcciones
            for id_doc_node in se_node.findall(get_tag("identification")):
                id_type_code = id_doc_node.get("identificationTypeCode", "").strip(); id_type_desc = id_doc_node.get("identificationTypeDescription", "").strip(); id_number = id_doc_node.get("number", "").strip(); id_country_desc_id = id_doc_node.get("countryDescription", "").strip(); id_issued_by = id_doc_node.get("issuedBy", "").strip(); id_name_on_doc = id_doc_node.get("nameOnDocument", "").strip(); id_latin_number = id_doc_node.get("latinNumber", "").strip(); id_remark_node_child = id_doc_node.find(get_tag("remark")); id_remark = id_remark_node_child.text.strip() if id_remark_node_child is not None and id_remark_node_child.text else ""
                tipo_id = id_type_desc if id_type_desc else id_type_code; num_id = id_number if id_number else id_latin_number; comentarios_id_parts = []
                if id_issued_by: comentarios_id_parts.append(f"Emitido por: {id_issued_by}");
                if id_name_on_doc: comentarios_id_parts.append(f"Nombre en documento: {id_name_on_doc}");
                if id_remark: comentarios_id_parts.append(id_remark);
                comentarios_id_final = "; ".join(comentarios_id_parts)
                if num_id: identificadores.append({'tipo_identificador': tipo_id, 'numero_identificador': num_id, 'pais_emisor': id_country_desc_id or None, 'comentarios': comentarios_id_final or None})
            entidad['identificadores'] = identificadores
            nombre_principal_val = None; name_alias_nodes = se_node.findall(get_tag("nameAlias"))
            for na_node in name_alias_nodes: 
                is_strong = na_node.get("strong", "false").lower() == 'true'; whole_name = na_node.get("wholeName", "").strip(); first_name_na = na_node.get("firstName", "").strip(); last_name_na = na_node.get("lastName", "").strip()
                current_name_from_parts = " ".join(filter(None, [first_name_na, last_name_na])); current_name = whole_name if whole_name else current_name_from_parts
                if is_strong and current_name: nombre_principal_val = current_name; break
            if not nombre_principal_val and name_alias_nodes:
                na_node = name_alias_nodes[0]; whole_name = na_node.get("wholeName", "").strip(); first_name_na = na_node.get("firstName", "").strip(); last_name_na = na_node.get("lastName", "").strip()
                current_name_from_parts = " ".join(filter(None, [first_name_na, last_name_na])); nombre_principal_val = whole_name if whole_name else current_name_from_parts
            for na_node in name_alias_nodes:
                whole_name = na_node.get("wholeName", "").strip(); first_name_na = na_node.get("firstName", "").strip(); last_name_na = na_node.get("lastName", "").strip(); name_language = na_node.get("nameLanguage", "").strip()
                current_name_from_parts = " ".join(filter(None, [first_name_na, last_name_na])); current_name = whole_name if whole_name else current_name_from_parts
                if not current_name: continue
                if current_name.lower() != (nombre_principal_val or "").lower(): aliases.append({'nombre_alias': current_name, 'tipo_alias': 'AKA', 'idioma_escritura': name_language if name_language and name_language.upper() != 'EN' else None})
                function_val = na_node.get("function", "").strip(); title_val_na = na_node.get("title", "").strip(); gender_val = na_node.get("gender", "").strip()
                if function_val: caracteristicas.append({'tipo_caracteristica': 'Function/Role', 'valor_caracteristica': function_val})
                if title_val_na: caracteristicas.append({'tipo_caracteristica': 'Title', 'valor_caracteristica': title_val_na})
                if gender_val: caracteristicas.append({'tipo_caracteristica': 'Gender', 'valor_caracteristica': gender_val})
            entidad['nombre_principal'] = nombre_principal_val
            entidad['aliases'] = list({frozenset(item.items()): item for item in aliases}.values())
            entidad['caracteristicas'] = list({frozenset(item.items()): item for item in caracteristicas}.values())
            if entidad.get('uid') or entidad.get('nombre_principal'): lista_completa_entidades.append(entidad)
        logging.info(f"UE: Análisis XML completado. Se extrajeron {len(lista_completa_entidades)} entidades.")
    except ET.ParseError as e: logging.error(f"Error de parsing XML en archivo UE {ruta_archivo_xml}: {e}")
    except Exception as e: logging.error(f"Error inesperado al analizar UE {ruta_archivo_xml}: {e}", exc_info=True)
    return lista_completa_entidades

def analizar_uk_xml(ruta_archivo_xml):
    # ... (el código de esta función no cambia)
    lista_completa_entidades = []
    logging.info(f"Iniciando análisis del archivo XML de UK (OFSI): {ruta_archivo_xml}")
    try:
        tree = ET.parse(ruta_archivo_xml)
        root = tree.getroot() 
        ns_uri_uk = get_namespace_uri(root)
        ns_uk_find = {'uk': ns_uri_uk} if ns_uri_uk else {} 
        def get_uk_tag(base_tag): return f"{{{ns_uri_uk}}}{base_tag}" if ns_uri_uk else base_tag
        target_tag = get_uk_tag("FinancialSanctionsTarget")
        targets_por_grupo = {}
        for fst_node in root.findall(target_tag):
            group_id_node = fst_node.find(get_uk_tag("GroupID"))
            if group_id_node is not None and group_id_node.text:
                group_id = group_id_node.text; targets_por_grupo.setdefault(group_id, []).append(fst_node)
        logging.info(f"UK: {len(targets_por_grupo)} grupos de entidades (GroupID) encontrados.")
        for group_id, fst_nodes_grupo in targets_por_grupo.items():
            entidad = {'fuente_lista': 'UK', 'uid': f"UK-{group_id}"}; aliases, direcciones, identificadores, caracteristicas, programas = [], [], [], [], []
            nombre_principal_val, tipo_entidad_val = None, None; nombres_candidatos_del_grupo = []
            for idx, fst_node in enumerate(fst_nodes_grupo):
                if tipo_entidad_val is None: group_type_node = fst_node.find(get_uk_tag("GroupTypeDescription")); tipo_entidad_val = group_type_node.text.strip() if group_type_node is not None and group_type_node.text else "Desconocido"
                nombre_parts = [fst_node.findtext(get_uk_tag(f"Name{i}"), default="").strip() for i in range(1, 7)]; nombre_concatenado = " ".join(filter(None, nombre_parts)).strip()
                title_node = fst_node.find(get_uk_tag("Title")); title = title_node.text.strip() if title_node is not None and title_node.text else ""
                if title and nombre_concatenado: nombre_concatenado = f"{title} {nombre_concatenado}".strip()
                elif title and not nombre_concatenado: nombre_concatenado = title
                alias_type_node = fst_node.find(get_uk_tag("AliasType")); tipo_alias_actual = alias_type_node.text.strip() if alias_type_node is not None and alias_type_node.text else "Alias"
                if nombre_concatenado: nombres_candidatos_del_grupo.append({'nombre': nombre_concatenado, 'tipo': tipo_alias_actual, 'idioma_escritura': None})
                name_non_latin_node = fst_node.find(get_uk_tag("NameNonLatinScript"))
                if name_non_latin_node is not None and name_non_latin_node.text:
                    non_latin_name = name_non_latin_node.text.strip(); script_type_node = fst_node.find(get_uk_tag("NonLatinScriptType")); script_lang_node = fst_node.find(get_uk_tag("NonLatinScriptLanguage")); script_info_parts = []
                    if script_type_node is not None and script_type_node.text: script_info_parts.append(script_type_node.text.strip())
                    if script_lang_node is not None and script_lang_node.text: script_info_parts.append(script_lang_node.text.strip())
                    script_info = ", ".join(script_info_parts) if script_info_parts else "Escritura No Latina"
                    nombres_candidatos_del_grupo.append({'nombre': non_latin_name, 'tipo': 'Nombre en Escritura Original', 'idioma_escritura': script_info})
                regime_name_node = fst_node.find(get_uk_tag("RegimeName"))
                if regime_name_node is not None and regime_name_node.text: programas.append(regime_name_node.text.strip())
                if idx == 0: 
                    uk_ref_node = fst_node.find(get_uk_tag("UKSanctionsListRef"));_ = identificadores.append({'tipo_identificador': 'UKSanctionsListRef', 'numero_identificador': uk_ref_node.text.strip(), 'pais_emisor': 'UK'}) if uk_ref_node is not None and uk_ref_node.text else None
                    un_ref_node = fst_node.find(get_uk_tag("UNRef"));_ = identificadores.append({'tipo_identificador': 'UNRef (from UK list)', 'numero_identificador': un_ref_node.text.strip(), 'pais_emisor': 'UN'}) if un_ref_node is not None and un_ref_node.text else None
                    addr_parts_uk = [fst_node.findtext(get_uk_tag(f"Address{i}"), default="").strip() for i in range(1, 7)]; post_code_uk = fst_node.findtext(get_uk_tag("PostCode"), default="").strip(); country_uk_val = fst_node.findtext(get_uk_tag("Country"), default="").strip()
                    if post_code_uk: addr_parts_uk.append(post_code_uk);
                    if country_uk_val: addr_parts_uk.append(country_uk_val)
                    dir_completa_uk = ", ".join(filter(None, addr_parts_uk))
                    if dir_completa_uk: direcciones.append({'calle1': fst_node.findtext(get_uk_tag("Address1"), default=""), 'ciudad': None, 'pais': country_uk_val or None, 'codigo_postal': post_code_uk or None, 'direccion_completa': dir_completa_uk, 'region': None, 'lugar': None, 'po_box': None})
                    reasons_node = fst_node.find(get_uk_tag("UKStatementOfReasons"));_ = caracteristicas.append({'tipo_caracteristica': 'UK Statement Of Reasons', 'valor_caracteristica': reasons_node.text.strip()}) if reasons_node is not None and reasons_node.text else None
                    other_info_node = fst_node.find(get_uk_tag("OtherInformation"));_ = caracteristicas.append({'tipo_caracteristica': 'Other Information', 'valor_caracteristica': other_info_node.text.strip()}) if other_info_node is not None and other_info_node.text else None
                    dob_container_node = fst_node.find(get_uk_tag("Individual_DateOfBirth")) 
                    if dob_container_node is not None: [caracteristicas.append({'tipo_caracteristica': 'Date of Birth', 'valor_caracteristica': date_node.text.strip()}) for date_node in dob_container_node.findall(get_uk_tag("Date")) if date_node.text]
                    pob_town_node = fst_node.find(get_uk_tag("Individual_TownOfBirth")); pob_country_node = fst_node.find(get_uk_tag("Individual_CountryOfBirth")); pob_uk_parts = []
                    if pob_town_node is not None and pob_town_node.text: pob_uk_parts.append(pob_town_node.text.strip())
                    if pob_country_node is not None and pob_country_node.text: pob_uk_parts.append(pob_country_node.text.strip())
                    if pob_uk_parts: caracteristicas.append({'tipo_caracteristica': 'Place of Birth', 'valor_caracteristica': ", ".join(pob_uk_parts)})
                    nat_container_node = fst_node.find(get_uk_tag("Individual_Nationality"))
                    if nat_container_node is not None: [caracteristicas.append({'tipo_caracteristica': 'Nationality', 'valor_caracteristica': nat_val_node.text.strip()}) for nat_val_node in nat_container_node.findall(get_uk_tag("Nationality")) if nat_val_node.text]
                    pos_node = fst_node.find(get_uk_tag("Individual_Position"));_ = caracteristicas.append({'tipo_caracteristica': 'Position', 'valor_caracteristica': pos_node.text.strip()}) if pos_node is not None and pos_node.text else None
                    gender_node = fst_node.find(get_uk_tag("Individual_Gender"));_ = caracteristicas.append({'tipo_caracteristica': 'Gender', 'valor_caracteristica': gender_node.text.strip()}) if gender_node is not None and gender_node.text else None
                    entity_type_node_uk = fst_node.find(get_uk_tag("Entity_Type"));_ = caracteristicas.append({'tipo_caracteristica': 'Entity Specific Type (UK)', 'valor_caracteristica': entity_type_node_uk.text.strip()}) if entity_type_node_uk is not None and entity_type_node_uk.text else None
                    date_listed_node = fst_node.find(get_uk_tag("DateListed"));_ = caracteristicas.append({'tipo_caracteristica': 'Date Listed', 'valor_caracteristica': date_listed_node.text.split('T')[0]}) if date_listed_node is not None and date_listed_node.text else None
                    last_updated_node = fst_node.find(get_uk_tag("LastUpdated"));_ = caracteristicas.append({'tipo_caracteristica': 'Last Updated', 'valor_caracteristica': last_updated_node.text.split('T')[0]}) if last_updated_node is not None and last_updated_node.text else None
                    passport_node = fst_node.find(get_uk_tag("Individual_PassportNumber"));_ = identificadores.append({'tipo_identificador': 'Passport Number', 'numero_identificador': passport_node.text.strip(), 'pais_emisor': None}) if passport_node is not None and passport_node.text else None
                    ni_node = fst_node.find(get_uk_tag("Individual_NINumber"));_ = identificadores.append({'tipo_identificador': 'National Insurance Number', 'numero_identificador': ni_node.text.strip(), 'pais_emisor': 'UK'}) if ni_node is not None and ni_node.text else None
                    biz_reg_node = fst_node.find(get_uk_tag("Entity_BusinessRegNumber"));_ = identificadores.append({'tipo_identificador': 'Business Registration Number', 'numero_identificador': biz_reg_node.text.strip(), 'pais_emisor': None}) if biz_reg_node is not None and biz_reg_node.text else None
            if nombres_candidatos_del_grupo:
                primary_name_entry = next((n for n in nombres_candidatos_del_grupo if "primary name" in n['tipo'].lower()), None)
                if primary_name_entry: nombre_principal_val = primary_name_entry['nombre']; aliases.extend([nc for nc in nombres_candidatos_del_grupo if nc['nombre'].lower() != nombre_principal_val.lower()])
                else: nombre_principal_val = nombres_candidatos_del_grupo[0]['nombre']; aliases.extend(nombres_candidatos_del_grupo[1:])
            entidad['nombre_principal'] = nombre_principal_val; entidad['tipo'] = tipo_entidad_val
            entidad['aliases'] = list({frozenset(item.items()): item for item in aliases}.values()) 
            entidad['direcciones'] = list({frozenset(item.items()): item for item in direcciones}.values()) 
            entidad['identificadores'] = list({frozenset(item.items()): item for item in identificadores}.values()) 
            entidad['caracteristicas'] = list({frozenset(item.items()): item for item in caracteristicas}.values()) 
            entidad['programas'] = list(set(programas))
            if entidad.get('uid') or entidad.get('nombre_principal'): lista_completa_entidades.append(entidad)
        logging.info(f"UK: Análisis XML completado. Se extrajeron {len(lista_completa_entidades)} entidades únicas por GroupID.")
    except ET.ParseError as e: logging.error(f"Error de parsing XML en archivo UK {ruta_archivo_xml}: {e}")
    except Exception as e: logging.error(f"Error inesperado al analizar UK {ruta_archivo_xml}: {e}", exc_info=True)
    return lista_completa_entidades

# --- Configuración de URLs y Nombres de Archivo ---
SOURCES_CONFIG = {
    "OFAC": {
        "url": "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN_ENHANCED.XML", 
        "local_filename": "sdn_enhanced.xml", 
        "parser_function": analizar_ofac_xml_sdn_enhanced
    },
    "ONU": {
        "url": "https://scsanctions.un.org/resources/xml/en/consolidated.xml",
        "local_filename": "onu_consolidated.xml",
        "parser_function": analizar_onu_xml
    },
    "UE": {
        "url": "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList/content?token=dG9rZW4tMjAxNw",
        "local_filename": "ue_consolidated.xml",
        "parser_function": analizar_ue_xml
    },
    "UK": {
        "url": "https://ofsistorage.blob.core.windows.net/publishlive/2022format/ConList.xml",
        "local_filename": "OFSI_List_2022.xml", 
        "parser_function": analizar_uk_xml
    }
}

# --- Funciones de Descarga ---
def descargar_archivo(url, nombre_archivo_local, fuente_nombre):
    logging.info(f"Intentando descargar {fuente_nombre} desde {url}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(url, headers=headers, timeout=120)
        response.raise_for_status()
        download_dir = "downloaded_lists"
        if not os.path.exists(download_dir): os.makedirs(download_dir)
        path_completo_local = os.path.join(download_dir, nombre_archivo_local)
        with open(path_completo_local, 'wb') as f: f.write(response.content)
        logging.info(f"{fuente_nombre} descargado exitosamente y guardado como {path_completo_local}")
        return path_completo_local
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al descargar {fuente_nombre} desde {url}: {e}")
        path_completo_local_fallback = os.path.join("downloaded_lists", nombre_archivo_local)
        if os.path.exists(path_completo_local_fallback): logging.warning(f"Usando archivo local existente {path_completo_local_fallback} para {fuente_nombre}."); return path_completo_local_fallback
        elif os.path.exists(nombre_archivo_local): logging.warning(f"Usando archivo local existente {nombre_archivo_local} (en raíz) para {fuente_nombre}."); return nombre_archivo_local
        else: logging.error(f"Archivo local {nombre_archivo_local} no encontrado. No se puede procesar {fuente_nombre}."); return None

# --- INICIO: NUEVAS FUNCIONES DE BASE DE DATOS SQLite ---
def conectar_db_sqlite(db_file="sanctions.db"):
    """Conecta a la base de datos SQLite y devuelve la conexión."""
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = ON;")
        logging.info(f"Conexión exitosa a la base de datos SQLite en '{db_file}'.")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error al conectar a SQLite: {e}")
        return None

def crear_tablas_sqlite(conn):
    """Crea las tablas en la base de datos SQLite si no existen."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Entidades (
                uid TEXT PRIMARY KEY, nombre_principal TEXT, tipo TEXT, fuente_lista TEXT,
                fecha_actualizacion_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Alias (
                id INTEGER PRIMARY KEY AUTOINCREMENT, entidad_uid TEXT, nombre_alias TEXT, tipo_alias TEXT, idioma_escritura TEXT,
                FOREIGN KEY (entidad_uid) REFERENCES Entidades (uid) ON DELETE CASCADE,
                UNIQUE(entidad_uid, nombre_alias, tipo_alias, idioma_escritura)
            )""")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Direcciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT, entidad_uid TEXT, calle1 TEXT, ciudad TEXT, pais TEXT, codigo_postal TEXT,
                direccion_completa TEXT, region TEXT, lugar TEXT, po_box TEXT,
                FOREIGN KEY (entidad_uid) REFERENCES Entidades (uid) ON DELETE CASCADE,
                UNIQUE(entidad_uid, direccion_completa)
            )""")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Programas (
                id INTEGER PRIMARY KEY AUTOINCREMENT, entidad_uid TEXT, programa TEXT,
                FOREIGN KEY (entidad_uid) REFERENCES Entidades (uid) ON DELETE CASCADE,
                UNIQUE(entidad_uid, programa)
            )""")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Identificadores (
                id INTEGER PRIMARY KEY AUTOINCREMENT, entidad_uid TEXT, tipo_identificador TEXT, numero_identificador TEXT,
                pais_emisor TEXT, comentarios TEXT,
                FOREIGN KEY (entidad_uid) REFERENCES Entidades (uid) ON DELETE CASCADE,
                UNIQUE(entidad_uid, tipo_identificador, numero_identificador)
            )""")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CaracteristicasAdicionales (
                id INTEGER PRIMARY KEY AUTOINCREMENT, entidad_uid TEXT, tipo_caracteristica TEXT, valor_caracteristica TEXT,
                FOREIGN KEY (entidad_uid) REFERENCES Entidades (uid) ON DELETE CASCADE,
                UNIQUE(entidad_uid, tipo_caracteristica, valor_caracteristica)
            )""")
        conn.commit()
        logging.info("Todas las tablas verificadas/creadas en SQLite.")
    except sqlite3.Error as e:
        logging.error(f"Error al crear/verificar las tablas en SQLite: {e}")
        conn.rollback()

def limpiar_tablas_sqlite(conn):
    """Limpia todas las tablas en SQLite usando DELETE."""
    try:
        cursor = conn.cursor()
        logging.info("Limpiando tablas existentes en SQLite (DELETE)...")
        tablas = ["Alias", "Direcciones", "Programas", "Identificadores", "CaracteristicasAdicionales", "Entidades"]
        for tabla in tablas:
            cursor.execute(f"DELETE FROM {tabla};")
        cursor.execute("DELETE FROM sqlite_sequence;") # Resetea contadores de AUTOINCREMENT
        conn.commit()
        logging.info("Tablas SQLite limpiadas exitosamente.")
    except sqlite3.Error as e:
        logging.error(f"Error al limpiar las tablas de SQLite: {e}")
        conn.rollback()

def guardar_datos_en_db_sqlite(conn, lista_entidades, fuente_lista_actual):
    """Guarda una lista de entidades en la BD SQLite usando executemany y ON CONFLICT."""
    if not lista_entidades:
        logging.warning(f"La lista de entidades para guardar de {fuente_lista_actual} está vacía.")
        return

    logging.info(f"Iniciando guardado de {len(lista_entidades)} entidades de {fuente_lista_actual} en SQLite...")
    cursor = conn.cursor()
    
    entidades_tuples, alias_tuples, direcciones_tuples, programas_tuples, identificadores_tuples, caracteristicas_tuples = [], [], [], [], [], []

    for entidad in lista_entidades:
        uid = entidad.get('uid')
        nombre_principal = entidad.get('nombre_principal')
        if not uid:
            uid = f"{fuente_lista_actual}_NO_UID_{nombre_principal[:40].replace(' ', '_') if nombre_principal else 'UNKNOWN'}"
        
        entidades_tuples.append((uid, nombre_principal, entidad.get('tipo'), fuente_lista_actual))
        for item in entidad.get('aliases', []): alias_tuples.append((uid, item.get('nombre_alias'), item.get('tipo_alias'), item.get('idioma_escritura')))
        for item in entidad.get('direcciones', []): direcciones_tuples.append((uid, item.get('calle1'), item.get('ciudad'), item.get('pais'), item.get('codigo_postal'), item.get('direccion_completa'), item.get('region'), item.get('lugar'), item.get('po_box')))
        for item in entidad.get('programas', []): programas_tuples.append((uid, item))
        for item in entidad.get('identificadores', []): identificadores_tuples.append((uid, item.get('tipo_identificador'), item.get('numero_identificador'), item.get('pais_emisor'), item.get('comentarios')))
        for item in entidad.get('caracteristicas', []): caracteristicas_tuples.append((uid, item.get('tipo_caracteristica'), item.get('valor_caracteristica')))

    try:
        sql_entidades = "INSERT INTO Entidades (uid, nombre_principal, tipo, fuente_lista) VALUES (?, ?, ?, ?) ON CONFLICT (uid) DO UPDATE SET nombre_principal = excluded.nombre_principal, tipo = excluded.tipo, fuente_lista = excluded.fuente_lista, fecha_actualizacion_registro = CURRENT_TIMESTAMP"
        cursor.executemany(sql_entidades, entidades_tuples)

        sql_alias = "INSERT INTO Alias (entidad_uid, nombre_alias, tipo_alias, idioma_escritura) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING"
        cursor.executemany(sql_alias, alias_tuples)

        sql_direcciones = "INSERT INTO Direcciones (entidad_uid, calle1, ciudad, pais, codigo_postal, direccion_completa, region, lugar, po_box) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING"
        cursor.executemany(sql_direcciones, direcciones_tuples)

        sql_programas = "INSERT INTO Programas (entidad_uid, programa) VALUES (?, ?) ON CONFLICT DO NOTHING"
        cursor.executemany(sql_programas, programas_tuples)

        sql_identificadores = "INSERT INTO Identificadores (entidad_uid, tipo_identificador, numero_identificador, pais_emisor, comentarios) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING"
        cursor.executemany(sql_identificadores, identificadores_tuples)

        sql_caracteristicas = "INSERT INTO CaracteristicasAdicionales (entidad_uid, tipo_caracteristica, valor_caracteristica) VALUES (?, ?, ?) ON CONFLICT DO NOTHING"
        cursor.executemany(sql_caracteristicas, caracteristicas_tuples)

        conn.commit()
        logging.info(f"Guardado de {fuente_lista_actual} en SQLite completado.")

    except sqlite3.Error as e:
        logging.error(f"Error durante guardado de {fuente_lista_actual} en DB SQLite: {e}")
        conn.rollback()
# --- FIN: NUEVAS FUNCIONES DE BASE DE DATOS SQLite ---

# --- Funciones de Base de Datos PostgreSQL (Originales) ---
def conectar_db_postgres():
    try:
        conn = psycopg2.connect(host=os.environ.get('DB_HOST'), database=os.environ.get('DB_NAME'), user=os.environ.get('DB_USER'), password=os.environ.get('DB_PASSWORD'), port=os.environ.get('DB_PORT', '5432'))
        logging.info("Conexión exitosa a la base de datos PostgreSQL.")
        return conn
    except Exception as e: logging.error(f"Error al conectar a PostgreSQL: {e}"); return None

def crear_tablas_postgres(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS Entidades (uid TEXT PRIMARY KEY, nombre_principal TEXT, tipo TEXT, fuente_lista TEXT, fecha_actualizacion_registro TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP)""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS Alias (id SERIAL PRIMARY KEY, entidad_uid TEXT REFERENCES Entidades (uid) ON DELETE CASCADE, nombre_alias TEXT, tipo_alias TEXT, idioma_escritura TEXT, UNIQUE(entidad_uid, nombre_alias, tipo_alias, idioma_escritura))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS Direcciones (id SERIAL PRIMARY KEY, entidad_uid TEXT REFERENCES Entidades (uid) ON DELETE CASCADE, calle1 TEXT, ciudad TEXT, pais TEXT, codigo_postal TEXT, direccion_completa TEXT, region TEXT, lugar TEXT, po_box TEXT, UNIQUE(entidad_uid, direccion_completa))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS Programas (id SERIAL PRIMARY KEY, entidad_uid TEXT REFERENCES Entidades (uid) ON DELETE CASCADE, programa TEXT, UNIQUE(entidad_uid, programa))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS Identificadores (id SERIAL PRIMARY KEY, entidad_uid TEXT REFERENCES Entidades (uid) ON DELETE CASCADE, tipo_identificador TEXT, numero_identificador TEXT, pais_emisor TEXT, comentarios TEXT, UNIQUE(entidad_uid, tipo_identificador, numero_identificador))""")
        cursor.execute("""CREATE TABLE IF NOT EXISTS CaracteristicasAdicionales (id SERIAL PRIMARY KEY, entidad_uid TEXT REFERENCES Entidades (uid) ON DELETE CASCADE, tipo_caracteristica TEXT, valor_caracteristica TEXT, UNIQUE(entidad_uid, tipo_caracteristica, valor_caracteristica))""")
        conn.commit(); logging.info("Todas las tablas verificadas/creadas en PostgreSQL.")
    except psycopg2.Error as e: logging.error(f"Error al crear/verificar las tablas en PostgreSQL: {e}"); conn.rollback()

def limpiar_tablas_postgres(conn):
    try:
        cursor = conn.cursor(); logging.info("Limpiando tablas existentes (TRUNCATE)...")
        cursor.execute("TRUNCATE TABLE Alias, Direcciones, Programas, Identificadores, CaracteristicasAdicionales, Entidades RESTART IDENTITY CASCADE")
        conn.commit(); logging.info("Tablas limpiadas exitosamente.")
    except psycopg2.Error as e: logging.error(f"Error al limpiar las tablas: {e}"); conn.rollback()

def guardar_datos_en_db_postgres(conn, lista_entidades, fuente_lista_actual):
    if not lista_entidades:
        logging.warning(f"La lista de entidades para guardar de {fuente_lista_actual} está vacía.")
        return

    logging.info(f"Iniciando guardado de {len(lista_entidades)} entidades de {fuente_lista_actual} en PostgreSQL usando execute_values...")
    cursor = conn.cursor()
    
    entidades_data_tuples, alias_data_tuples, direcciones_data_tuples, programas_data_tuples, identificadores_data_tuples, caracteristicas_data_tuples = [], [], [], [], [], []
    TRANSACTION_BATCH_SIZE = 200 
    EXECUTE_VALUES_PAGE_SIZE = 100 
    ent_proc_total = 0

    try:
        for i, entidad_data_dict in enumerate(lista_entidades):
            uid = entidad_data_dict.get('uid')
            nombre_principal = entidad_data_dict.get('nombre_principal')
            tipo = entidad_data_dict.get('tipo')

            if not uid and nombre_principal:
                entidad_uid_str = f"{fuente_lista_actual}_NO_UID_{nombre_principal[:40].replace(' ', '_').replace('/', '_').replace(':', '_')}"
            elif not uid and not nombre_principal:
                logging.warning(f"Entidad de {fuente_lista_actual} sin UID ni nombre principal. Saltando: {entidad_data_dict}")
                continue
            else:
                entidad_uid_str = str(uid)

            entidades_data_tuples.append((entidad_uid_str, nombre_principal, tipo, fuente_lista_actual))
            for alias in entidad_data_dict.get('aliases', []):
                if alias.get('nombre_alias'): alias_data_tuples.append((entidad_uid_str, alias.get('nombre_alias'), alias.get('tipo_alias'), alias.get('idioma_escritura')))
            for direccion in entidad_data_dict.get('direcciones', []):
                 if direccion.get('direccion_completa'): direcciones_data_tuples.append((entidad_uid_str, direccion.get('calle1'), direccion.get('ciudad'), direccion.get('pais'), direccion.get('codigo_postal'), direccion.get('direccion_completa'), direccion.get('region'), direccion.get('lugar'), direccion.get('po_box')))
            for programa_item in entidad_data_dict.get('programas', []):
                if programa_item: programas_data_tuples.append((entidad_uid_str, programa_item))
            for identificador in entidad_data_dict.get('identificadores', []):
                if identificador.get('numero_identificador'): identificadores_data_tuples.append((entidad_uid_str, identificador.get('tipo_identificador'), identificador.get('numero_identificador'), identificador.get('pais_emisor'), identificador.get('comentarios')))
            for caracteristica in entidad_data_dict.get('caracteristicas', []):
                if caracteristica.get('valor_caracteristica'): caracteristicas_data_tuples.append((entidad_uid_str, caracteristica.get('tipo_caracteristica'), caracteristica.get('valor_caracteristica')))
            
            ent_proc_total += 1

            if (i + 1) % TRANSACTION_BATCH_SIZE == 0 or (i + 1) == len(lista_entidades):
                if entidades_data_tuples:
                    sql_entidades = "INSERT INTO Entidades (uid, nombre_principal, tipo, fuente_lista) VALUES %s ON CONFLICT (uid) DO UPDATE SET nombre_principal = EXCLUDED.nombre_principal, tipo = EXCLUDED.tipo, fuente_lista = EXCLUDED.fuente_lista, fecha_actualizacion_registro = CURRENT_TIMESTAMP"
                    execute_values(cursor, sql_entidades, entidades_data_tuples, page_size=EXECUTE_VALUES_PAGE_SIZE)
                    entidades_data_tuples = []
                if alias_data_tuples:
                    sql_alias = "INSERT INTO Alias (entidad_uid, nombre_alias, tipo_alias, idioma_escritura) VALUES %s ON CONFLICT (entidad_uid, nombre_alias, tipo_alias, idioma_escritura) DO NOTHING"
                    execute_values(cursor, sql_alias, alias_data_tuples, page_size=EXECUTE_VALUES_PAGE_SIZE)
                    alias_data_tuples = []
                if direcciones_data_tuples:
                    sql_direcciones = "INSERT INTO Direcciones (entidad_uid, calle1, ciudad, pais, codigo_postal, direccion_completa, region, lugar, po_box) VALUES %s ON CONFLICT (entidad_uid, direccion_completa) DO NOTHING"
                    execute_values(cursor, sql_direcciones, direcciones_data_tuples, page_size=EXECUTE_VALUES_PAGE_SIZE)
                    direcciones_data_tuples = []
                if programas_data_tuples:
                    sql_programas = "INSERT INTO Programas (entidad_uid, programa) VALUES %s ON CONFLICT (entidad_uid, programa) DO NOTHING"
                    execute_values(cursor, sql_programas, programas_data_tuples, page_size=EXECUTE_VALUES_PAGE_SIZE)
                    programas_data_tuples = []
                if identificadores_data_tuples:
                    sql_identificadores = "INSERT INTO Identificadores (entidad_uid, tipo_identificador, numero_identificador, pais_emisor, comentarios) VALUES %s ON CONFLICT (entidad_uid, tipo_identificador, numero_identificador) DO NOTHING"
                    execute_values(cursor, sql_identificadores, identificadores_data_tuples, page_size=EXECUTE_VALUES_PAGE_SIZE)
                    identificadores_data_tuples = []
                if caracteristicas_data_tuples:
                    sql_caracteristicas = "INSERT INTO CaracteristicasAdicionales (entidad_uid, tipo_caracteristica, valor_caracteristica) VALUES %s ON CONFLICT (entidad_uid, tipo_caracteristica, valor_caracteristica) DO NOTHING"
                    execute_values(cursor, sql_caracteristicas, caracteristicas_data_tuples, page_size=EXECUTE_VALUES_PAGE_SIZE)
                    caracteristicas_data_tuples = []
                conn.commit()
                logging.info(f"Commit de transacción realizado después de procesar {ent_proc_total} entidades principales de {fuente_lista_actual}.")
        
        logging.info(f"Guardado de {fuente_lista_actual} completado. Total entidades principales procesadas: {ent_proc_total}.")

    except psycopg2.Error as e:
        logging.error(f"Error durante guardado de {fuente_lista_actual} en DB PostgreSQL: {e}")
        try: conn.rollback(); logging.info("Rollback realizado debido a error en el lote.")
        except psycopg2.Error as rb_error: logging.error(f"Error durante el rollback: {rb_error}. La conexión puede estar cerrada.")
        if "closed the connection unexpectedly" in str(e).lower() or "connection already closed" in str(e).lower() or "no connection to the server" in str(e).lower():
             logging.error("La conexión con el servidor se perdió. No se pueden procesar más lotes para esta fuente.")
    except Exception as general_e:
        logging.error(f"Error general inesperado durante guardado de {fuente_lista_actual}: {general_e}", exc_info=True)
        try: conn.rollback(); logging.info("Rollback realizado debido a error general.")
        except Exception as rb_general_error: logging.error(f"Error durante el rollback general: {rb_general_error}.")

# --- Flujo Principal de Ejecución (MODIFICADO) ---
if __name__ == "__main__":
    # --- SELECCIONA TU BASE DE DATOS AQUÍ ---
    # Cambia a 'postgres' para usar PostgreSQL o 'sqlite' para usar el archivo local.
    USE_DATABASE_TYPE = 'sqlite' 

    conn = None
    if USE_DATABASE_TYPE == 'postgres':
        conn = conectar_db_postgres()
    elif USE_DATABASE_TYPE == 'sqlite':
        conn = conectar_db_sqlite("sanctions_lists.db") # El archivo se creará en la misma carpeta
    
    if conn:
        # Creación y limpieza de tablas según el tipo de BD
        if USE_DATABASE_TYPE == 'postgres':
            crear_tablas_postgres(conn)
            limpiar_tablas_postgres(conn)
        elif USE_DATABASE_TYPE == 'sqlite':
            crear_tablas_sqlite(conn)
            limpiar_tablas_sqlite(conn)

        # Bucle principal de procesamiento de fuentes
        for fuente_nombre, config in SOURCES_CONFIG.items():
            logging.info(f"--- Iniciando Proceso {fuente_nombre} ---")
            ruta_archivo_xml = descargar_archivo(config["url"], config["local_filename"], fuente_nombre)
            
            if ruta_archivo_xml:
                lista_entidades_procesadas = config["parser_function"](ruta_archivo_xml)
                if lista_entidades_procesadas:
                    # Guardado de datos en la base de datos seleccionada
                    if USE_DATABASE_TYPE == 'postgres':
                        guardar_datos_en_db_postgres(conn, lista_entidades_procesadas, fuente_nombre)
                    elif USE_DATABASE_TYPE == 'sqlite':
                        guardar_datos_en_db_sqlite(conn, lista_entidades_procesadas, fuente_nombre)
                else:
                    logging.warning(f"{fuente_nombre}: No se extrajeron datos del archivo XML.")
            else:
                logging.error(f"{fuente_nombre}: No se pudo obtener el archivo XML. Saltando esta fuente.")
            logging.info(f"--- Proceso {fuente_nombre} Completado ---")
            
        conn.close()
        logging.info(f"Conexión a la base de datos ({USE_DATABASE_TYPE}) cerrada.")
    else:
        logging.error(f"No se pudo conectar a la base de datos ({USE_DATABASE_TYPE}). El script no puede continuar.")
        
    logging.info("Todos los procesos de parsing y actualización de la base de datos han finalizado.")
