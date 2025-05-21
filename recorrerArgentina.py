import pygsheets
import unicodedata
import re
from datetime import datetime
import psycopg2
from psycopg2 import Error
import argparse
import time  # Para los reintentos con espera
import random  # Para backoff exponencial

# Configuración global para reintentos
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 2  # segundos
MAX_RETRY_DELAY = 60  # segundos máximo de espera entre reintentos

def get_db_connection():
    """Establece una conexión a la base de datos con reintentos"""
    retry_count = 0
    last_error = None
    
    while retry_count < MAX_RETRIES:
        try:
            connection = psycopg2.connect(
                host="ep-plain-voice-a47r1b05-pooler.us-east-1.aws.neon.tech",
                database="verceldb",
                user="default",
                password="Rx3Eq5iQwMpl",
                sslmode="require",
                # Parámetros de optimización básicos compatibles con Neon
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
                client_encoding='utf8',
                connect_timeout=30  # Aumentar el timeout de conexión
            )
            
            # Configurar el cursor para usar diccionarios
            connection.autocommit = False
            
            # Verificar que la conexión está activa
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            
            print("Conexión a la base de datos establecida correctamente")
            return connection
        except Error as e:
            last_error = e
            retry_count += 1
            
            if retry_count < MAX_RETRIES:
                # Calcular tiempo de espera con backoff exponencial y jitter
                delay = min(INITIAL_RETRY_DELAY * (2 ** (retry_count - 1)) + random.uniform(0, 1), MAX_RETRY_DELAY)
                print(f"Error al conectar a PostgreSQL (intento {retry_count}/{MAX_RETRIES}): {e}")
                print(f"Reintentando en {delay:.2f} segundos...")
                time.sleep(delay)
            else:
                print(f"Error al conectar a PostgreSQL después de {MAX_RETRIES} intentos: {e}")
    
    return None

def ensure_db_connection(connection):
    """Verifica que la conexión está activa y la restablece si es necesario"""
    if connection is None:
        return get_db_connection()
    
    try:
        # Verificar que la conexión está activa
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        return connection
    except (Error, AttributeError) as e:
        print(f"La conexión a la base de datos se ha perdido: {e}")
        print("Intentando reconectar...")
        return get_db_connection()

def execute_with_retry(connection, query, params=None, is_many=False, batch=None):
    """Ejecuta una consulta con reintentos automáticos en caso de error de conexión"""
    retry_count = 0
    last_error = None
    result = None
    
    while retry_count < MAX_RETRIES:
        try:
            # Asegurar que la conexión está activa
            conn = ensure_db_connection(connection)
            if conn is None:
                raise Exception("No se pudo establecer conexión a la base de datos")
            
            cursor = conn.cursor()
            
            if is_many and batch:
                cursor.executemany(query, batch)
                affected_rows = cursor.rowcount
                conn.commit()
                cursor.close()
                return conn, affected_rows
            elif params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if query.strip().upper().startswith(("SELECT", "SHOW", "DESCRIBE")):
                result = cursor.fetchall()
            
            conn.commit()
            cursor.close()
            return conn, result
        except Error as e:
            last_error = e
            retry_count += 1
            
            # Intentar cerrar la conexión si está abierta
            try:
                if 'conn' in locals() and conn:
                    conn.rollback()
            except:
                pass
            
            if retry_count < MAX_RETRIES:
                # Calcular tiempo de espera con backoff exponencial y jitter
                delay = min(INITIAL_RETRY_DELAY * (2 ** (retry_count - 1)) + random.uniform(0, 1), MAX_RETRY_DELAY)
                print(f"Error en la consulta (intento {retry_count}/{MAX_RETRIES}): {e}")
                print(f"Reintentando en {delay:.2f} segundos...")
                time.sleep(delay)
                connection = None  # Forzar reconexión
            else:
                print(f"Error en la consulta después de {MAX_RETRIES} intentos: {e}")
                raise e
    
    raise last_error if last_error else Exception("Error desconocido al ejecutar la consulta")

def get_all_tickets_data(connection):
    """Obtiene todos los datos de tickets de una sola vez para evitar múltiples consultas"""
    try:
        query = """
            SELECT artista, fecha_show, fecha_venta, venta_diaria, venta_total 
            FROM tickets
        """
        
        connection, resultados = execute_with_retry(connection, query)
        datos_por_artista_show = {}
        
        for artista, fecha_show, fecha_venta, venta_diaria, venta_total in resultados:
            # Crear clave compuesta para artista y fecha_show
            clave = (artista, fecha_show)
            
            # Inicializar el diccionario para esta clave si no existe
            if clave not in datos_por_artista_show:
                datos_por_artista_show[clave] = {
                    'fechas': [],
                    'datos_por_fecha': {}
                }
            
            # Añadir fecha_venta a la lista de fechas
            datos_por_artista_show[clave]['fechas'].append(fecha_venta)
            
            # Añadir datos para esta fecha_venta
            datos_por_artista_show[clave]['datos_por_fecha'][fecha_venta] = {
                'venta_diaria': venta_diaria,
                'venta_total': venta_total
            }
        
        return connection, datos_por_artista_show
    except Exception as e:
        print(f"Error al obtener datos de tickets: {e}")
        return connection, {}

def get_all_shows_ticketing(connection):
    """Obtiene todos los datos de shows_ticketing de una sola vez"""
    try:
        query = """
            SELECT artista, fecha
            FROM shows_ticketing
        """
        
        connection, resultados = execute_with_retry(connection, query)
        shows_ticketing = set()
        
        for artista, fecha in resultados:
            # Crear clave compuesta para artista y fecha
            clave = (artista, fecha)
            shows_ticketing.add(clave)
        
        return connection, shows_ticketing
    except Exception as e:
        print(f"Error al obtener datos de shows_ticketing: {e}")
        return connection, set()

def get_fechas_venta_y_datos(datos_tickets, artista, fecha_show):
    """Obtiene fechas de venta y datos para un artista y fecha de show específicos"""
    clave = (artista, fecha_show)
    
    if clave in datos_tickets:
        return datos_tickets[clave]['fechas'], datos_tickets[clave]['datos_por_fecha']
    else:
        return [], {}

def check_shows_ticketing(shows_ticketing, artista, fecha_show):
    """Verifica si un show existe en shows_ticketing"""
    return (artista, fecha_show) in shows_ticketing

def update_db_values(connection, updates):
    """Actualiza los valores en la base de datos para corregir discrepancias con reintentos"""
    if not updates:
        return connection, 0
    
    updated_count = 0
    
    # Preparar la consulta para actualización por lotes
    query = """
        UPDATE tickets 
        SET venta_diaria = %s, venta_total = %s, monto_diario_ars = %s, recaudacion_total = %s
        WHERE artista = %s 
        AND fecha_show = %s 
        AND fecha_venta = %s
    """
    
    # Crear una lista de tuplas para actualización por lotes
    batch_data = []
    for update in updates:
        batch_data.append((
            update['vd_sheet'], 
            update['vt_sheet'],
            update['md_sheet'],
            update['rt_sheet'],
            update['artista'], 
            update['fecha_show'], 
            update['fecha_venta']
        ))
    
    # Ejecutar actualizaciones en lotes más pequeños para Neon
    batch_size = 25  # Reducir aún más el tamaño del lote para evitar sobrecarga
    total_batches = (len(batch_data) + batch_size - 1) // batch_size
    
    print(f"Procesando {len(batch_data)} actualizaciones en {total_batches} lotes...")
    
    for i in range(0, len(batch_data), batch_size):
        batch = batch_data[i:i+batch_size]
        
        # Mostrar progreso más detallado
        batch_num = (i // batch_size) + 1
        print(f"  Procesando lote {batch_num}/{total_batches} ({len(batch)} registros)...")
        
        start_time = datetime.now()
        
        try:
            # Ejecutar con reintentos
            connection, rows_affected = execute_with_retry(connection, query, is_many=True, batch=batch)
            updated_count += rows_affected
            
            # Calcular y mostrar tiempo de procesamiento
            elapsed_time = (datetime.now() - start_time).total_seconds()
            records_per_second = len(batch) / elapsed_time if elapsed_time > 0 else 0
            
            # Mostrar progreso detallado
            print(f"  Lote {batch_num} completado en {elapsed_time:.2f} segundos ({records_per_second:.2f} registros/segundo)")
            print(f"  Progreso: {min(i+batch_size, len(batch_data))}/{len(batch_data)} registros procesados ({(min(i+batch_size, len(batch_data))*100/len(batch_data)):.1f}%)")
            
            # Pequeña pausa entre lotes para no sobrecargar la conexión
            if batch_num < total_batches:
                time.sleep(0.5)
                
        except Exception as e:
            print(f"Error al procesar el lote {batch_num}: {e}")
            # Continuar con el siguiente lote
    
    return connection, updated_count

def formatear_fecha(fecha_str, es_fecha_show=False):
    """Convierte diferentes formatos de fecha a 'aaaa-mm-dd'"""
    fecha_str = fecha_str.strip()
    
    # Si es fecha de show, siempre usar 2025
    if es_fecha_show:
        # Si ya está en formato aaaa-mm-dd
        if re.match(r'^\d{4}-\d{2}-\d{2}$', fecha_str):
            partes = fecha_str.split('-')
            return f"2025-{partes[1]}-{partes[2]}"
        
        # Caso especial: formato dd/mm (sin año)
        if re.match(r'^\d{1,2}/\d{1,2}$', fecha_str):
            try:
                partes = fecha_str.split('/')
                dia = partes[0].zfill(2)
                mes = int(partes[1])
                mes_str = str(mes).zfill(2)
                return f"2025-{mes_str}-{dia}"
            except Exception:
                return fecha_str
        
        # Intentar diferentes formatos comunes
        formatos = [
            '%d/%m/%Y',  # 31/12/2023
            '%d-%m-%Y',  # 31-12-2023
            '%d.%m.%Y',  # 31.12.2023
            '%d/%m/%y',  # 31/12/23
            '%Y/%m/%d',  # 2023/12/31
        ]
        
        for formato in formatos:
            try:
                fecha = datetime.strptime(fecha_str, formato)
                mes = fecha.month
                dia = fecha.day
                return f"2025-{mes:02d}-{dia:02d}"
            except ValueError:
                continue
        
        # Si no se pudo convertir, devolver el original
        return fecha_str
    
    # Para fechas de venta (FV), mantener la lógica actual
    # Si ya está en formato aaaa-mm-dd, devolverlo tal cual pero asegurando que el año sea correcto según el mes
    if re.match(r'^\d{4}-\d{2}-\d{2}$', fecha_str):
        # Extraer mes y día
        partes = fecha_str.split('-')
        mes = int(partes[1])
        # Asignar año según el mes: junio a diciembre -> 2024, enero a mayo -> 2025
        año = "2024" if 6 <= mes <= 12 else "2025"
        return f"{año}-{partes[1]}-{partes[2]}"
    
    # Caso especial: formato dd/mm (sin año)
    if re.match(r'^\d{1,2}/\d{1,2}$', fecha_str):
        try:
            # Separar día y mes
            partes = fecha_str.split('/')
            dia = partes[0].zfill(2)  # Asegurar que tenga 2 dígitos
            mes = int(partes[1])  # Convertir a entero para comparar
            
            # Asignar año según el mes: junio a diciembre -> 2024, enero a mayo -> 2025
            año = "2024" if 6 <= mes <= 12 else "2025"
            
            mes_str = str(mes).zfill(2)  # Convertir de nuevo a string con 2 dígitos
            
            # Crear la fecha en formato aaaa-mm-dd
            return f"{año}-{mes_str}-{dia}"
        except Exception:
            # Si hay algún error, devolver el original
            return fecha_str
    
    # Intentar diferentes formatos comunes
    formatos = [
        '%d/%m/%Y',  # 31/12/2023
        '%d-%m-%Y',  # 31-12-2023
        '%d.%m.%Y',  # 31.12.2023
        '%d/%m/%y',  # 31/12/23
        '%Y/%m/%d',  # 2023/12/31
    ]
    
    for formato in formatos:
        try:
            fecha = datetime.strptime(fecha_str, formato)
            # Extraer mes y día
            mes = fecha.month
            dia = fecha.day
            # Asignar año según el mes: junio a diciembre -> 2024, enero a mayo -> 2025
            año = "2024" if 6 <= mes <= 12 else "2025"
            return f"{año}-{mes:02d}-{dia:02d}"
        except ValueError:
            continue
    
    # Si no se pudo convertir, devolver el original
    return fecha_str

def process_artist_name(artist):
    if artist.upper().replace(' ', '') == 'HA*ASH':
        return 'HAASH'
    if artist.upper() == 'C.R.O':
        return 'C.R.O'  # Mantener C.R.O tal cual
    if artist.upper() == 'KANY GARCÍA' or artist.upper() == 'KANY GARCIA':
        return 'Kany Garcia'  # Convertir Kany García a Kany Garcia
    
    artist = ''.join(c for c in unicodedata.normalize('NFD', artist)
                    if unicodedata.category(c) != 'Mn')
    
    words = artist.strip().split()
    processed_words = []
    
    for word in words:
        if word:
            processed_word = word.lower().capitalize()
            processed_words.append(processed_word)
    
    return ' '.join(processed_words)

def normalizar_numero(valor):
    """Normaliza un valor numérico eliminando separadores de miles y símbolos de moneda"""
    # Si ya es un número, simplemente devolverlo como float
    if isinstance(valor, (int, float)):
        return float(valor)
    
    # Si es None o un valor vacío o un guión
    if not valor or valor == '-' or valor == '$ -':
        return 0.0
    
    # Eliminar símbolos de moneda y espacios
    valor = valor.replace('$', '').replace(' ', '').strip()
    
    # Detectar formato español (1.234,56 o 1.234)
    if '.' in valor and (',' in valor or len(valor.split('.')[-1]) > 2 or valor.count('.') > 1):
        # Es un número con formato español (punto como separador de miles)
        valor = valor.replace('.', '')  # Eliminar puntos (separadores de miles)
        valor = valor.replace(',', '.')  # Reemplazar coma por punto (para decimales)
    
    try:
        return float(valor)
    except ValueError:
        # Si falla, intentar otros métodos
        try:
            # Intentar reemplazando coma por punto
            return float(valor.replace(',', '.'))
        except ValueError:
            return 0.0

def limpiar_valor_numerico(valor):
    """Limpia un valor numérico dejando solo números, puntos y comas"""
    if not valor or valor == '-' or valor == '$ -':
        return '0'
    
    # Eliminar símbolos de moneda y espacios
    valor = valor.replace('$', '').strip()
    
    # Mantener solo números, puntos y comas
    return valor

def recorrer_argentina(solo_actualizaciones=False, solo_discrepancias=False):
    # URL del sheet de Argentina - Actualizada según AgregacionTickets.py
    sheet_url = "https://docs.google.com/spreadsheets/d/18PvV89ic4-jV-SdsM2qsSI37AQG_ifCCXAgVBWJP_dY/edit?gid=139082797#gid=139082797"
    
    try:
        # Autorizar y abrir el sheet
        print("Autorizando y abriendo el Google Sheet...")
        gc = pygsheets.authorize(client_secret='client_secret.json', credentials_directory='./')
        sh = gc.open_by_url(sheet_url)
        
        print(f"\nProcesando el sheet de Argentina...")
        
        # Obtener todas las hojas excepto 'Resumen'
        worksheets = [wks for wks in sh.worksheets() if wks.title != 'Resumen']
        print(f"Total de hojas a procesar: {len(worksheets)}")
        
        # Conectar a la base de datos
        print("Conectando a la base de datos...")
        conn = get_db_connection()
        if not conn:
            print("No se pudo conectar a la base de datos.")
            return
        
        # Cargar todos los datos de tickets y shows_ticketing de una sola vez
        print("Cargando datos de la base de datos...")
        conn, datos_tickets = get_all_tickets_data(conn)
        conn, shows_ticketing = get_all_shows_ticketing(conn)
        print(f"Datos cargados: {len(datos_tickets)} artistas/shows en tickets, {len(shows_ticketing)} en shows_ticketing")
        
        # Contador de hojas procesadas
        hojas_procesadas = 0
        
        # Lista para almacenar shows con discrepancias
        shows_con_discrepancias = []
        
        # Lista para almacenar actualizaciones a realizar en la BD
        actualizaciones_bd = []
        
        # Lista para almacenar shows no encontrados
        shows_no_encontrados = []
        
        # Precarga de datos de todas las hojas para reducir llamadas a la API
        print("Precargando datos de todas las hojas...")
        datos_hojas = {}
        for wks in worksheets:
            try:
                # Obtener información básica de la hoja
                artista_raw = wks.get_value('B1').strip()
                fecha_show_raw = wks.get_value('B2').strip()
                
                # Obtener todos los valores de una vez
                all_values = wks.get_all_values(include_tailing_empty=False)
                
                datos_hojas[wks.title] = {
                    'artista_raw': artista_raw,
                    'fecha_show_raw': fecha_show_raw,
                    'all_values': all_values
                }
                print(f"  Hoja {wks.title} cargada: {artista_raw} - {fecha_show_raw}")
            except Exception as e:
                print(f"  Error al cargar la hoja {wks.title}: {e}")
        
        print(f"Datos precargados de {len(datos_hojas)} hojas")
        
        # Primera pasada: identificar hojas con actualizaciones pendientes
        if solo_actualizaciones:
            print("\nIdentificando hojas con actualizaciones pendientes...")
            hojas_con_actualizaciones = set()
            
            for hoja_title, hoja_datos in datos_hojas.items():
                # Obtener datos precargados
                artista_raw = hoja_datos['artista_raw']
                fecha_show_raw = hoja_datos['fecha_show_raw']
                
                # Procesar nombre del artista
                artista_procesado = process_artist_name(artista_raw)
                
                # Formatear fecha del show
                fecha_show_formateada = formatear_fecha(fecha_show_raw, es_fecha_show=True)
                
                # Verificar si existe en la base de datos
                fechas_venta, _ = get_fechas_venta_y_datos(datos_tickets, artista_procesado, fecha_show_formateada)
                if fechas_venta:
                    hojas_con_actualizaciones.add(hoja_title)
            
            print(f"Se encontraron {len(hojas_con_actualizaciones)} hojas con actualizaciones pendientes")
            
            # Filtrar las hojas a procesar
            datos_hojas = {k: v for k, v in datos_hojas.items() if k in hojas_con_actualizaciones}
        
        # Primera pasada para identificar hojas con discrepancias si se solicita
        if solo_discrepancias:
            print("\nIdentificando hojas con discrepancias...")
            hojas_con_discrepancias = set()
            hojas_analizadas = 0
            
            for hoja_title, hoja_datos in list(datos_hojas.items()):
                hojas_analizadas += 1
                print(f"  Analizando hoja {hojas_analizadas}/{len(datos_hojas)}: {hoja_title}...", end="")
                
                # Obtener datos precargados
                artista_raw = hoja_datos['artista_raw']
                fecha_show_raw = hoja_datos['fecha_show_raw']
                all_values = hoja_datos['all_values']
                
                # Procesar nombre del artista
                artista_procesado = process_artist_name(artista_raw)
                
                # Formatear fecha del show
                fecha_show_formateada = formatear_fecha(fecha_show_raw, es_fecha_show=True)
                
                # Obtener fechas de venta y datos de la base de datos
                fechas_venta, datos_por_fecha = get_fechas_venta_y_datos(datos_tickets, artista_procesado, fecha_show_formateada)
                
                if not fechas_venta:
                    print(" sin datos en BD")
                    continue
                
                # Verificar si hay discrepancias
                tiene_discrepancias = False
                
                # Comenzar desde la fila 14 (índice 13 en Python)
                row_index = 13
                
                # Recorrer filas hasta que la columna B esté vacía
                while row_index < len(all_values):
                    row = all_values[row_index]
                    
                    # Verificar si la columna B está vacía
                    if len(row) < 2 or not row[1].strip():
                        break
                    
                    # Extraer valores de las columnas B, C (índices 1, 2)
                    b_valor = row[1].strip() if len(row) > 1 else ''
                    c_valor = row[2].strip() if len(row) > 2 else ''
                    e_valor = row[4].strip() if len(row) > 4 else ''
                    
                    # Formatear la fecha (columna B - FV) al formato aaaa-mm-dd
                    if b_valor:
                        b_valor_formateado = formatear_fecha(b_valor)
                        
                        # Verificar si la fecha formateada coincide con alguna fecha_venta de la base de datos
                        if b_valor_formateado in fechas_venta:
                            # Obtener datos de la base de datos para esta fecha
                            datos_bd = datos_por_fecha[b_valor_formateado]
                            venta_diaria_bd = datos_bd['venta_diaria']
                            venta_total_bd = datos_bd['venta_total']
                            
                            # Normalizar valores
                            vd_sheet = normalizar_numero(c_valor)
                            vt_sheet = normalizar_numero(e_valor)
                            vd_bd = normalizar_numero(venta_diaria_bd)
                            vt_bd = normalizar_numero(venta_total_bd)
                            
                            # Comparación directa de valores normalizados
                            coincide_vd = abs(vd_sheet - vd_bd) < 0.01
                            coincide_vt = abs(vt_sheet - vt_bd) < 0.01
                            
                            # Si hay discrepancia, marcar la hoja
                            if not coincide_vd or not coincide_vt:
                                tiene_discrepancias = True
                                break
                    
                    # Incrementar el índice
                    row_index += 1
                
                if tiene_discrepancias:
                    hojas_con_discrepancias.add(hoja_title)
                    print(" tiene discrepancias")
                else:
                    print(" sin discrepancias")
            
            print(f"Se encontraron {len(hojas_con_discrepancias)} hojas con discrepancias")
            
            # Filtrar las hojas a procesar
            datos_hojas = {k: v for k, v in datos_hojas.items() if k in hojas_con_discrepancias}
        
        # Procesar todas las hojas con los datos precargados
        total_hojas = len(datos_hojas)
        for idx, (hoja_title, hoja_datos) in enumerate(datos_hojas.items(), 1):
            print(f"\nProcesando hoja {idx}/{total_hojas}: {hoja_title}")
            
            # Obtener datos precargados
            artista_raw = hoja_datos['artista_raw']
            fecha_show_raw = hoja_datos['fecha_show_raw']
            all_values = hoja_datos['all_values']
            
            # Procesar nombre del artista
            artista_procesado = process_artist_name(artista_raw)
            
            # Formatear fecha del show
            fecha_show_formateada = formatear_fecha(fecha_show_raw, es_fecha_show=True)
            
            print(f"Artista (B1): {artista_procesado}")
            print(f"Fecha Show (B2): {fecha_show_formateada}")
            
            # Obtener fechas de venta y datos de la base de datos
            fechas_venta, datos_por_fecha = get_fechas_venta_y_datos(datos_tickets, artista_procesado, fecha_show_formateada)
            if not fechas_venta:
                # Si no hay registros en tickets, verificar en shows_ticketing
                existe_en_shows_ticketing = check_shows_ticketing(shows_ticketing, artista_procesado, fecha_show_formateada)
                
                if existe_en_shows_ticketing:
                    # Si existe en shows_ticketing, continuar con la siguiente hoja sin mostrar nada
                    print(f"Encontrado en shows_ticketing: {artista_procesado} - {fecha_show_formateada}")
                    hojas_procesadas += 1
                    continue
                else:
                    # Si no existe en ninguna tabla, registrar y continuar con la siguiente hoja
                    print(f"\nNo se encontraron registros en ninguna tabla para {artista_procesado} - {fecha_show_formateada}")
                    shows_no_encontrados.append({
                        'artista': artista_procesado,
                        'fecha_show': fecha_show_formateada,
                        'hoja': hoja_title
                    })
                    hojas_procesadas += 1
                    continue
            
            # Imprimir encabezados
            print("-" * 100)
            print(f"{'FV':<15} {'VD':<15} {'MD':<15} {'VT':<15} {'RT':<15} {'Coincide VD':<10} {'Coincide VT':<10}")
            print("-" * 100)
            
            # Comenzar desde la fila 14 (índice 13 en Python)
            row_index = 13
            filas_coincidentes = 0
            
            # Variable para almacenar el último valor de RT válido
            ultimo_rt_valido = '$ 0'
            
            # Recorrer filas hasta que la columna B esté vacía
            while row_index < len(all_values):
                row = all_values[row_index]
                
                # Verificar si la columna B está vacía
                if len(row) < 2 or not row[1].strip():
                    break
                
                # Extraer valores de las columnas B, C, D, E, F (índices 1, 2, 3, 4, 5)
                b_valor = row[1].strip() if len(row) > 1 else ''
                c_valor = row[2].strip() if len(row) > 2 else ''
                d_valor = row[3].strip() if len(row) > 3 else ''
                e_valor = row[4].strip() if len(row) > 4 else ''
                f_valor = row[5].strip() if len(row) > 5 else ''
                
                # Formatear columna VD (c_valor): si está vacía o es '-', mostrar '0'
                if not c_valor or c_valor == '-':
                    c_valor = '0'
                
                # Formatear columna MD (d_valor): si está vacía o es '$ -', mostrar '$ 0'
                if not d_valor or d_valor == '$ -':
                    d_valor = '$ 0'
                
                # Formatear columna RT (f_valor): si está vacía o es '$ -', usar el último valor válido
                if not f_valor or f_valor == '$ -':
                    f_valor = ultimo_rt_valido
                else:
                    # Actualizar el último valor válido de RT
                    ultimo_rt_valido = f_valor
                
                # Formatear la fecha (columna B - FV) al formato aaaa-mm-dd
                if b_valor:
                    b_valor_formateado = formatear_fecha(b_valor)
                    
                    # Verificar si la fecha formateada coincide con alguna fecha_venta de la base de datos
                    if b_valor_formateado in fechas_venta:
                        filas_coincidentes += 1
                        
                        # Obtener datos de la base de datos para esta fecha
                        datos_bd = datos_por_fecha[b_valor_formateado]
                        venta_diaria_bd = datos_bd['venta_diaria']
                        venta_total_bd = datos_bd['venta_total']
                        
                        # Mostrar valores originales para depuración solo si hay discrepancia
                        vd_sheet = normalizar_numero(c_valor)
                        vt_sheet = normalizar_numero(e_valor)
                        vd_bd = normalizar_numero(venta_diaria_bd)
                        vt_bd = normalizar_numero(venta_total_bd)
                        
                        # Limpiar valores de MD y RT para la base de datos (solo números, puntos y comas)
                        md_sheet = limpiar_valor_numerico(d_valor)
                        rt_sheet = limpiar_valor_numerico(f_valor)
                        
                        # Comparación directa de valores normalizados
                        coincide_vd = abs(vd_sheet - vd_bd) < 0.01
                        coincide_vt = abs(vt_sheet - vt_bd) < 0.01
                        
                        # Registrar discrepancias
                        if not coincide_vd or not coincide_vt:
                            discrepancia = {
                                'artista': artista_procesado,
                                'fecha_show': fecha_show_formateada,
                                'fecha_venta': b_valor_formateado,
                                'vd_sheet': vd_sheet,
                                'vd_bd': vd_bd,
                                'vd_bd_raw': venta_diaria_bd,
                                'vt_sheet': vt_sheet,
                                'vt_bd': vt_bd,
                                'vt_bd_raw': venta_total_bd,
                                'coincide_vd': coincide_vd,
                                'coincide_vt': coincide_vt
                            }
                            shows_con_discrepancias.append(discrepancia)
                            
                            # Añadir a la lista de actualizaciones para la BD
                            actualizaciones_bd.append({
                                'artista': artista_procesado,
                                'fecha_show': fecha_show_formateada,
                                'fecha_venta': b_valor_formateado,
                                'vd_sheet': vd_sheet,
                                'vt_sheet': vt_sheet,
                                'md_sheet': md_sheet,
                                'rt_sheet': rt_sheet
                            })
                            
                            # Mostrar detalles de la discrepancia inmediatamente
                            print(f"\n⚠️ DISCREPANCIA DETECTADA en {artista_procesado} - {fecha_show_formateada} - {b_valor_formateado}:")
                            if not coincide_vd:
                                print(f"  VD Sheet: {c_valor} → {vd_sheet:.2f}")
                                print(f"  VD BD:    {venta_diaria_bd} → {vd_bd:.2f}")
                                print(f"  Diferencia: {abs(vd_sheet - vd_bd):.2f}")
                            if not coincide_vt:
                                print(f"  VT Sheet: {e_valor} → {vt_sheet:.2f}")
                                print(f"  VT BD:    {venta_total_bd} → {vt_bd:.2f}")
                                print(f"  Diferencia: {abs(vt_sheet - vt_bd):.2f}")
                        else:
                            # Si no hay discrepancias en VD y VT, aún así actualizar MD y RT
                            actualizaciones_bd.append({
                                'artista': artista_procesado,
                                'fecha_show': fecha_show_formateada,
                                'fecha_venta': b_valor_formateado,
                                'vd_sheet': vd_sheet,
                                'vt_sheet': vt_sheet,
                                'md_sheet': md_sheet,
                                'rt_sheet': rt_sheet
                            })
                        
                        # Imprimir los valores con indicadores de coincidencia
                        coincide_vd_str = "✓" if coincide_vd else "✗"
                        coincide_vt_str = "✓" if coincide_vt else "✗"
                        
                        print(f"{b_valor_formateado:<15} {c_valor:<15} {d_valor:<15} {e_valor:<15} {f_valor:<15} {coincide_vd_str:<10} {coincide_vt_str:<10}")
                
                # Incrementar el índice
                row_index += 1
            
            print("-" * 100)
            print(f"Total de filas coincidentes: {filas_coincidentes}")
            
            # Si no se encontraron coincidencias en esta hoja
            if filas_coincidentes == 0:
                print("\nNo se encontraron coincidencias entre las fechas del sheet y las fechas de venta en la base de datos.")
            
            # Incrementar el contador de hojas procesadas
            hojas_procesadas += 1
        
        # Mostrar resumen de discrepancias
        print("\n" + "=" * 100)
        print(f"✨ RESUMEN DE DISCREPANCIAS ✨")
        print("=" * 100)
        
        if shows_con_discrepancias:
            print(f"Se encontraron {len(shows_con_discrepancias)} shows con datos que no coinciden:")
            print("-" * 100)
            print(f"{'Artista':<20} {'Fecha Show':<12} {'Fecha Venta':<12} {'VD Sheet':<15} {'VD BD':<15} {'VT Sheet':<15} {'VT BD':<15} {'Problema':<20}")
            print("-" * 100)
            
            for disc in shows_con_discrepancias:
                problema = []
                if not disc['coincide_vd']:
                    problema.append("VD")
                if not disc['coincide_vt']:
                    problema.append("VT")
                
                problema_str = " y ".join(problema)
                
                print(f"{disc['artista']:<20} {disc['fecha_show']:<12} {disc['fecha_venta']:<12} "
                      f"{disc['vd_sheet']:<15.2f} {disc['vd_bd_raw']:<15} {disc['vt_sheet']:<15.2f} {disc['vt_bd_raw']:<15} "
                      f"{problema_str:<20}")
        else:
            print("¡No se encontraron discrepancias! Todos los datos coinciden correctamente.")
        
        # Mostrar resumen de shows no encontrados
        if shows_no_encontrados:
            print("\n" + "=" * 100)
            print(f"⚠️ SHOWS NO ENCONTRADOS EN LA BASE DE DATOS ⚠️")
            print("=" * 100)
            print(f"Se encontraron {len(shows_no_encontrados)} shows que no existen en la base de datos:")
            print("-" * 100)
            print(f"{'Artista':<30} {'Fecha Show':<15} {'Hoja':<20}")
            print("-" * 100)
            
            for show in shows_no_encontrados:
                print(f"{show['artista']:<30} {show['fecha_show']:<15} {show['hoja']:<20}")
            
            print("-" * 100)
        
        # Actualizar la base de datos con los valores del sheet
        if actualizaciones_bd:
            print("\n" + "=" * 100)
            print(f"🔄 ACTUALIZANDO BASE DE DATOS 🔄")
            print("=" * 100)
            
            print(f"Se van a actualizar {len(actualizaciones_bd)} registros en la base de datos...")
            print("Utilizando actualización por lotes para mayor velocidad...")
            
            # Verificar conexión antes de actualizar
            conn = ensure_db_connection(conn)
            if not conn:
                print("No se pudo establecer conexión a la base de datos para las actualizaciones.")
                return
            
            # Actualizar automáticamente sin pedir confirmación
            conn, registros_actualizados = update_db_values(conn, actualizaciones_bd)
            print(f"✅ Se han actualizado {registros_actualizados} registros en la base de datos.")
        
        print("=" * 100)
        print(f"🎵 Se han recorrido todas las hojas ({hojas_procesadas}/{len(worksheets)}) 🎵")
        print("=" * 100)
        
        # Cerrar la conexión a la base de datos
        if conn:
            try:
                conn.close()
                print("Conexión a la base de datos cerrada correctamente")
            except:
                pass
        
    except Exception as e:
        print(f"\nError al procesar el sheet: {e}")
        # Cerrar la conexión a la base de datos en caso de error
        if 'conn' in locals() and conn:
            try:
                conn.close()
            except:
                pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Procesar el sheet de Argentina y actualizar la base de datos.')
    parser.add_argument('--solo-actualizaciones', action='store_true', 
                        help='Procesar solo las hojas con actualizaciones pendientes')
    parser.add_argument('--solo-discrepancias', action='store_true',
                        help='Procesar solo las hojas con discrepancias')
    parser.add_argument('--reintentos', type=int, default=5,
                        help='Número máximo de reintentos para operaciones de base de datos')
    
    args = parser.parse_args()
    
    # Configurar reintentos globales
    if args.reintentos:
        MAX_RETRIES = args.reintentos
    
    recorrer_argentina(solo_actualizaciones=args.solo_actualizaciones,
                      solo_discrepancias=args.solo_discrepancias) 