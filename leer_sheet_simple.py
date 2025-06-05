import pygsheets
import re
import psycopg2
from psycopg2 import Error
import unicodedata
from datetime import datetime, timedelta

# URLs predefinidas extraídas de los otros archivos Python
URLS_PREDEFINIDAS = {
    "Argentina": {
        "nombre": "Argentina",
        "url": "https://docs.google.com/spreadsheets/d/18PvV89ic4-jV-SdsM2qsSI37AQG_ifCCXAgVBWJP_dY/edit?gid=139082797#gid=139082797"
    },
    "España": {
        "nombre": "España", 
        "url": "https://docs.google.com/spreadsheets/d/10nr7R_rtkUh7DX8uC_dQXkJJSszDd53P-gxnD3Mxi3s/edit?gid=1650683826#gid=1650683826"
    }
}

def get_db_connection():
    """Establece una conexión a la base de datos"""
    try:
        connection = psycopg2.connect(
            host="ep-plain-voice-a47r1b05-pooler.us-east-1.aws.neon.tech",
            database="verceldb",
            user="default",
            password="Rx3Eq5iQwMpl",
            sslmode="require"
        )
        return connection
    except Error as e:
        print(f"Error al conectar a PostgreSQL: {e}")
        return None

def process_artist_name(artist):
    """Procesa el nombre del artista aplicando reglas específicas de capitalización"""
    if artist.upper().replace(' ', '') == 'HA*ASH':
        return 'HAASH'
    if artist.upper() == 'C.R.O':
        return 'C.R.O'  # Mantener C.R.O tal cual
    if artist.upper() == 'KANY GARCÍA' or artist.upper() == 'KANY GARCIA':
        return 'Kany Garcia'  # Convertir Kany García a Kany Garcia
    
    # Remover tildes
    artist = ''.join(c for c in unicodedata.normalize('NFD', artist)
                    if unicodedata.category(c) != 'Mn')
    
    # Aplicar capitalización normal (primera letra mayúscula, resto minúscula por palabra)
    words = artist.strip().split()
    processed_words = []
    
    for word in words:
        if word:
            processed_word = word.lower().capitalize()
            processed_words.append(processed_word)
    
    return ' '.join(processed_words)

def extraer_nombre_hoja(formula):
    """
    Extrae el nombre de la hoja que está entre comillas simples en una fórmula
    y elimina espacios al principio y al final
    Ejemplo: ='  28-10-2025 DUKI (Madrid)  '!$B$1 -> 28-10-2025 DUKI (Madrid)
    """
    if not formula:
        return ""
    
    # Buscar el patrón entre comillas simples
    patron = r"='([^']+)'"
    match = re.search(patron, str(formula))
    
    if match:
        # Extraer el nombre y eliminar espacios al principio y al final
        nombre_hoja = match.group(1).strip()
        return nombre_hoja
    
    return ""

def parsear_nombre_hoja(nombre_hoja):
    """
    Parsea un nombre de hoja con formato: dd-mm-aaaa ARTISTA (CIUDAD)
    Retorna un diccionario con fecha (convertida a aaaa-mm-dd), artista y ciudad
    Ejemplo: "28-10-2025 DUKI (Madrid)" -> {"fecha": "2025-10-28", "artista": "Duki", "ciudad": "Madrid"}
    """
    if not nombre_hoja:
        return {"fecha": "", "artista": "", "ciudad": ""}
    
    # Patrón para extraer: fecha, artista y ciudad
    # dd-mm-aaaa ARTISTA (CIUDAD)
    patron = r'^(\d{2}-\d{2}-\d{4})\s+(.+?)\s+\(([^)]+)\)$'
    match = re.match(patron, nombre_hoja.strip())
    
    if match:
        fecha_original = match.group(1)  # dd-mm-aaaa
        artista_raw = match.group(2).strip()
        ciudad = match.group(3).strip()
        
        # Convertir fecha de dd-mm-aaaa a aaaa-mm-dd
        try:
            partes_fecha = fecha_original.split('-')
            if len(partes_fecha) == 3:
                dia = partes_fecha[0]
                mes = partes_fecha[1]
                año = partes_fecha[2]
                fecha_convertida = f"{año}-{mes}-{dia}"
            else:
                fecha_convertida = fecha_original  # Si no se puede convertir, mantener original
        except:
            fecha_convertida = fecha_original  # Si hay error, mantener original
        
        # Procesar el nombre del artista
        artista_procesado = process_artist_name(artista_raw)
        
        return {
            "fecha": fecha_convertida,
            "artista": artista_procesado, 
            "ciudad": ciudad
        }
    else:
        # Si no coincide con el patrón, intentar extraer lo que se pueda
        return {
            "fecha": "N/A",
            "artista": nombre_hoja,
            "ciudad": "N/A"
        }

def obtener_combinaciones_tickets(connection):
    """Obtiene todas las combinaciones únicas de fecha_show y artista de la tabla tickets"""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT DISTINCT fecha_show, artista 
            FROM tickets 
            ORDER BY fecha_show, artista
        """)
        
        resultados = cursor.fetchall()
        cursor.close()
        
        # Convertir a set de tuplas para búsqueda rápida
        combinaciones = set()
        for fecha_show, artista in resultados:
            combinaciones.add((str(fecha_show), str(artista)))
        
        return combinaciones
    except Error as e:
        print(f"Error al consultar la tabla tickets: {e}")
        return set()

def verificar_combinaciones_faltantes(datos_parseados, combinaciones_tickets):
    """Verifica qué combinaciones de fecha_show y artista no existen en tickets"""
    faltantes = []
    
    for datos in datos_parseados:
        fecha = datos['fecha']
        artista = datos['artista']
        ciudad = datos['ciudad']
        
        # Crear la combinación a verificar
        combinacion = (fecha, artista)
        
        # Verificar si NO existe en tickets
        if combinacion not in combinaciones_tickets:
            faltantes.append({
                'fecha': fecha,
                'artista': artista,
                'ciudad': ciudad
            })
    
    return faltantes

def leer_primera_hoja_con_url(sheet_url, nombre_sheet=""):
    """
    Lee la primera hoja de Google Sheets y extrae los nombres de hojas
    desde las fórmulas de la Columna A únicamente
    """
    try:
        print(f"\n🔗 Conectando con Google Sheets...")
        print(f"📊 Sheet: {nombre_sheet}")
        
        # Conectar con Google Sheets usando las credenciales
        gc = pygsheets.authorize(client_secret='client_secret.json', credentials_directory='./')
        
        # Abrir el Google Sheet usando URL completa
        sh = gc.open_by_url(sheet_url)
        
        # Obtener la primera hoja (worksheet)
        primera_hoja = sh[0]  # Índice 0 para la primera hoja
        
        print(f"📄 Leyendo la hoja: {primera_hoja.title}")
        print("=" * 80)
        
        # Obtener fórmulas solo de la columna A
        print("🔍 Obteniendo fórmulas de la Columna A...")
        
        try:
            columna_a_formula = primera_hoja.get_col(1, include_tailing_empty=False, value_render='FORMULA')
        except Exception as e:
            print(f"❌ Error al obtener fórmulas: {e}")
            return []
        
        # Determinar hasta qué fila hay datos
        max_filas = len(columna_a_formula)
        
        # Si no hay suficientes filas, mostrar mensaje
        if max_filas < 4:
            print("⚠️  No hay suficientes datos (menos de 4 filas)")
            return []
        
        # Lista para almacenar los nombres de hojas únicos y los datos parseados
        nombres_hojas = set()
        datos_parseados = []
        
        # Extraer nombres de hojas desde la fila 4 (índice 3)
        print(f"📋 Extrayendo nombres de hojas desde la fila 4 (Solo Columna A) y moneda (Columna K):")
        print("-" * 80)
        print(f"{'Fila':<6} | {'Nombre de Hoja (Columna A)':<50} | {'Moneda (Columna K)':<15}")
        print("-" * 80)
        
        # Obtener también la columna K (moneda)
        try:
            columna_k_valores = primera_hoja.get_col(11, include_tailing_empty=False)  # K es columna 11
        except Exception as e:
            print(f"❌ Error al obtener columna K: {e}")
            columna_k_valores = []
        
        filas_procesadas = 0
        for i in range(3, max_filas):  # Empezar desde índice 3 (fila 4)
            # Obtener fórmula de columna A
            formula_a = columna_a_formula[i] if i < len(columna_a_formula) else ""
            
            # Obtener valor de columna K (moneda)
            moneda_valor = columna_k_valores[i] if i < len(columna_k_valores) else ""
            moneda_str = str(moneda_valor).strip() if moneda_valor else "N/A"
            
            # Extraer nombre de hoja (ya viene con .strip() aplicado)
            nombre_hoja_a = extraer_nombre_hoja(formula_a)
            
            # Solo mostrar si la columna A tiene nombre de hoja
            if nombre_hoja_a:
                print(f"{i+1:<6} | {nombre_hoja_a:<50} | {moneda_str:<15}")
                filas_procesadas += 1
                
                # Agregar a la lista de nombres únicos
                if nombre_hoja_a not in nombres_hojas:
                    nombres_hojas.add(nombre_hoja_a)
                    # Parsear el nombre de hoja y agregar a datos parseados
                    datos = parsear_nombre_hoja(nombre_hoja_a)
                    datos['moneda'] = moneda_str  # Agregar la moneda
                    datos_parseados.append(datos)
        
        print("-" * 80)
        print(f"✅ Procesadas {filas_procesadas} filas con nombres de hojas")
        
        # Mostrar resumen de hojas únicas encontradas
        if nombres_hojas:
            nombres_ordenados = sorted(list(nombres_hojas))
            print(f"\n📋 RESUMEN - Hojas únicas encontradas ({len(nombres_ordenados)}):")
            print("-" * 80)
            
            for idx, nombre in enumerate(nombres_ordenados, 1):
                print(f"{idx:3}. {nombre}")
            
            print("-" * 80)
        else:
            print("\n⚠️  No se encontraron nombres de hojas en las fórmulas de la Columna A")
        
        return datos_parseados
        
    except FileNotFoundError:
        print("❌ Error: No se encontró el archivo de credenciales 'client_secret.json'")
        print("Asegúrate de que el archivo esté en el directorio actual")
        return []
    except Exception as e:
        print(f"❌ Error al leer Google Sheets: {e}")
        print("\n🔧 Posibles soluciones:")
        print("1. Verifica que la URL del Google Sheet sea correcta")
        print("2. Asegúrate de que las credenciales tengan acceso al sheet")
        print("3. Comprueba que el sheet tenga al menos una hoja")
        return []

def leer_datos_hoja_especifica(sh, nombre_hoja):
    """
    Lee los datos específicos de una hoja: headers de fila 13 (B-F) y datos desde fila 14 (B-F)
    Además obtiene venue (B1), capacidad (D8), holdeo (E8) y cortesias (F8)
    """
    try:
        # Buscar la hoja específica por nombre
        hoja_encontrada = None
        for worksheet in sh.worksheets():
            if worksheet.title == nombre_hoja:
                hoja_encontrada = worksheet
                break
        
        if not hoja_encontrada:
            print(f"  ❌ No se encontró la hoja: {nombre_hoja}")
            return None, []
        
        # Obtener datos adicionales específicos
        info_adicional = {}
        try:
            # Agregar el nombre de la hoja como "show"
            info_adicional['show'] = nombre_hoja
            
            # B2 como fecha del show (valor absoluto)
            try:
                fecha_show_value = hoja_encontrada.get_value('B2', value_render='UNFORMATTED_VALUE')
                
                # Si es un número (serial de fecha), convertirlo
                if isinstance(fecha_show_value, (int, float)) or (fecha_show_value and str(fecha_show_value).replace('.', '').isdigit()):
                    fecha_show_str = serial_to_date(fecha_show_value)
                else:
                    fecha_show_str = str(fecha_show_value).strip() if fecha_show_value else ""
                    
                info_adicional['fecha_show'] = fecha_show_str
            except Exception as e:
                # Si hay error, intentar obtener el valor normal
                fecha_show_value = hoja_encontrada.get_value('B2')
                info_adicional['fecha_show'] = str(fecha_show_value).strip() if fecha_show_value else ""
            
            # B1 como artista (con formato: primera letra mayúscula, resto minúscula por palabra)
            artista_value = hoja_encontrada.get_value('B1')
            artista_str = str(artista_value).strip() if artista_value else ""
            if artista_str:
                # Usar la función process_artist_name que maneja casos especiales como C.R.O
                artista_str = process_artist_name(artista_str)
            info_adicional['artista'] = artista_str
            
            # B4 como venue
            venue_value = hoja_encontrada.get_value('B4')
            info_adicional['venue'] = str(venue_value).strip() if venue_value else ""
            
            # D8 como capacidad (sin separadores de miles)
            capacidad_value = hoja_encontrada.get_value('D8')
            capacidad_str = str(capacidad_value).strip() if capacidad_value else "0"
            capacidad_str = capacidad_str.replace('.', '').replace(',', '')
            if capacidad_str == "" or capacidad_str == "-":
                capacidad_str = "0"
            info_adicional['capacidad'] = capacidad_str
            
            # E8 como holdeo
            holdeo_value = hoja_encontrada.get_value('E8')
            info_adicional['holdeo'] = str(holdeo_value).strip() if holdeo_value else ""
            
            # F8 como cortesias
            cortesias_value = hoja_encontrada.get_value('F8')
            info_adicional['cortesias'] = str(cortesias_value).strip() if cortesias_value else ""
            
        except Exception as e:
            print(f"  ⚠️  Error al leer datos adicionales: {e}")
            info_adicional = {'show': nombre_hoja, 'fecha_show': '', 'artista': '', 'venue': '', 'capacidad': '0', 'holdeo': '', 'cortesias': ''}
        
        # Obtener headers de la fila 13, columnas B a F
        try:
            headers_range = hoja_encontrada.get_values('B13', 'F13')
            headers = headers_range[0] if headers_range else []
            
            # Asegurar que tenemos 5 headers (B, C, D, E, F)
            while len(headers) < 5:
                headers.append("")
                
        except Exception as e:
            print(f"  ⚠️  Error al leer headers: {e}")
            headers = ["B", "C", "D", "E", "F"]  # Headers por defecto
        
        # Obtener todos los datos desde la fila 14 en adelante, columnas B a F
        try:
            # Calcular la fecha de ayer
            fecha_ayer = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            
            # Obtener un rango amplio para asegurar que capturamos todos los datos
            datos_range = hoja_encontrada.get_values('B14', 'F200')  # Hasta fila 200 debería ser suficiente
            
            # Obtener valores reales de la columna fecha (columna B) sin formato
            fechas_reales = hoja_encontrada.get_values('B14', 'B200', value_render='UNFORMATTED_VALUE')
            
            if not datos_range:
                return headers, [], info_adicional
            
            datos_procesados = []
            
            # Procesar filas hasta encontrar la fecha de ayer en columna A (fecha)
            for i, fila in enumerate(datos_range):
                fila_numero = 14 + i
                
                # Asegurar que la fila tenga 5 columnas (B, C, D, E, F)
                while len(fila) < 5:
                    fila.append("")
                
                # Solo tomar las primeras 5 columnas (B, C, D, E, F)
                fila_procesada = fila[:5]
                
                # Obtener valor de columna C para referencia
                columna_c = fila_procesada[1].strip() if len(fila_procesada) > 1 and fila_procesada[1] else ""
                
                # Obtener la fecha real de la celda para comparación y mostrar
                fecha_real = ""
                fecha_mostrar = ""
                if i < len(fechas_reales) and len(fechas_reales[i]) > 0:
                    valor_crudo = fechas_reales[i][0]
                    
                    # Si es un número (serial de fecha), convertirlo
                    try:
                        if isinstance(valor_crudo, (int, float)) or str(valor_crudo).replace('.', '').isdigit():
                            fecha_mostrar = serial_to_date(valor_crudo)
                            fecha_real = fecha_mostrar
                        else:
                            fecha_real = str(valor_crudo).strip()
                            fecha_mostrar = fecha_real
                    except:
                        fecha_real = str(valor_crudo).strip()
                        fecha_mostrar = fecha_real
                
                # Para la comparación, intentar extraer la fecha en formato YYYY-MM-DD
                fecha_celda = ""
                if fecha_real and fecha_real != "-" and fecha_real != "":
                    # Intentar convertir diversos formatos a YYYY-MM-DD para comparación
                    try:
                        if '/' in fecha_real:
                            partes = fecha_real.split('/')
                            if len(partes) == 3:  # dd/mm/yyyy
                                dia = partes[0].zfill(2)
                                mes = partes[1].zfill(2)
                                año = partes[2]
                                fecha_celda = f"{año}-{mes}-{dia}"
                        elif '-' in fecha_real and len(fecha_real) == 10:  # yyyy-mm-dd
                            fecha_celda = fecha_real
                    except:
                        pass
                
                # Si la fecha es mayor a ayer, no incluir este registro y parar
                if fecha_celda and fecha_celda > fecha_ayer:
                    break
                
                # Si encontramos la fecha de ayer, incluir esta fila y parar después
                incluir_fila = True
                
                # Procesar valores: reemplazar "-" y vacíos por "0"
                fila_limpia = []
                for idx, valor in enumerate(fila_procesada):
                    valor_str = str(valor).strip()
                    
                    # Columna fecha (índice 0): usar valor real de la celda
                    if idx == 0:
                        if fecha_mostrar and fecha_mostrar != "-" and fecha_mostrar != "":
                            valor_str = fecha_mostrar
                        else:
                            valor_str = "0"
                    # Columnas de venta (índices 1 y 3: Venta diaria y Venta Total)
                    elif idx == 1 or idx == 3:
                        # Remover separadores de miles (puntos y comas)
                        valor_str = valor_str.replace('.', '').replace(',', '')
                        # Reemplazar - por 0
                        if valor_str == "" or valor_str == "-":
                            valor_str = "0"
                    # Procesar columnas de montos (índices 2 y 4: Monto diario y Recaudacion total)
                    elif idx == 2 or idx == 4:
                        # Remover símbolo $ y espacios
                        valor_str = valor_str.replace('$', '').strip()
                        # Reemplazar - por 0
                        if valor_str == "" or valor_str == "-":
                            valor_str = "0"
                    else:
                        # Para otras columnas, solo reemplazar vacíos y - por 0
                        if valor_str == "" or valor_str == "-":
                            valor_str = "0"
                    
                    fila_limpia.append(valor_str)
                
                # Agregar la fila procesada
                datos_procesados.append({
                    'fila': fila_numero,
                    'datos': fila_limpia,
                    'columna_c': columna_c if columna_c else "0"  # Si está vacía, mostrar como "0"
                })
                
                # Si esta fila tiene la fecha de ayer, parar después de procesarla
                if fecha_celda == fecha_ayer:
                    break
            
            return headers, datos_procesados, info_adicional
            
        except Exception as e:
            print(f"  ⚠️  Error al leer datos: {e}")
            return headers, [], info_adicional
            
    except Exception as e:
        print(f"  ❌ Error general al leer hoja {nombre_hoja}: {e}")
        return None, [], {}

def mostrar_datos_hoja_faltante(nombre_hoja, headers, datos, info_adicional):
    """
    Muestra los datos de una hoja faltante en formato tabla, incluyendo información adicional
    """
    if not datos:
        print(f"  📋 {nombre_hoja}: Sin datos para mostrar")
        return
    
    print(f"\n  📋 {nombre_hoja}:")
    print("  " + "-" * 80)
    
    # Mostrar información adicional
    if info_adicional:
        print(f"  📋 Show: {info_adicional.get('show', 'N/A')}")
        print(f"  📅 Fecha: {info_adicional.get('fecha_show', 'N/A')}")
        print(f"  🌍 País: {info_adicional.get('pais', '')}")
        print(f"  🎤 Artista: {info_adicional.get('artista', 'N/A')}")
        print(f"  🏟️  Venue: {info_adicional.get('venue', 'N/A')}")
        print(f"  💰 Moneda: {info_adicional.get('moneda', 'N/A')}")
        print(f"  📊 Capacidad: {info_adicional.get('capacidad', '0')}")
        print(f"  🔒 Holdeo: {info_adicional.get('holdeo', 'N/A')}")
        print(f"  🎫 Cortesias: {info_adicional.get('cortesias', 'N/A')}")
        print("  " + "-" * 80)
    
    # Mostrar headers
    header_line = f"  {'Fila':<6}"
    for i, header in enumerate(headers):
        header_display = header if header else f"Col{chr(66+i)}"  # B, C, D, E, F
        header_line += f" | {header_display:<12}"
    print(header_line)
    print("  " + "-" * 80)
    
    # Mostrar datos
    datos_mostrados = 0
    for item in datos:
        fila_num = item['fila']
        fila_datos = item['datos']
        
        # Solo mostrar si al menos un valor no es "0"
        tiene_datos = any(valor != "0" for valor in fila_datos)
        
        if tiene_datos:
            line = f"  {fila_num:<6}"
            for valor in fila_datos:
                line += f" | {valor:<12}"
            print(line)
            datos_mostrados += 1
    
    print("  " + "-" * 80)
    print(f"  ✅ {datos_mostrados} filas con datos mostradas")

def mostrar_datos_parseados(datos_argentina, datos_espana):
    """
    Muestra los datos parseados de ambos sheets en formato tabla
    """
    print("\n" + "=" * 90)
    print("📊 DATOS SEPARADOS POR COLUMNAS")
    print("=" * 90)
    
    # Mostrar datos de Argentina
    if datos_argentina:
        print(f"\n🇦🇷 ARGENTINA ({len(datos_argentina)} shows):")
        print("-" * 90)
        print(f"{'#':<3} | {'Fecha':<12} | {'Artista':<25} | {'Ciudad':<25}")
        print("-" * 90)
        
        # Ordenar por fecha
        datos_argentina_ordenados = sorted(datos_argentina, key=lambda x: x['fecha'])
        
        for idx, datos in enumerate(datos_argentina_ordenados, 1):
            fecha = datos['fecha']
            artista = datos['artista'][:25]  # Truncar si es muy largo
            ciudad = datos['ciudad'][:25]    # Truncar si es muy largo
            print(f"{idx:<3} | {fecha:<12} | {artista:<25} | {ciudad:<25}")
        
        print("-" * 90)
    
    # Mostrar datos de España
    if datos_espana:
        print(f"\n🇪🇸 ESPAÑA ({len(datos_espana)} shows):")
        print("-" * 90)
        print(f"{'#':<3} | {'Fecha':<12} | {'Artista':<25} | {'Ciudad':<25}")
        print("-" * 90)
        
        # Ordenar por fecha
        datos_espana_ordenados = sorted(datos_espana, key=lambda x: x['fecha'])
        
        for idx, datos in enumerate(datos_espana_ordenados, 1):
            fecha = datos['fecha']
            artista = datos['artista'][:25]  # Truncar si es muy largo
            ciudad = datos['ciudad'][:25]    # Truncar si es muy largo
            print(f"{idx:<3} | {fecha:<12} | {artista:<25} | {ciudad:<25}")
        
        print("-" * 90)
    
    # Resumen general
    total_shows = len(datos_argentina) + len(datos_espana)
    print(f"\n📈 RESUMEN GENERAL:")
    print(f"   Argentina: {len(datos_argentina)} shows")
    print(f"   España:    {len(datos_espana)} shows")
    print(f"   TOTAL:     {total_shows} shows")

def mostrar_combinaciones_faltantes(faltantes_argentina, faltantes_espana):
    """Muestra las combinaciones que no existen en la tabla tickets"""
    print("\n" + "=" * 90)
    print("⚠️  COMBINACIONES NO ENCONTRADAS EN TABLA TICKETS")
    print("=" * 90)
    
    total_faltantes = len(faltantes_argentina) + len(faltantes_espana)
    
    if total_faltantes == 0:
        print("\n✅ ¡Excelente! Todas las combinaciones de fecha_show y artista existen en la tabla tickets.")
        print("=" * 90)
        return faltantes_argentina, faltantes_espana
    
    # Mostrar faltantes de Argentina
    if faltantes_argentina:
        print(f"\n🇦🇷 ARGENTINA - Faltantes ({len(faltantes_argentina)}):")
        print("-" * 90)
        print(f"{'#':<3} | {'Fecha Show':<12} | {'Artista':<25} | {'Ciudad':<25}")
        print("-" * 90)
        
        # Ordenar por fecha
        faltantes_argentina_ordenados = sorted(faltantes_argentina, key=lambda x: x['fecha'])
        
        for idx, datos in enumerate(faltantes_argentina_ordenados, 1):
            fecha = datos['fecha']
            artista = datos['artista'][:25]
            ciudad = datos['ciudad'][:25]
            print(f"{idx:<3} | {fecha:<12} | {artista:<25} | {ciudad:<25}")
        
        print("-" * 90)
    
    # Mostrar faltantes de España
    if faltantes_espana:
        print(f"\n🇪🇸 ESPAÑA - Faltantes ({len(faltantes_espana)}):")
        print("-" * 90)
        print(f"{'#':<3} | {'Fecha Show':<12} | {'Artista':<25} | {'Ciudad':<25}")
        print("-" * 90)
        
        # Ordenar por fecha
        faltantes_espana_ordenados = sorted(faltantes_espana, key=lambda x: x['fecha'])
        
        for idx, datos in enumerate(faltantes_espana_ordenados, 1):
            fecha = datos['fecha']
            artista = datos['artista'][:25]
            ciudad = datos['ciudad'][:25]
            print(f"{idx:<3} | {fecha:<12} | {artista:<25} | {ciudad:<25}")
        
        print("-" * 90)
    
    print(f"\n📊 RESUMEN DE FALTANTES:")
    print(f"   Argentina: {len(faltantes_argentina)} combinaciones faltantes")
    print(f"   España:    {len(faltantes_espana)} combinaciones faltantes")
    print(f"   TOTAL:     {total_faltantes} combinaciones faltantes")
    print("=" * 90)
    
    return faltantes_argentina, faltantes_espana

def serial_to_date(serial_number):
    """
    Convierte un número serial de Google Sheets a fecha legible
    Google Sheets cuenta días desde el 30 de diciembre de 1899
    """
    try:
        serial_num = float(serial_number)
        # Google Sheets epoch: 30 de diciembre de 1899
        epoch = datetime(1899, 12, 30)
        fecha = epoch + timedelta(days=serial_num)
        return fecha.strftime("%Y-%m-%d")
    except:
        return str(serial_number)

def generar_insert_statements(faltantes_data):
    """
    Genera los INSERT statements para los shows faltantes sin ejecutarlos
    """
    insert_statements = []
    
    for show_data in faltantes_data:
        nombre_show = show_data['info_adicional']['show']
        fecha_show = show_data['info_adicional']['fecha_show']
        artista = show_data['info_adicional']['artista']
        venue = show_data['info_adicional']['venue']
        capacidad = show_data['info_adicional']['capacidad']
        holdeo = show_data['info_adicional']['holdeo']
        cortesias = show_data['info_adicional']['cortesias']
        moneda = show_data['info_adicional']['moneda']
        pais = show_data['info_adicional']['pais']
        ciudad = show_data['ciudad']
        
        # Determinar función: NULL si termina en F1 o F2, sino el nombre del show
        funcion = "NULL"
        if not (nombre_show.endswith(" F1") or nombre_show.endswith(" F2")):
            funcion = f"'{nombre_show}'"
        
        # Generar INSERT para cada fila de datos de venta
        for dato in show_data['datos']:
            fila_datos = dato['datos']
            
            # Extraer valores de cada columna
            fecha_venta = fila_datos[0] if len(fila_datos) > 0 else ''
            venta_diaria = fila_datos[1] if len(fila_datos) > 1 else '0'
            monto_diario = fila_datos[2] if len(fila_datos) > 2 else '0'
            venta_total = fila_datos[3] if len(fila_datos) > 3 else '0'
            recaudacion_total = fila_datos[4] if len(fila_datos) > 4 else '0'
            
            # Limpiar y formatear valores para el INSERT
            fecha_venta_clean = fecha_venta.replace("'", "''") if fecha_venta else ''
            venta_diaria_clean = venta_diaria.replace("'", "''") if venta_diaria else '0'
            monto_diario_clean = monto_diario.replace("'", "''") if monto_diario else '0'
            venta_total_clean = venta_total.replace("'", "''") if venta_total else '0'
            recaudacion_total_clean = recaudacion_total.replace("'", "''") if recaudacion_total else '0'
            artista_clean = artista.replace("'", "''") if artista else ''
            fecha_show_clean = fecha_show.replace("'", "''") if fecha_show else ''
            capacidad_clean = capacidad.replace("'", "''") if capacidad else '0'
            holdeo_clean = holdeo.replace("'", "''") if holdeo else '0'
            venue_clean = venue.replace("'", "''") if venue else ''
            ciudad_clean = ciudad.replace("'", "''") if ciudad else ''
            show_clean = nombre_show.replace("'", "''") if nombre_show else ''
            moneda_clean = moneda.replace("'", "''") if moneda else ''
            pais_clean = pais.replace("'", "''") if pais else ''
            cortesias_clean = cortesias.replace("'", "''") if cortesias else '0'
            
            # Crear el INSERT statement
            insert_sql = f"""INSERT INTO tickets (
    fecha_venta, venta_diaria, monto_diario_ars, venta_total, recaudacion_total, 
    artista, fecha_show, capacidad, holdeo, venue, ciudad, funcion, show, 
    moneda, tipo_cambio, monto_diario_usd, dias_restantes, pais, cortesias
) VALUES (
    '{fecha_venta_clean}', '{venta_diaria_clean}', '{monto_diario_clean}', '{venta_total_clean}', '{recaudacion_total_clean}',
    '{artista_clean}', '{fecha_show_clean}', '{capacidad_clean}', '{holdeo_clean}', '{venue_clean}', '{ciudad_clean}', {funcion}, '{show_clean}',
    '{moneda_clean}', NULL, NULL, NULL, '{pais_clean}', {cortesias_clean}
);"""
            
            insert_statements.append(insert_sql)
    
    return insert_statements

def ejecutar_insert_statements(insert_statements, connection):
    """
    Ejecuta los INSERT statements en la base de datos y devuelve estadísticas
    """
    if not insert_statements:
        print("\n📋 No hay INSERT statements para ejecutar")
        return {
            'total_queries': 0,
            'ejecutados_exitosos': 0,
            'errores': 0,
            'detalles_errores': []
        }
    
    print("\n" + "=" * 100)
    print("💾 EJECUTANDO INSERT STATEMENTS EN LA BASE DE DATOS")
    print("=" * 100)
    print(f"📊 Total de registros a insertar: {len(insert_statements)}")
    print("-" * 100)
    
    cursor = connection.cursor()
    ejecutados_exitosos = 0
    errores = 0
    detalles_errores = []
    
    for idx, insert_sql in enumerate(insert_statements, 1):
        try:
            print(f"📝 Ejecutando registro {idx}/{len(insert_statements)}...", end=" ")
            cursor.execute(insert_sql)
            connection.commit()
            print("✅ OK")
            ejecutados_exitosos += 1
            
        except Exception as e:
            print(f"❌ ERROR")
            errores += 1
            error_detalle = {
                'registro': idx,
                'error': str(e),
                'query': insert_sql[:200] + "..." if len(insert_sql) > 200 else insert_sql
            }
            detalles_errores.append(error_detalle)
            print(f"   Error: {str(e)}")
            # Continuar con el siguiente registro en caso de error
            continue
    
    cursor.close()
    
    # Estadísticas finales
    estadisticas = {
        'total_queries': len(insert_statements),
        'ejecutados_exitosos': ejecutados_exitosos,
        'errores': errores,
        'detalles_errores': detalles_errores
    }
    
    print("\n" + "-" * 100)
    print(f"📊 ESTADÍSTICAS DE EJECUCIÓN:")
    print(f"   ✅ Registros insertados exitosamente: {ejecutados_exitosos}")
    print(f"   ❌ Errores: {errores}")
    print(f"   📈 Porcentaje de éxito: {(ejecutados_exitosos/len(insert_statements)*100):.1f}%")
    
    if errores > 0:
        print(f"\n⚠️  DETALLES DE ERRORES:")
        for error in detalles_errores:
            print(f"   Registro {error['registro']}: {error['error']}")
    
    print("=" * 100)
    
    return estadisticas

def mostrar_resumen_final(estadisticas_argentina, estadisticas_espana, faltantes_arg, faltantes_esp):
    """
    Muestra un resumen final de toda la operación
    """
    print("\n" + "🎯" * 50)
    print("📋 RESUMEN FINAL DE LA OPERACIÓN")
    print("🎯" * 50)
    
    # Resumen de shows procesados
    total_shows_faltantes = len(faltantes_arg) + len(faltantes_esp)
    print(f"\n📊 SHOWS FALTANTES PROCESADOS:")
    print(f"   🇦🇷 Argentina: {len(faltantes_arg)} shows")
    print(f"   🇪🇸 España: {len(faltantes_esp)} shows")
    print(f"   📈 TOTAL: {total_shows_faltantes} shows")
    
    # Estadísticas de inserción
    total_registros_argentina = estadisticas_argentina['total_queries']
    total_registros_espana = estadisticas_espana['total_queries']
    total_registros = total_registros_argentina + total_registros_espana
    
    exitosos_argentina = estadisticas_argentina['ejecutados_exitosos']
    exitosos_espana = estadisticas_espana['ejecutados_exitosos']
    total_exitosos = exitosos_argentina + exitosos_espana
    
    errores_argentina = estadisticas_argentina['errores']
    errores_espana = estadisticas_espana['errores']
    total_errores = errores_argentina + errores_espana
    
    print(f"\n💾 REGISTROS DE VENTA INSERTADOS:")
    print(f"   🇦🇷 Argentina: {exitosos_argentina}/{total_registros_argentina} (éxito: {(exitosos_argentina/total_registros_argentina*100):.1f}%)" if total_registros_argentina > 0 else "   🇦🇷 Argentina: 0/0")
    print(f"   🇪🇸 España: {exitosos_espana}/{total_registros_espana} (éxito: {(exitosos_espana/total_registros_espana*100):.1f}%)" if total_registros_espana > 0 else "   🇪🇸 España: 0/0")
    print(f"   📈 TOTAL: {total_exitosos}/{total_registros} (éxito: {(total_exitosos/total_registros*100):.1f}%)" if total_registros > 0 else "   📈 TOTAL: 0/0")
    
    # Resumen de errores
    if total_errores > 0:
        print(f"\n⚠️  ERRORES ENCONTRADOS:")
        print(f"   🇦🇷 Argentina: {errores_argentina} errores")
        print(f"   🇪🇸 España: {errores_espana} errores")
        print(f"   📈 TOTAL: {total_errores} errores")
        
        # Mostrar algunos ejemplos de errores si los hay
        if estadisticas_argentina['detalles_errores']:
            print(f"\n   🔍 Ejemplo de error en Argentina:")
            error_ejemplo = estadisticas_argentina['detalles_errores'][0]
            print(f"      {error_ejemplo['error']}")
        
        if estadisticas_espana['detalles_errores']:
            print(f"\n   🔍 Ejemplo de error en España:")
            error_ejemplo = estadisticas_espana['detalles_errores'][0]
            print(f"      {error_ejemplo['error']}")
    else:
        print(f"\n✅ ¡PERFECTO! No se encontraron errores durante la inserción")
    
    # Estado final
    if total_errores == 0:
        print(f"\n🎉 OPERACIÓN COMPLETADA EXITOSAMENTE")
        print(f"   ✅ Todos los {total_registros} registros fueron insertados correctamente")
        print(f"   📋 Los {total_shows_faltantes} shows ya están sincronizados con la base de datos")
    else:
        print(f"\n⚠️  OPERACIÓN COMPLETADA CON ERRORES")
        print(f"   ✅ {total_exitosos} registros insertados correctamente")
        print(f"   ❌ {total_errores} registros fallaron")
        print(f"   🔧 Revisa los errores para corregir los registros faltantes")
    
    print("\n" + "🎯" * 50)

def obtener_datos_shows_faltantes(faltantes, sheet_url, nombre_sheet, datos_parseados_sheet):
    """
    Obtiene los datos específicos de cada show faltante y retorna la información estructurada
    """
    if not faltantes:
        return []
    
    print(f"\n📊 OBTENIENDO DATOS ESPECÍFICOS DE SHOWS FALTANTES - {nombre_sheet}")
    print("=" * 90)
    
    faltantes_data = []
    
    try:
        # Conectar con Google Sheets
        gc = pygsheets.authorize(client_secret='client_secret.json', credentials_directory='./')
        sh = gc.open_by_url(sheet_url)
        
        print(f"🔗 Conectado al sheet: {nombre_sheet}")
        print(f"📋 Shows faltantes a analizar: {len(faltantes)}")
        
        for idx, faltante in enumerate(faltantes, 1):
            fecha = faltante['fecha']
            artista = faltante['artista']
            ciudad = faltante['ciudad']
            
            # Buscar la moneda correspondiente en los datos parseados
            moneda_show = "N/A"
            for datos_show in datos_parseados_sheet:
                if (datos_show['fecha'] == fecha and 
                    datos_show['artista'] == artista and 
                    datos_show['ciudad'] == ciudad):
                    moneda_show = datos_show.get('moneda', 'N/A')
                    break
            
            # Reconstruir el nombre de la hoja original (dd-mm-aaaa ARTISTA (CIUDAD))
            # Convertir fecha de aaaa-mm-dd a dd-mm-aaaa
            partes_fecha = fecha.split('-')
            if len(partes_fecha) == 3:
                año, mes, dia = partes_fecha
                fecha_original = f"{dia}-{mes}-{año}"
            else:
                fecha_original = fecha
            
            # Reconstruir nombre de hoja
            nombre_hoja_buscar = f"{fecha_original} {artista.upper()} ({ciudad})"
            
            print(f"\n🔍 ({idx}/{len(faltantes)}) Analizando: {nombre_hoja_buscar}")
            
            # Leer datos específicos de la hoja
            headers, datos, info_adicional = leer_datos_hoja_especifica(sh, nombre_hoja_buscar)
            
            # Agregar la moneda a la información adicional
            info_adicional['moneda'] = moneda_show
            
            # Agregar información del país según el sheet
            if nombre_sheet == "España":
                info_adicional['pais'] = "España"
            else:
                info_adicional['pais'] = ""
            
            if headers is not None:
                mostrar_datos_hoja_faltante(nombre_hoja_buscar, headers, datos, info_adicional)
                
                # Guardar datos para generar INSERTs
                faltantes_data.append({
                    'headers': headers,
                    'datos': datos,
                    'info_adicional': info_adicional,
                    'ciudad': ciudad
                })
            else:
                print(f"  ❌ No se pudieron obtener datos de: {nombre_hoja_buscar}")
        
        print("\n" + "=" * 90)
        print(f"✅ Análisis de {nombre_sheet} completado")
        
        return faltantes_data
        
    except Exception as e:
        print(f"❌ Error al obtener datos de shows faltantes: {e}")
        return []

def main():
    """
    Función principal del programa - Ejecuta automáticamente Argentina y España
    """
    print("🔍 Script para extraer, parsear y comparar con tabla tickets")
    print("📊 Artistas formateados + Comparación con Base de Datos + Datos específicos + INSERCIÓN EN BD")
    print("🚀 Ejecutando automáticamente: Argentina → España")
    print("=" * 80)
    
    # Conectar a la base de datos
    print("\n🔗 Conectando a la base de datos...")
    conn = get_db_connection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos. Abortando proceso.")
        return
    
    # Obtener combinaciones existentes en tickets
    print("📋 Obteniendo combinaciones existentes en tabla tickets...")
    combinaciones_tickets = obtener_combinaciones_tickets(conn)
    print(f"✅ Se encontraron {len(combinaciones_tickets)} combinaciones en tickets")
    
    # Lista de sheets a procesar en orden
    sheets_a_procesar = ["Argentina", "España"]
    datos_todos_sheets = {}
    
    for sheet_key in sheets_a_procesar:
        if sheet_key in URLS_PREDEFINIDAS:
            sheet_info = URLS_PREDEFINIDAS[sheet_key]
            print(f"\n🌍 Procesando: {sheet_info['nombre']}")
            
            # Ejecutar la función de lectura y obtener datos parseados
            datos_parseados = leer_primera_hoja_con_url(sheet_info['url'], sheet_info['nombre'])
            datos_todos_sheets[sheet_key] = datos_parseados
            
            # Separador entre sheets
            if sheet_key != sheets_a_procesar[-1]:  # Si no es el último
                print("\n" + "🔄" * 40)
    
    # Mostrar todos los datos parseados
    mostrar_datos_parseados(
        datos_todos_sheets.get("Argentina", []),
        datos_todos_sheets.get("España", [])
    )
    
    # Verificar combinaciones faltantes
    print("\n🔍 Verificando combinaciones contra tabla tickets...")
    
    faltantes_argentina = verificar_combinaciones_faltantes(
        datos_todos_sheets.get("Argentina", []), 
        combinaciones_tickets
    )
    
    faltantes_espana = verificar_combinaciones_faltantes(
        datos_todos_sheets.get("España", []), 
        combinaciones_tickets
    )
    
    # Mostrar combinaciones faltantes
    faltantes_arg, faltantes_esp = mostrar_combinaciones_faltantes(faltantes_argentina, faltantes_espana)
    
    # Lista para almacenar todos los datos de shows faltantes
    todos_los_faltantes_data = []
    
    # Variables para estadísticas
    estadisticas_argentina = {'total_queries': 0, 'ejecutados_exitosos': 0, 'errores': 0, 'detalles_errores': []}
    estadisticas_espana = {'total_queries': 0, 'ejecutados_exitosos': 0, 'errores': 0, 'detalles_errores': []}
    
    # Obtener datos específicos de shows faltantes y ejecutar INSERTs
    if faltantes_arg:
        faltantes_data_arg = obtener_datos_shows_faltantes(
            faltantes_arg, 
            URLS_PREDEFINIDAS["Argentina"]["url"], 
            "Argentina",
            datos_todos_sheets["Argentina"]
        )
        
        # Generar y ejecutar INSERT statements para Argentina
        if faltantes_data_arg:
            insert_statements_arg = generar_insert_statements(faltantes_data_arg)
            estadisticas_argentina = ejecutar_insert_statements(insert_statements_arg, conn)
    
    if faltantes_esp:
        faltantes_data_esp = obtener_datos_shows_faltantes(
            faltantes_esp, 
            URLS_PREDEFINIDAS["España"]["url"], 
            "España",
            datos_todos_sheets["España"]
        )
        
        # Generar y ejecutar INSERT statements para España
        if faltantes_data_esp:
            insert_statements_esp = generar_insert_statements(faltantes_data_esp)
            estadisticas_espana = ejecutar_insert_statements(insert_statements_esp, conn)
    
    # Mostrar resumen final de la operación
    mostrar_resumen_final(estadisticas_argentina, estadisticas_espana, faltantes_arg, faltantes_esp)
    
    # Cerrar conexión a la base de datos
    if conn:
        conn.close()
        print("\n🔗 Conexión a la base de datos cerrada")
    
    print("\n" + "=" * 80)
    print("🎉 ¡Proceso completado! Datos extraídos, parseados, comparados, analizados e INSERTADOS en BD.")
    print("=" * 80)

if __name__ == "__main__":
    main() 