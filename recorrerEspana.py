import pygsheets
import unicodedata
import re
from datetime import datetime
import psycopg2
from psycopg2 import Error
import time

def get_db_connection():
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

def get_fechas_venta_y_datos(connection, artista, fecha_show):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT fecha_venta, venta_diaria, venta_total 
            FROM tickets 
            WHERE artista = %s 
            AND fecha_show = %s 
            ORDER BY fecha_venta
        """, (artista, fecha_show))
        
        resultados = cursor.fetchall()
        fechas = []
        datos_por_fecha = {}
        
        for fecha_venta, venta_diaria, venta_total in resultados:
            fechas.append(fecha_venta)
            datos_por_fecha[fecha_venta] = {
                'venta_diaria': venta_diaria,
                'venta_total': venta_total
            }
        
        cursor.close()
        return fechas, datos_por_fecha
    except Error as e:
        print(f"Error en la consulta: {e}")
        return [], {}

def update_db_values(connection, updates, batch_size=25):
    """Actualiza los valores en la base de datos para corregir discrepancias
    
    Args:
        connection: Conexión a la base de datos
        updates: Lista de diccionarios con los valores a actualizar
        batch_size: Tamaño del lote para actualizar (por defecto 25)
    
    Returns:
        int: Número de registros actualizados
    """
    if not updates:
        return 0
    
    try:
        updated_count = 0
        total_updates = len(updates)
        
        # Dividir las actualizaciones en lotes
        batches = [updates[i:i + batch_size] for i in range(0, len(updates), batch_size)]
        
        print(f"\nActualizando {total_updates} registros en {len(batches)} lotes de {batch_size}...")
        
        for batch_index, batch in enumerate(batches, 1):
            try:
                cursor = connection.cursor()
                batch_updated = 0
                
                print(f"\nProcesando lote {batch_index}/{len(batches)} ({len(batch)} registros)...")
                
                for update_index, update in enumerate(batch, 1):
                    try:
                        cursor.execute("""
                            UPDATE tickets 
                            SET venta_diaria = %s, venta_total = %s, monto_diario_ars = %s, recaudacion_total = %s
                            WHERE artista = %s 
                            AND fecha_show = %s 
                            AND fecha_venta = %s
                        """, (
                            update['vd_sheet'], 
                            update['vt_sheet'],
                            update['md_sheet'],
                            update['rt_sheet'],
                            update['artista'], 
                            update['fecha_show'], 
                            update['fecha_venta']
                        ))
                        
                        rows_affected = cursor.rowcount
                        batch_updated += rows_affected
                        updated_count += rows_affected
                        
                        # Mostrar progreso cada 5 registros o en el último registro
                        if update_index % 5 == 0 or update_index == len(batch):
                            print(f"  Progreso: {update_index}/{len(batch)} registros procesados en este lote")
                            
                    except Error as e:
                        print(f"  ❌ Error al actualizar registro {update_index} del lote {batch_index}: {e}")
                        print(f"  Detalles: {update['artista']} - {update['fecha_show']} - {update['fecha_venta']}")
                        connection.rollback()
                        continue
                
                # Commit al final de cada lote
                connection.commit()
                print(f"  ✅ Lote {batch_index} completado: {batch_updated} registros actualizados")
                
                # Pequeña pausa entre lotes para no sobrecargar la BD
                if batch_index < len(batches):
                    print("  Pausa de 1 segundo antes del siguiente lote...")
                    time.sleep(1)
                    
            except Error as e:
                print(f"  ❌ Error procesando lote {batch_index}: {e}")
                connection.rollback()
            finally:
                cursor.close()
        
        return updated_count
    except Error as e:
        print(f"Error general al actualizar la base de datos: {e}")
        if 'connection' in locals() and connection:
            connection.rollback()
        return 0

def check_shows_ticketing(connection, artista, fecha_show):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT EXISTS(
                SELECT 1 
                FROM shows_ticketing 
                WHERE artista = %s 
                AND fecha = %s
            )
        """, (artista, fecha_show))
        
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists
    except Error as e:
        print(f"Error en la consulta a shows_ticketing: {e}")
        return False

def formatear_fecha(fecha_str, es_fecha_show=False):
    """Convierte diferentes formatos de fecha a 'aaaa-mm-dd'
    
    Args:
        fecha_str: String con la fecha a formatear
        es_fecha_show: Booleano que indica si es la fecha del show (B2) o una fecha de venta (columna B)
    """
    fecha_str = fecha_str.strip()
    
    # Si ya está en formato aaaa-mm-dd, extraer mes y día, y asignar año según el mes
    if re.match(r'^\d{4}-\d{2}-\d{2}$', fecha_str):
        # Extraer mes y día
        partes = fecha_str.split('-')
        mes = int(partes[1])
        
        # Si es fecha del show (B2), siempre usar 2025
        if es_fecha_show:
            return f"2025-{partes[1]}-{partes[2]}"
        
        # Para fechas de venta (columna B): junio a diciembre -> 2024, enero a mayo -> 2025
        año = "2024" if 6 <= mes <= 12 else "2025"
        return f"{año}-{partes[1]}-{partes[2]}"
    
    # Caso especial: formato dd/mm (sin año)
    if re.match(r'^\d{1,2}/\d{1,2}$', fecha_str):
        try:
            # Separar día y mes
            partes = fecha_str.split('/')
            dia = partes[0].zfill(2)  # Asegurar que tenga 2 dígitos
            mes = int(partes[1])  # Convertir a entero para comparar
            
            # Si es fecha del show (B2), siempre usar 2025
            if es_fecha_show:
                año = 2025
            else:
                # Para fechas de venta (columna B): junio a diciembre -> 2024, enero a mayo -> 2025
                año = 2024 if 6 <= mes <= 12 else 2025
            
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
            mes = fecha.month
            
            # Si es fecha del show (B2), siempre usar 2025
            if es_fecha_show:
                return f"2025-{fecha.strftime('%m-%d')}"
            
            # Para fechas de venta (columna B): junio a diciembre -> 2024, enero a mayo -> 2025
            año = 2024 if 6 <= mes <= 12 else 2025
            return f"{año}-{fecha.strftime('%m-%d')}"
        except ValueError:
            continue
    
    # Si no se pudo convertir, devolver el original
    return fecha_str

def process_artist_name(artist):
    if artist.upper().replace(' ', '') == 'HA*ASH':
        return 'HAASH'
    if artist.upper() == 'C.R.O':
        return 'C.R.O'
    
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

def recorrer_espana():
    # URL del sheet de España
    sheet_url = "https://docs.google.com/spreadsheets/d/10nr7R_rtkUh7DX8uC_dQXkJJSszDd53P-gxnD3Mxi3s/edit?gid=1650683826#gid=1650683826"
    
    try:
        # Autorizar y abrir el sheet
        gc = pygsheets.authorize(client_secret='client_secret.json', credentials_directory='./')
        sh = gc.open_by_url(sheet_url)
        
        print(f"\nProcesando el sheet de España...")
        
        # Obtener todas las hojas excepto 'Resumen'
        worksheets = [wks for wks in sh.worksheets() if wks.title != 'Resumen']
        
        # Conectar a la base de datos
        conn = get_db_connection()
        if not conn:
            print("No se pudo conectar a la base de datos.")
            return
        
        # Variable para rastrear si el proceso se completó exitosamente
        proceso_completado = True
        
        # Contador de hojas procesadas
        hojas_procesadas = 0
        
        # Lista para almacenar shows con discrepancias
        shows_con_discrepancias = []
        
        # Lista para almacenar actualizaciones a realizar en la BD
        actualizaciones_bd = []
        
        # Procesar todas las hojas automáticamente
        for wks in worksheets:
            print(f"\nHoja: {wks.title}")
            
            # Obtener el nombre del artista de la celda B1
            artista_raw = wks.get_value('B1').strip()
            artista_procesado = process_artist_name(artista_raw)
            
            # Obtener la fecha del show de la celda B2
            fecha_show_raw = wks.get_value('B2').strip()
            fecha_show_formateada = formatear_fecha(fecha_show_raw, es_fecha_show=True)
            
            print(f"Artista (B1): {artista_procesado}")
            print(f"Fecha Show (B2): {fecha_show_formateada}")
            
            # Obtener fechas de venta y datos de la base de datos
            fechas_venta, datos_por_fecha = get_fechas_venta_y_datos(conn, artista_procesado, fecha_show_formateada)
            if not fechas_venta:
                # Si no hay registros en tickets, verificar en shows_ticketing
                existe_en_shows_ticketing = check_shows_ticketing(conn, artista_procesado, fecha_show_formateada)
                
                if existe_en_shows_ticketing:
                    # Si existe en shows_ticketing, continuar con la siguiente hoja sin mostrar nada
                    print(f"Encontrado en shows_ticketing: {artista_procesado} - {fecha_show_formateada}")
                    hojas_procesadas += 1
                    continue
                else:
                    # Si no existe en ninguna tabla, mostrar mensaje y continuar con la siguiente hoja
                    print(f"\nNo se encontraron registros en ninguna tabla para {artista_procesado} - {fecha_show_formateada}")
                    continue
            
            # Obtener todos los valores
            all_values = wks.get_all_values(include_tailing_empty=False)
            
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
                    b_valor_formateado = formatear_fecha(b_valor, es_fecha_show=False)
                    
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
        
        # Actualizar la base de datos con los valores del sheet
        if actualizaciones_bd:
            print("\n" + "=" * 100)
            print(f"🔄 ACTUALIZANDO BASE DE DATOS 🔄")
            print("=" * 100)
            
            print(f"Se van a actualizar {len(actualizaciones_bd)} registros en la base de datos...")
            
            # Cerrar la conexión actual y abrir una nueva para asegurar una conexión fresca
            if conn:
                print("Cerrando conexión actual a la base de datos...")
                conn.close()
            
            print("Estableciendo nueva conexión a la base de datos...")
            conn = get_db_connection()
            
            if not conn:
                print("❌ No se pudo establecer una nueva conexión a la base de datos.")
                return
            
            print("✅ Nueva conexión establecida correctamente.")
            
            # Actualizar por lotes
            registros_actualizados = update_db_values(conn, actualizaciones_bd, batch_size=25)
            print(f"\n✅ RESUMEN: Se han actualizado {registros_actualizados} de {len(actualizaciones_bd)} registros en la base de datos.")
        
        print("=" * 100)
        print(f"🎵 Se han recorrido todas las hojas ({hojas_procesadas}/{len(worksheets)}) 🎵")
        print("=" * 100)
        
        # Cerrar la conexión a la base de datos
        if conn:
            conn.close()
        
    except Exception as e:
        print(f"\nError al procesar el sheet: {e}")
        # Cerrar la conexión a la base de datos en caso de error
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    recorrer_espana() 